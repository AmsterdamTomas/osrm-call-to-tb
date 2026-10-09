"""
E-trike -> charging station distance publisher (batched OSRM + ThingsBoard HTTP)

Flow (every INTERVAL_S seconds):
  1. GET each device's SHARED ATTRIBUTES (LA/LO) from ThingsBoard, in parallel.
  2. Make ONE OSRM `table` call: all trikes = sources, charging station = destination.
  3. Extract only the distance (meters -> km) for each trike, matched by source order.
  4. POST {"chargeStationDistance": <km>} to each device's telemetry endpoint
     using that device's own access token, in parallel.

Devices are loaded from a CSV file (column A = vehicle_name, column B = access_token).

Install:   pip install requests
Run:       python chargestation_distance_http.py
           python chargestation_distance_http.py path/to/devices.csv
"""

import csv
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

# =====================================================================
# CONFIG  -- edit this section only
# =====================================================================

# ThingsBoard HTTP base URL (include scheme and HTTP port, default 8080 / 443 for HTTPS).
# NOTE: the old MQTT script used port 1883; HTTP uses a different port, so check your tunnel.
TB_BASE_URL = "https://sikloweb.localto.net/"
TB_TIMEOUT_S = 2.5            # per ThingsBoard request

# CSV listing all devices. Column A = vehicle_name, column B = access_token.
# First row is treated as a header and skipped. Extra columns are ignored.
# Can be overridden from the command line: python script.py other.csv
DEVICES_CSV = "devices_access_token_profiles.csv"

# Charging station coordinates
STATION_LAT = 14.566447065332111
STATION_LON = 120.99211867786774

# Public demo OSRM server
OSRM_BASE_URL = "https://router.project-osrm.org"
OSRM_PROFILE = "driving"      # the public demo server is driving-only
OSRM_TIMEOUT_S = 4
OSRM_MAX_BATCH = 50           # max trikes per table call (demo server default limit is 100 coords)

# Timing
INTERVAL_S = 5

# ThingsBoard keys
ATTR_LAT_KEY = "LA"                      # shared attribute: latitude
ATTR_LON_KEY = "LO"                      # shared attribute: longitude
TELEMETRY_KEY = "chargeStationDistance"  # key sent to ThingsBoard

# One-time startup indicator: sent once per device after the first successful
# HTTP exchange, to confirm the program is talking to the ThingsBoard dashboard.
STARTUP_KEY = "a"
STARTUP_VALUE = 2

DISTANCE_DECIMALS = 3
LOG_LEVEL = logging.INFO                 # logging.DEBUG for every coordinate update

# =====================================================================
log = logging.getLogger("chargestation")


# =====================================================================
# CSV loader
# =====================================================================
def load_devices_from_csv(path):
    """
    Read devices from a CSV: column A = vehicle_name, column B = access_token.
    Skips the header row, blank rows, and rows missing a name or token.
    Duplicate vehicle names are skipped (first one wins).
    Returns a list of {"name": ..., "token": ...}.
    """
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Devices CSV not found: {path.resolve()}")

    devices, seen = [], set()
    # utf-8-sig strips the BOM that Excel adds; newline="" lets csv handle CRLF
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header row
        for line_no, row in enumerate(reader, start=2):
            if len(row) < 2:
                if any(cell.strip() for cell in row):
                    log.warning("CSV line %d skipped: fewer than 2 columns", line_no)
                continue
            name, token = row[0].strip(), row[1].strip()
            if not name or not token:
                log.warning("CSV line %d skipped: missing vehicle_name or access_token", line_no)
                continue
            if name in seen:
                log.warning("CSV line %d skipped: duplicate vehicle_name %s", line_no, name)
                continue
            seen.add(name)
            devices.append({"name": name, "token": token})
    return devices


# =====================================================================
# Per-device state + HTTP helpers
# =====================================================================
class Device:
    def __init__(self, name, token):
        self.name = name
        self.token = token
        self.lat = None
        self.lon = None
        self.startup_sent = False
        self.session = requests.Session()   # keep-alive: reuses the TCP connection
        base = f"{TB_BASE_URL.rstrip('/')}/api/v1/{token}"
        self.url_attrs = f"{base}/attributes"
        self.url_telemetry = f"{base}/telemetry"

    # ---- shared attributes ----
    def fetch_coords(self):
        """Pull LA/LO shared attributes. Keeps last known values on failure."""
        try:
            resp = self.session.get(
                self.url_attrs,
                params={"sharedKeys": f"{ATTR_LAT_KEY},{ATTR_LON_KEY}"},
                timeout=TB_TIMEOUT_S,
            )
            resp.raise_for_status()
            shared = resp.json().get("shared", {})
        except requests.exceptions.RequestException as e:
            log.error("[%s] ATTRIBUTE FETCH FAILED: %s", self.name, e)
            return
        except ValueError:
            log.error("[%s] ATTRIBUTE FETCH FAILED: response was not valid JSON", self.name)
            return

        try:
            first_fix = self.lat is None or self.lon is None
            if ATTR_LAT_KEY in shared:
                self.lat = float(shared[ATTR_LAT_KEY])
            if ATTR_LON_KEY in shared:
                self.lon = float(shared[ATTR_LON_KEY])
        except (TypeError, ValueError):
            log.warning("[%s] received non-numeric coordinate data: %s", self.name, shared)
            return

        if self.lat is not None and self.lon is not None:
            if first_fix:
                log.info("[%s] coordinates available: LA=%s LO=%s", self.name, self.lat, self.lon)
            else:
                log.debug("[%s] coordinates: LA=%s LO=%s", self.name, self.lat, self.lon)

        # Reaching ThingsBoard successfully -> send the one-time startup flag
        self.publish_startup_flag()

    def get_coords(self):
        if self.lat is None or self.lon is None:
            return None
        return self.lat, self.lon

    # ---- telemetry ----
    def _post_telemetry(self, payload):
        resp = self.session.post(self.url_telemetry, json=payload, timeout=TB_TIMEOUT_S)
        resp.raise_for_status()   # ThingsBoard returns 200 on success

    def publish_startup_flag(self):
        """Send {STARTUP_KEY: STARTUP_VALUE} exactly once per program run."""
        if self.startup_sent:
            return
        payload = {STARTUP_KEY: STARTUP_VALUE}
        try:
            self._post_telemetry(payload)
        except requests.exceptions.RequestException as e:
            log.error("[%s] STARTUP FLAG FAILED: %s (will retry next cycle)", self.name, e)
            return
        self.startup_sent = True
        log.info("[%s] STARTUP FLAG SUCCESS: %s=%s", self.name, STARTUP_KEY, STARTUP_VALUE)

    def publish_distance(self, km):
        try:
            self._post_telemetry({TELEMETRY_KEY: km})
        except requests.exceptions.RequestException as e:
            log.error("[%s] TRANSMISSION FAILED: %s (%s=%s km)", self.name, e, TELEMETRY_KEY, km)
            return
        log.info("[%s] TRANSMISSION SUCCESS: %s=%s km", self.name, TELEMETRY_KEY, km)


# =====================================================================
# OSRM batched call (chunked, with bad-coordinate isolation)
# =====================================================================
class OsrmBadInput(Exception):
    """OSRM rejected the request (e.g. NoSegment / InvalidQuery): likely one bad coordinate."""

class OsrmUnavailable(Exception):
    """Timeout, network error, 429 or 5xx: retrying with smaller batches will not help."""


def _osrm_table(chunk):
    """One OSRM table call for a list of (Device, (lat, lon)). Returns {name: km}."""
    coords = [f"{lon:.6f},{lat:.6f}" for _, (lat, lon) in chunk]   # OSRM wants lon,lat
    coords.append(f"{STATION_LON:.6f},{STATION_LAT:.6f}")           # destination = last
    n = len(chunk)
    sources = ";".join(str(i) for i in range(n))
    url = (f"{OSRM_BASE_URL}/table/v1/{OSRM_PROFILE}/{';'.join(coords)}"
           f"?sources={sources}&destinations={n}&annotations=distance")

    try:
        resp = requests.get(url, timeout=OSRM_TIMEOUT_S,
                            headers={"User-Agent": "etrike-chargestation-distance/1.0"})
    except requests.exceptions.RequestException as e:
        raise OsrmUnavailable(str(e))

    if resp.status_code == 429 or resp.status_code >= 500:
        raise OsrmUnavailable(f"HTTP {resp.status_code}")
    try:
        body = resp.json()
    except ValueError:
        raise OsrmUnavailable(f"HTTP {resp.status_code}, response was not valid JSON")
    if resp.status_code >= 400 or body.get("code") != "Ok" or "distances" not in body:
        raise OsrmBadInput(f"code={body.get('code')} message={body.get('message')}")

    result = {}
    for i, (dev, _) in enumerate(chunk):
        meters = body["distances"][i][0]      # row i = source i, col 0 = charging station
        if meters is None:
            log.warning("[%s] OSRM found no route to the charging station", dev.name)
            continue
        result[dev.name] = round(meters / 1000.0, DISTANCE_DECIMALS)
    return result


def _osrm_with_isolation(chunk):
    """Try the chunk; if OSRM rejects it, bisect to isolate the offending device(s)."""
    try:
        result = _osrm_table(chunk)
        log.info("OSRM CALL SUCCESS: %d device(s) in one request", len(chunk))
        return result
    except OsrmUnavailable as e:
        log.error("OSRM UNAVAILABLE (%d device(s) skipped this cycle): %s", len(chunk), e)
        return {}
    except OsrmBadInput as e:
        if len(chunk) == 1:
            dev, (lat, lon) = chunk[0]
            log.warning("[%s] OSRM rejected coordinates (%s, %s): %s", dev.name, lat, lon, e)
            return {}
        log.warning("OSRM rejected a %d-device batch (%s); splitting to isolate bad input", len(chunk), e)
        mid = len(chunk) // 2
        return {**_osrm_with_isolation(chunk[:mid]), **_osrm_with_isolation(chunk[mid:])}


def osrm_batch_distances_km(ready):
    """
    ready: list of (Device, (lat, lon)).
    Returns {device_name: km} (possibly empty). Splits into chunks of OSRM_MAX_BATCH.
    """
    result = {}
    for i in range(0, len(ready), OSRM_MAX_BATCH):
        result.update(_osrm_with_isolation(ready[i:i + OSRM_MAX_BATCH]))
    return result


# =====================================================================
# One cycle
# =====================================================================
def run_cycle(devices, pool):
    # 1. Refresh coordinates from ThingsBoard (parallel)
    list(pool.map(lambda d: d.fetch_coords(), devices))

    ready, waiting = [], []
    for dev in devices:
        coords = dev.get_coords()
        if coords:
            ready.append((dev, coords))
        else:
            waiting.append(dev)

    for dev in waiting:
        log.warning("[%s] skipped: no %s/%s shared attributes received yet",
                    dev.name, ATTR_LAT_KEY, ATTR_LON_KEY)

    if not ready:
        log.warning("Cycle skipped: no device has coordinates yet")
        return

    # 2. One batched OSRM call
    distances = osrm_batch_distances_km(ready)
    if not distances:
        return

    # 3. Publish telemetry (parallel)
    def send(item):
        dev, _ = item
        km = distances.get(dev.name)
        if km is None:
            return
        log.info("[%s] distance to charging station: %s km", dev.name, km)
        dev.publish_distance(km)

    list(pool.map(send, ready))


# =====================================================================
# Main
# =====================================================================
def main():
    logging.basicConfig(
        level=LOG_LEVEL,
        format="%(asctime)s | %(levelname)-7s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if STATION_LAT is None or STATION_LON is None:
        log.critical("Set STATION_LAT and STATION_LON in the CONFIG section.")
        sys.exit(1)
    if TB_BASE_URL == "YOUR_THINGSBOARD_URL":
        log.critical("Set TB_BASE_URL in the CONFIG section.")
        sys.exit(1)

    # Load devices from CSV (optional CLI arg overrides DEVICES_CSV)
    csv_path = sys.argv[1] if len(sys.argv) > 1 else DEVICES_CSV
    try:
        device_rows = load_devices_from_csv(csv_path)
    except (OSError, csv.Error, UnicodeDecodeError) as e:
        log.critical("Could not read devices CSV: %s", e)
        sys.exit(1)
    if not device_rows:
        log.critical("No valid devices found in %s", csv_path)
        sys.exit(1)

    log.info("Loaded %d device(s) from %s", len(device_rows), csv_path)
    log.info("Starting (HTTP): %d device(s), interval=%ss, station=(%s, %s), OSRM=%s",
             len(device_rows), INTERVAL_S, STATION_LAT, STATION_LON, OSRM_BASE_URL)

    devices = [Device(d["name"], d["token"]) for d in device_rows]

    next_tick = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(len(devices), 16)) as pool:
        try:
            while True:
                next_tick += INTERVAL_S
                time.sleep(max(0.0, next_tick - time.monotonic()))
                if time.monotonic() - next_tick > INTERVAL_S:   # fell behind: resync
                    next_tick = time.monotonic()
                run_cycle(devices, pool)
        except KeyboardInterrupt:
            log.info("Stopping (Ctrl+C) ...")
        finally:
            for dev in devices:
                dev.session.close()
            log.info("All HTTP sessions closed. Bye.")


if __name__ == "__main__":
    main()