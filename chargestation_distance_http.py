"""
E-trike -> charging station distance publisher (batched OSRM + ThingsBoard HTTP)

Flow (every INTERVAL_S seconds):
  1. GET each device's SHARED ATTRIBUTES (LA/LO) from ThingsBoard, in parallel.
     GET each device's SERVER ATTRIBUTE (depot_location) -> picks its charging station.
  2. Make ONE OSRM `table` call PER DEPOT (chunked if large): that depot's trikes = sources,
     the depot's charging station = destination.
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
import threading
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

# Server attributes can NOT be read with a device access token (device API only exposes
# client/shared attributes), so a ThingsBoard tenant user login is used to read them.
TB_USERNAME = "tithesis2026@gmail.com"   # PLACEHOLDER
TB_PASSWORD = "ReyAmsterLorenz2026"   # PLACEHOLDER

# CSV listing all devices. Column A = vehicle_name, column B = access_token.
# First row is treated as a header and skipped. Extra columns are ignored.
# vehicle_name must match the device name in ThingsBoard exactly (used to look up the
# device ID for reading its server attribute).
# Can be overridden from the command line: python script.py other.csv
DEVICES_CSV = "devices_access_token_profiles.csv"

# Server attribute that selects each device's charging station
DEPOT_ATTR_KEY = "depot_location"

# Charging station coordinates per depot (lat, lon). Keys are lowercase.
DEPOTS = {
    "las_pinas": (14.4542, 120.9767),
    "taft": (14.5664, 120.9920),
}

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
# ThingsBoard user login (needed only to read SERVER attributes)
# =====================================================================
class TbAuth:
    def __init__(self):
        self.token = None
        self.lock = threading.Lock()

    def _login(self):
        resp = requests.post(
            f"{TB_BASE_URL.rstrip('/')}/api/auth/login",
            json={"username": TB_USERNAME, "password": TB_PASSWORD},
            timeout=TB_TIMEOUT_S,
        )
        resp.raise_for_status()
        self.token = resp.json()["token"]
        log.info("ThingsBoard user login SUCCESS")

    def get(self, path, **kwargs):
        """Authenticated GET; logs in on first use and re-logs in once on a 401 (expired JWT)."""
        url = f"{TB_BASE_URL.rstrip('/')}{path}"
        with self.lock:
            if self.token is None:
                self._login()
            token = self.token
        resp = requests.get(url, headers={"X-Authorization": f"Bearer {token}"},
                            timeout=TB_TIMEOUT_S, **kwargs)
        if resp.status_code == 401:
            with self.lock:
                if self.token == token:   # nobody refreshed it yet
                    self._login()
                token = self.token
            resp = requests.get(url, headers={"X-Authorization": f"Bearer {token}"},
                                timeout=TB_TIMEOUT_S, **kwargs)
        resp.raise_for_status()
        return resp


AUTH = TbAuth()


# =====================================================================
# Per-device state + HTTP helpers
# =====================================================================
class Device:
    def __init__(self, name, token):
        self.name = name
        self.token = token
        self.lat = None
        self.lon = None
        self.depot = None                   # lowercase depot_location, once known
        self.device_id = None               # ThingsBoard device UUID (looked up once)
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

    # ---- server attribute: depot_location ----
    def fetch_depot(self):
        """Read the depot_location SERVER attribute. Keeps last known value on failure."""
        try:
            if self.device_id is None:
                resp = AUTH.get("/api/tenant/devices", params={"deviceName": self.name})
                self.device_id = resp.json()["id"]["id"]
                log.info("[%s] device ID resolved", self.name)
            resp = AUTH.get(
                f"/api/plugins/telemetry/DEVICE/{self.device_id}/values/attributes/SERVER_SCOPE",
                params={"keys": DEPOT_ATTR_KEY},
            )
            attrs = resp.json()
        except requests.exceptions.RequestException as e:
            log.error("[%s] DEPOT FETCH FAILED: %s", self.name, e)
            return
        except (ValueError, KeyError, TypeError):
            log.error("[%s] DEPOT FETCH FAILED: unexpected response format", self.name)
            return

        value = next((a.get("value") for a in attrs if a.get("key") == DEPOT_ATTR_KEY), None)
        if value is None:
            log.warning("[%s] server attribute '%s' is not set", self.name, DEPOT_ATTR_KEY)
            self.depot = None
            return

        depot = str(value).strip().lower()
        if depot not in DEPOTS:
            log.warning("[%s] unknown %s '%s' (expected one of: %s)",
                        self.name, DEPOT_ATTR_KEY, value, ", ".join(DEPOTS))
            self.depot = None
            return

        if depot != self.depot:
            log.info("[%s] %s = %s", self.name, DEPOT_ATTR_KEY, depot)
        self.depot = depot

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
# OSRM batched call (per depot, chunked, with bad-coordinate isolation)
# =====================================================================
class OsrmBadInput(Exception):
    """OSRM rejected the request (e.g. NoSegment / InvalidQuery): likely one bad coordinate."""

class OsrmUnavailable(Exception):
    """Timeout, network error, 429 or 5xx: retrying with smaller batches will not help."""


def _osrm_table(chunk, depot):
    """One OSRM table call for a list of (Device, (lat, lon)) of one depot. Returns {name: km}."""
    station_lat, station_lon = DEPOTS[depot]
    coords = [f"{lon:.6f},{lat:.6f}" for _, (lat, lon) in chunk]   # OSRM wants lon,lat
    coords.append(f"{station_lon:.6f},{station_lat:.6f}")           # destination = last
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


def _osrm_with_isolation(chunk, depot):
    """Try the chunk; if OSRM rejects it, bisect to isolate the offending device(s)."""
    try:
        result = _osrm_table(chunk, depot)
        log.info("OSRM CALL SUCCESS: %d device(s) for depot '%s' in one request", len(chunk), depot)
        return result
    except OsrmUnavailable as e:
        log.error("OSRM UNAVAILABLE (%d device(s) of depot '%s' skipped this cycle): %s",
                  len(chunk), depot, e)
        return {}
    except OsrmBadInput as e:
        if len(chunk) == 1:
            dev, (lat, lon) = chunk[0]
            log.warning("[%s] OSRM rejected coordinates (%s, %s): %s", dev.name, lat, lon, e)
            return {}
        log.warning("OSRM rejected a %d-device batch for depot '%s' (%s); splitting to isolate bad input",
                    len(chunk), depot, e)
        mid = len(chunk) // 2
        return {**_osrm_with_isolation(chunk[:mid], depot),
                **_osrm_with_isolation(chunk[mid:], depot)}


def osrm_batch_distances_km(ready):
    """
    ready: list of (Device, (lat, lon)).
    Groups devices by depot, then makes one OSRM call per depot (split into chunks of
    OSRM_MAX_BATCH). Returns {device_name: km} (possibly empty).
    """
    by_depot = {}
    for item in ready:
        by_depot.setdefault(item[0].depot, []).append(item)

    result = {}
    for depot, group in by_depot.items():
        for i in range(0, len(group), OSRM_MAX_BATCH):
            result.update(_osrm_with_isolation(group[i:i + OSRM_MAX_BATCH], depot))
    return result


# =====================================================================
# One cycle
# =====================================================================
def run_cycle(devices, pool):
    # 1. Refresh coordinates (shared attrs) and depot (server attr) from ThingsBoard (parallel)
    list(pool.map(lambda d: (d.fetch_coords(), d.fetch_depot()), devices))

    ready, waiting = [], []
    for dev in devices:
        coords = dev.get_coords()
        if coords and dev.depot:
            ready.append((dev, coords))
        else:
            waiting.append(dev)

    for dev in waiting:
        log.warning("[%s] skipped: missing %s/%s shared attributes or valid %s",
                    dev.name, ATTR_LAT_KEY, ATTR_LON_KEY, DEPOT_ATTR_KEY)

    if not ready:
        log.warning("Cycle skipped: no device has coordinates and a valid depot yet")
        return

    # 2. Batched OSRM calls (one per depot)
    distances = osrm_batch_distances_km(ready)
    if not distances:
        return

    # 3. Publish telemetry (parallel)
    def send(item):
        dev, _ = item
        km = distances.get(dev.name)
        if km is None:
            return
        log.info("[%s] distance to charging station (%s): %s km", dev.name, dev.depot, km)
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

    if TB_BASE_URL == "YOUR_THINGSBOARD_URL":
        log.critical("Set TB_BASE_URL in the CONFIG section.")
        sys.exit(1)
    if TB_USERNAME == "YOUR_TB_USERNAME" or TB_PASSWORD == "YOUR_TB_PASSWORD":
        log.critical("Set TB_USERNAME and TB_PASSWORD in the CONFIG section.")
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
    log.info("Starting (HTTP): %d device(s), interval=%ss, depots=%s, OSRM=%s",
             len(device_rows), INTERVAL_S, ", ".join(DEPOTS), OSRM_BASE_URL)

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