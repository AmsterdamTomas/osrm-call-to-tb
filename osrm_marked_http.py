"""
E-trike -> charging station distance publisher (batched OSRM + ThingsBoard HTTP)

Flow (every INTERVAL_S seconds):
  1. GET each device's SHARED ATTRIBUTES (LA/LO) from ThingsBoard, in parallel.
  2. Make ONE OSRM `table` call: all trikes = sources, charging station = destination.
  3. Extract only the distance (meters -> km) for each trike, matched by source order.
  4. POST {"chargeStationDistance": <km>} to each device's telemetry endpoint
     using that device's own access token, in parallel.

Install:   pip install requests
Run:       python chargestation_distance_http.py
"""

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import requests

# =====================================================================
# CONFIG  -- edit this section only
# =====================================================================

# ThingsBoard HTTP base URL (include scheme and HTTP port, default 8080 / 443 for HTTPS).
# NOTE: the old MQTT script used port 1883; HTTP uses a different port, so check your tunnel.
TB_BASE_URL = "https://sikloweb.localto.net/"
TB_TIMEOUT_S = 2.5            # per ThingsBoard request

# One entry per device. To scale to 30 devices, just add more entries.
DEVICES = [
    {"name": "ETRIKE_21", "token": "GOzNT5MOTex8WuJwM0Fu"},
    {"name": "ETRIKE_22", "token": "aR8FQtNB5zAysJCNG7Wd"},
    {"name": "ETRIKE_23", "token": "hiRji0GHKHpjVxD9YR0K"},
]

# Charging station coordinates
STATION_LAT = 14.566447065332111
STATION_LON = 120.99211867786774

# Public demo OSRM server
OSRM_BASE_URL = "https://router.project-osrm.org"
OSRM_PROFILE = "driving"      # the public demo server is driving-only
OSRM_TIMEOUT_S = 4

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
# OSRM batched call
# =====================================================================
def osrm_batch_distances_km(ready):
    """
    ready: list of (Device, (lat, lon)).
    Returns {device_name: km} or None if the whole call failed.
    """
    coords = [f"{lon:.6f},{lat:.6f}" for _, (lat, lon) in ready]   # OSRM wants lon,lat
    coords.append(f"{STATION_LON:.6f},{STATION_LAT:.6f}")           # destination = last
    n = len(ready)
    sources = ";".join(str(i) for i in range(n))

    url = (f"{OSRM_BASE_URL}/table/v1/{OSRM_PROFILE}/{';'.join(coords)}"
           f"?sources={sources}&destinations={n}&annotations=distance")

    try:
        resp = requests.get(url, timeout=OSRM_TIMEOUT_S,
                            headers={"User-Agent": "etrike-chargestation-distance/1.0"})
        resp.raise_for_status()
        body = resp.json()
    except requests.exceptions.RequestException as e:
        log.error("OSRM CALL FAILED: %s", e)
        return None
    except ValueError:
        log.error("OSRM CALL FAILED: response was not valid JSON")
        return None

    if body.get("code") != "Ok" or "distances" not in body:
        log.error("OSRM CALL FAILED: code=%s message=%s",
                  body.get("code"), body.get("message"))
        return None

    log.info("OSRM CALL SUCCESS: %d device(s) in one batched request", n)

    result = {}
    for i, (dev, _) in enumerate(ready):
        meters = body["distances"][i][0]      # row i = source i, col 0 = charging station
        if meters is None:
            log.warning("[%s] OSRM found no route to the charging station", dev.name)
            continue
        result[dev.name] = round(meters / 1000.0, DISTANCE_DECIMALS)
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
    if distances is None:
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

    log.info("Starting (HTTP): %d device(s), interval=%ss, station=(%s, %s), OSRM=%s",
             len(DEVICES), INTERVAL_S, STATION_LAT, STATION_LON, OSRM_BASE_URL)

    devices = [Device(d["name"], d["token"]) for d in DEVICES]

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