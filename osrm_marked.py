"""
E-trike -> charging station distance publisher (batched OSRM + ThingsBoard MQTT)
 
Flow (every INTERVAL_S seconds):
  1. Take the latest LA/LO of every device (kept up to date from each device's
     ThingsBoard SHARED ATTRIBUTES, which your rule chain fills in).
  2. Make ONE OSRM `table` call: all trikes = sources, charging station = destination.
  3. Extract only the distance (meters -> km) for each trike, matched by source order.
  4. Publish {"chargeStationDistance": <km>} to each device via MQTT using that
     device's own access token.
 
Install:   pip install "paho-mqtt>=2.0" requests
Run:       python chargestation_distance.py
"""
 
import json
import logging
import sys
import threading
import time
 
import paho.mqtt.client as mqtt
import requests
 
# =====================================================================
# CONFIG  -- edit this section only
# =====================================================================
 
# ThingsBoard MQTT broker (PLACEHOLDER: replace with your host)
TB_HOST = "sikloweb.localto.net"
TB_PORT = 1883
 
# One entry per device. To scale to 30 devices, just add more entries.
# "token" is the device's ThingsBoard access token (PLACEHOLDER).
DEVICES = [
    {"name": "ETRIKE_21", "token": "GOzNT5MOTex8WuJwM0Fu"},
    {"name": "ETRIKE_22", "token": "aR8FQtNB5zAysJCNG7Wd"},
    {"name": "ETRIKE_23", "token": "hiRji0GHKHpjVxD9YR0K"},
]
 
# Charging station coordinates (PLACEHOLDERS: set your preset values)
STATION_LAT = 14.566447065332111   # e.g. 15.0000
STATION_LON = 120.99211867786774   # e.g. 120.0000
 
# Public demo OSRM server
OSRM_BASE_URL = "https://router.project-osrm.org"
OSRM_PROFILE = "driving"      # the public demo server is driving-only
OSRM_TIMEOUT_S = 4            # must be < INTERVAL_S
 
# Timing: OSRM call + ThingsBoard send
INTERVAL_S = 5
 
# ThingsBoard keys
ATTR_LAT_KEY = "LA"                      # shared attribute: latitude
ATTR_LON_KEY = "LO"                      # shared attribute: longitude
TELEMETRY_KEY = "chargeStationDistance"  # key sent to ThingsBoard
 
# One-time startup indicator: sent once per device after the first successful
# MQTT connection, to confirm the program is talking to the ThingsBoard dashboard.
STARTUP_KEY = "a"
STARTUP_VALUE = 1
 
DISTANCE_DECIMALS = 3                    # km rounding (configurable)
LOG_LEVEL = logging.INFO                 # set logging.DEBUG for every attribute update
 
# =====================================================================
# ThingsBoard MQTT device API topics
# =====================================================================
TOPIC_TELEMETRY = "v1/devices/me/telemetry"
TOPIC_ATTR = "v1/devices/me/attributes"
TOPIC_ATTR_RESP = "v1/devices/me/attributes/response/+"
TOPIC_ATTR_REQ = "v1/devices/me/attributes/request/{req_id}"
 
log = logging.getLogger("chargestation")
 
 
# =====================================================================
# Per-device state + MQTT client
# =====================================================================
class Device:
    def __init__(self, name, token):
        self.name = name
        self.token = token
        self.lat = None
        self.lon = None
        self.connected = False
        self.startup_sent = False  # one-time startup indicator already sent?
        self.pending = {}          # mid -> (key, value), for PUBACK confirmation logs
        self._req_id = 0
        self.lock = threading.Lock()
 
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"chargestation-distance-{name}",
            userdata=self,
        )
        self.client.username_pw_set(token)
        self.client.on_connect = _on_connect
        self.client.on_disconnect = _on_disconnect
        self.client.on_message = _on_message
        self.client.on_publish = _on_publish
 
    # ---- connection ----
    def start(self):
        log.info("[%s] connecting to MQTT %s:%s ...", self.name, TB_HOST, TB_PORT)
        self.client.connect_async(TB_HOST, TB_PORT, keepalive=30)
        self.client.loop_start()   # auto-reconnects in the background
 
    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()
 
    # ---- shared attributes ----
    def request_shared_attributes(self):
        self._req_id += 1
        topic = TOPIC_ATTR_REQ.format(req_id=self._req_id)
        payload = json.dumps({"sharedKeys": f"{ATTR_LAT_KEY},{ATTR_LON_KEY}"})
        self.client.publish(topic, payload, qos=1)
        log.info("[%s] requested current shared attributes (%s, %s)",
                 self.name, ATTR_LAT_KEY, ATTR_LON_KEY)
 
    def update_coords(self, data):
        """Accepts a flat dict that may contain LA and/or LO."""
        changed = False
        with self.lock:
            first_fix = self.lat is None or self.lon is None
            try:
                if ATTR_LAT_KEY in data:
                    self.lat = float(data[ATTR_LAT_KEY])
                    changed = True
                if ATTR_LON_KEY in data:
                    self.lon = float(data[ATTR_LON_KEY])
                    changed = True
            except (TypeError, ValueError):
                log.warning("[%s] received non-numeric coordinate data: %s", self.name, data)
                return
            lat, lon = self.lat, self.lon
            ready_now = lat is not None and lon is not None
 
        if changed and ready_now:
            if first_fix:
                log.info("[%s] coordinates available: LA=%s LO=%s", self.name, lat, lon)
            else:
                log.debug("[%s] coordinates updated: LA=%s LO=%s", self.name, lat, lon)
 
    def get_coords(self):
        with self.lock:
            if self.lat is None or self.lon is None:
                return None
            return self.lat, self.lon
 
    # ---- telemetry ----
    def publish_startup_flag(self):
        """Send {STARTUP_KEY: STARTUP_VALUE} exactly once per program run."""
        payload = json.dumps({STARTUP_KEY: STARTUP_VALUE})
        with self.lock:
            if self.startup_sent:
                return
            info = self.client.publish(TOPIC_TELEMETRY, payload, qos=1)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error("[%s] STARTUP FLAG FAILED: publish error rc=%s (%s=%s)",
                          self.name, info.rc, STARTUP_KEY, STARTUP_VALUE)
                return
            self.startup_sent = True
            self.pending[info.mid] = (STARTUP_KEY, STARTUP_VALUE)
        log.info("[%s] startup flag queued: %s", self.name, payload)
 
    def publish_distance(self, km):
        payload = json.dumps({TELEMETRY_KEY: km})
        with self.lock:
            if not self.connected:
                log.error("[%s] TRANSMISSION FAILED: MQTT not connected (%s=%s km)",
                          self.name, TELEMETRY_KEY, km)
                return
            info = self.client.publish(TOPIC_TELEMETRY, payload, qos=1)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                log.error("[%s] TRANSMISSION FAILED: publish error rc=%s (%s=%s km)",
                          self.name, info.rc, TELEMETRY_KEY, km)
                return
            self.pending[info.mid] = (TELEMETRY_KEY, km)
        log.debug("[%s] telemetry queued: %s", self.name, payload)
 
 
# =====================================================================
# MQTT callbacks (paho-mqtt 2.x signatures)
# =====================================================================
def _on_connect(client, dev, flags, reason_code, properties):
    if reason_code.is_failure:
        log.error("[%s] MQTT connection FAILED: %s", dev.name, reason_code)
        return
    with dev.lock:
        dev.connected = True
    log.info("[%s] MQTT connected", dev.name)
    client.subscribe([(TOPIC_ATTR, 1), (TOPIC_ATTR_RESP, 1)])
    log.info("[%s] subscribed to shared attribute updates", dev.name)
    dev.request_shared_attributes()
    dev.publish_startup_flag()   # one-time only; skipped automatically on reconnects
 
 
def _on_disconnect(client, dev, disconnect_flags, reason_code, properties):
    with dev.lock:
        dev.connected = False
    log.warning("[%s] MQTT disconnected (%s); auto-reconnect active", dev.name, reason_code)
 
 
def _on_message(client, dev, msg):
    try:
        data = json.loads(msg.payload)
    except (ValueError, UnicodeDecodeError):
        log.warning("[%s] received non-JSON message on %s", dev.name, msg.topic)
        return
    # Attribute request responses wrap values in {"shared": {...}}.
    # Pushed shared-attribute updates arrive as a flat {"LA":..., "LO":...}.
    if isinstance(data, dict) and isinstance(data.get("shared"), dict):
        data = data["shared"]
    if isinstance(data, dict):
        dev.update_coords(data)
 
 
def _on_publish(client, dev, mid, reason_code, properties):
    with dev.lock:
        sent = dev.pending.pop(mid, None)
    if sent is None:   # ignore attribute-request publishes
        return
    key, value = sent
    if key == TELEMETRY_KEY:
        log.info("[%s] TRANSMISSION SUCCESS: %s=%s km (broker acknowledged)",
                 dev.name, key, value)
    else:
        log.info("[%s] STARTUP FLAG SUCCESS: %s=%s (broker acknowledged)",
                 dev.name, key, value)
 
 
# =====================================================================
# OSRM batched call
# =====================================================================
def osrm_batch_distances_km(ready):
    """
    ready: list of (Device, (lat, lon)).
    Returns {device_name: km} or None if the whole call failed.
    Only the distance is extracted from the OSRM response.
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
# One 5-second cycle
# =====================================================================
def run_cycle(devices):
    ready, waiting = [], []
    for dev in devices:
        coords = dev.get_coords()
        (ready if coords else waiting).append((dev, coords) if coords else dev)
 
    for dev in waiting:
        log.warning("[%s] skipped: no %s/%s shared attributes received yet",
                    dev.name, ATTR_LAT_KEY, ATTR_LON_KEY)
 
    if not ready:
        log.warning("Cycle skipped: no device has coordinates yet")
        return
 
    distances = osrm_batch_distances_km(ready)
    if distances is None:
        return
 
    for dev, _ in ready:
        km = distances.get(dev.name)
        if km is None:
            continue
        log.info("[%s] distance to charging station: %s km", dev.name, km)
        dev.publish_distance(km)
 
 
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
    if TB_HOST == "YOUR_THINGSBOARD_HOST":
        log.critical("Set TB_HOST in the CONFIG section.")
        sys.exit(1)
 
    log.info("Starting: %d device(s), interval=%ss, station=(%s, %s), OSRM=%s",
             len(DEVICES), INTERVAL_S, STATION_LAT, STATION_LON, OSRM_BASE_URL)
 
    devices = [Device(d["name"], d["token"]) for d in DEVICES]
    for dev in devices:
        dev.start()
 
    next_tick = time.monotonic()
    try:
        while True:
            next_tick += INTERVAL_S
            time.sleep(max(0.0, next_tick - time.monotonic()))
            if time.monotonic() - next_tick > INTERVAL_S:   # fell behind: resync
                next_tick = time.monotonic()
            run_cycle(devices)
    except KeyboardInterrupt:
        log.info("Stopping (Ctrl+C) ...")
    finally:
        for dev in devices:
            dev.stop()
        log.info("All MQTT clients disconnected. Bye.")
 
 
if __name__ == "__main__":
    main()