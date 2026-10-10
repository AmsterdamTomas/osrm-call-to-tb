# E-Trike Charging Station Distance Publisher

Python service that computes the road distance from each e-trike to its assigned charging station. It reads live GPS coordinates from ThingsBoard, queries OSRM in batched calls every 5 seconds, and publishes each trike's distance (km) back to ThingsBoard. Built for e-trike fleet management and designed to scale from a few devices to 30+.

## How It Works

Every `INTERVAL_S` seconds (default 5):

1. **Read from ThingsBoard** (in parallel, per device)
   - Shared attributes `LA` / `LO` (latitude / longitude) using the device access token.
   - Server attribute `depot_location` using a ThingsBoard tenant user login. This selects the device's charging station.
2. **Query OSRM** (public demo server). Devices are grouped by depot, and each depot gets one batched `table` request: that depot's trikes are the sources and its charging station is the destination. Large groups are split into chunks of `OSRM_MAX_BATCH`.
3. **Extract only the distance**, converted from meters to km.
4. **Publish to ThingsBoard** (in parallel, per device) under the telemetry key `chargeStationDistance`, using each device's own access token.

A one-time startup flag (`a = STARTUP_VALUE`) is also sent per device after the first successful ThingsBoard exchange, to confirm the program is talking to the dashboard.

### Depots

| `depot_location` value | Latitude | Longitude |
|------------------------|----------|-----------|
| `las pinas`            | 14.4542  | 120.9767  |
| `taft`                 | 14.5664  | 120.9920  |

Matching is case-insensitive. A device with a missing or unrecognized `depot_location` is skipped, with a warning in the console.

## Directory Structure

```
.
├── chargestation_distance_http.py     # Main program
├── devices_access_token_profiles.csv  # Device list (vehicle_name, access_token)
└── README.md
```

## Requirements

- Python 3.8 or newer
- Python package: [`requests`](https://pypi.org/project/requests/)
- Network access to your ThingsBoard HTTP endpoint and to `https://router.project-osrm.org`

## ThingsBoard Prerequisites

| Item | Where | Notes |
|------|-------|-------|
| `LA`, `LO` | Device **shared attributes** | Kept up to date by your rule chain (copied from device telemetry). |
| `depot_location` | Device **server attribute** | Must be `las pinas` or `taft`. |
| Device access tokens | One per device | Listed in the CSV. |
| Tenant user (username and password) | ThingsBoard | Needed because server attributes cannot be read with a device access token. |
| Device names | ThingsBoard | `vehicle_name` in the CSV must **exactly match** the device name in ThingsBoard. |

## Setup

### 1. Get the files

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

### 2. Create a virtual environment (recommended)

**Windows (PowerShell)**
```powershell
python -m venv venv
venv\Scripts\Activate.ps1
```

**macOS / Linux**
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install requests
```

### 4. Prepare the devices CSV

Create `devices_access_token_profiles.csv` next to the script:

```csv
vehicle_name,access_token
ETRIKE_21,<access_token_for_ETRIKE_21>
ETRIKE_22,<access_token_for_ETRIKE_22>
ETRIKE_23,<access_token_for_ETRIKE_23>
```

- Column A = `vehicle_name`, column B = `access_token`. Extra columns are ignored.
- The first row is treated as a header and skipped.
- Blank rows, rows missing a name or token, and duplicate names are skipped, with a warning in the console.
- To scale to more devices, add more rows. No code changes are needed.

### 5. Edit the CONFIG section

Open `chargestation_distance_http.py` and edit the CONFIG section at the top:

| Setting | Default | Description |
|---------|---------|-------------|
| `TB_BASE_URL` | `https://sikloweb.localto.net/` | ThingsBoard HTTP base URL (scheme and port included if needed). |
| `TB_TIMEOUT_S` | `2.5` | Timeout per ThingsBoard request (seconds). |
| `TB_USERNAME` / `TB_PASSWORD` | placeholders | **Required.** ThingsBoard tenant user login. |
| `DEVICES_CSV` | `devices_access_token_profiles.csv` | Path to the devices CSV. |
| `DEPOT_ATTR_KEY` | `depot_location` | Server attribute that selects the charging station. |
| `DEPOTS` | Las Pinas, Taft | Charging station coordinates per depot (lowercase keys). |
| `OSRM_BASE_URL` | `https://router.project-osrm.org` | Public demo OSRM server. |
| `OSRM_TIMEOUT_S` | `4` | OSRM request timeout (seconds). |
| `OSRM_MAX_BATCH` | `50` | Max trikes per OSRM table call. |
| `INTERVAL_S` | `5` | Seconds between cycles. |
| `ATTR_LAT_KEY` / `ATTR_LON_KEY` | `LA` / `LO` | Shared attribute keys for coordinates. |
| `TELEMETRY_KEY` | `chargeStationDistance` | Telemetry key sent to ThingsBoard. |
| `STARTUP_KEY` / `STARTUP_VALUE` | `a` / `2` | One-time startup indicator. |
| `DISTANCE_DECIMALS` | `3` | Rounding of the km value. |
| `LOG_LEVEL` | `logging.INFO` | Use `logging.DEBUG` to log every coordinate update. |

> **Security:** the CSV contains device access tokens and the script contains your ThingsBoard login. Do not commit real credentials to a public repository. Add the CSV to `.gitignore` and keep placeholders in the committed script.

## Run

```bash
python chargestation_distance_http.py
```

Or with a different CSV:

```bash
python chargestation_distance_http.py path/to/other_devices.csv
```

Stop with `Ctrl+C`.

## Console Logs

The program logs to the console with timestamps. Key events:

| Event | Example message |
|-------|-----------------|
| Startup | `Loaded 3 device(s) from ...` / `Starting (HTTP): ...` |
| ThingsBoard login | `ThingsBoard user login SUCCESS` |
| Coordinates received | `[ETRIKE_21] coordinates available: LA=... LO=...` |
| Depot read | `[ETRIKE_21] depot_location = taft` |
| Startup flag | `STARTUP FLAG SUCCESS` / `STARTUP FLAG FAILED` |
| OSRM call | `OSRM CALL SUCCESS: 2 device(s) for depot 'taft' in one request` / `OSRM UNAVAILABLE ...` |
| Transmission | `TRANSMISSION SUCCESS: chargeStationDistance=1.234 km` / `TRANSMISSION FAILED ...` |
| Skipped device | `[ETRIKE_22] skipped: missing LA/LO shared attributes or valid depot_location` |

## Rate Limits and Scaling

- OSRM calls are batched **per depot**, so with two depots the program makes about 2 OSRM requests per cycle (0.4 requests/second at the default 5 s), regardless of fleet size. This stays under the public demo server's fair-use guideline of roughly 1 request/second.
- ThingsBoard requests grow with fleet size: each cycle makes about two reads (shared attributes and the server attribute) plus one telemetry send per device. The device ID lookup happens once per device.
- The public demo OSRM server is best-effort with no uptime guarantee. For production reliability, self-host OSRM and change `OSRM_BASE_URL`.

## Troubleshooting

| Symptom | Likely cause and fix |
|---------|----------------------|
| `Set TB_USERNAME and TB_PASSWORD...` at startup | Fill in the login placeholders in the CONFIG section. |
| `Devices CSV not found` | Check `DEVICES_CSV` or pass the path as an argument. |
| `DEPOT FETCH FAILED` for a device | `vehicle_name` doesn't match the ThingsBoard device name, or the tenant user lacks permission. |
| `unknown depot_location '...'` | The attribute value isn't `las pinas` or `taft`. |
| `ATTRIBUTE FETCH FAILED` | Wrong access token, or `TB_BASE_URL` is unreachable (check your tunnel). |
| `skipped: missing LA/LO shared attributes` | The rule chain hasn't populated `LA`/`LO` for that device yet. |
| `OSRM UNAVAILABLE` | Network issue, timeout, or the public server is rate limiting or down. |
| `OSRM rejected coordinates` | A device's `LA`/`LO` can't be snapped to a road; check its GPS values. |
| Connection refused when using MQTT ports | This version uses HTTP only. Use the HTTP URL of your ThingsBoard or tunnel. |
