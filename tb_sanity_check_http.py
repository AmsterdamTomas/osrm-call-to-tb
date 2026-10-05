"""
ThingsBoard HTTP sanity check: sends {"a": 1} to ONE device using its access token.

Install:   pip install requests
Run:       python tb_sanity_check_http.py
"""

import sys

import requests

# ===================== CONFIG =====================
TB_BASE_URL = "https://sikloweb.localto.net"
ACCESS_TOKEN = "GOzNT5MOTex8WuJwM0Fu"   # PLACEHOLDER

TELEMETRY_KEY = "a"
TELEMETRY_VALUE = 1
TIMEOUT_S = 10
# ==================================================


def main():
    if ACCESS_TOKEN == "YOUR_DEVICE_ACCESS_TOKEN":
        print("Set ACCESS_TOKEN in the CONFIG section.")
        sys.exit(1)

    url = f"{TB_BASE_URL.rstrip('/')}/api/v1/{ACCESS_TOKEN}/telemetry"
    payload = {TELEMETRY_KEY: TELEMETRY_VALUE}

    print(f"[..] POST {TB_BASE_URL}/api/v1/<ACCESS_TOKEN>/telemetry  body={payload}")
    try:
        resp = requests.post(url, json=payload, timeout=TIMEOUT_S)
    except requests.exceptions.RequestException as e:
        print(f"[FAIL] Request error: {e}")
        sys.exit(1)

    if resp.status_code == 200:
        print(f"[OK] ThingsBoard accepted: {TELEMETRY_KEY}={TELEMETRY_VALUE} (HTTP 200)")
        sys.exit(0)

    print(f"[FAIL] HTTP {resp.status_code}: {resp.text[:200]}")
    if resp.status_code == 401:
        print("       -> Access token not recognized. Check ACCESS_TOKEN.")
    sys.exit(1)


if __name__ == "__main__":
    main()