"""
SoSoValue ETF data collection
==============================
Fetches BTC/ETH/SOL Spot ETF metrics and saves summary to data/.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import json
import urllib3
from datetime import datetime, timezone
import config

DATA = config.DATA_DIR

# Disable SSL verification (api.sosovalue.xyz cert issue workaround)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BASE_URL = "https://api.sosovalue.xyz"

ETF_TYPES = {
    "btc": "us-btc-spot",
    "eth": "us-eth-spot",
    "sol": "us-sol-spot",
}


def _headers() -> dict:
    return {
        "x-soso-api-key": config.SOSOVALUE_API_KEY,
        "Content-Type": "application/json",
    }


def fetch_current_metrics(etf_type: str) -> dict:
    """Fetch current ETF metrics (AUM, daily net inflow, etc.)"""
    r = requests.post(
        f"{BASE_URL}/openapi/v2/etf/currentEtfDataMetrics",
        headers=_headers(),
        json={"type": etf_type},
        timeout=15,
        verify=False,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise Exception(f"API error: {data.get('msg')}")
    return data["data"]


def save_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"  [OK] saved: {path.name}")


def main():
    if not config.SOSOVALUE_API_KEY:
        print("[SKIP] SOSOVALUE_API_KEY not set, skipping ETF fetch")
        return

    updated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"[{updated_at}] ETF data collection started")

    all_summary = {"updatedAt": updated_at, "btc": {}, "eth": {}, "sol": {}}

    for coin, etf_type in ETF_TYPES.items():
        print(f"\n--- {coin.upper()} ({etf_type}) ---")

        try:
            metrics = fetch_current_metrics(etf_type)
            all_summary[coin] = {
                "totalNetAssets": metrics.get("totalNetAssets", {}).get("value"),
                "dailyNetInflow": metrics.get("dailyNetInflow", {}).get("value"),
                "cumNetInflow": metrics.get("cumNetInflow", {}).get("value"),
                "dailyTotalValueTraded": metrics.get("dailyTotalValueTraded", {}).get("value"),
                "totalTokenHoldings": metrics.get("totalTokenHoldings", {}).get("value"),
                "lastUpdateDate": metrics.get("dailyNetInflow", {}).get("lastUpdateDate"),
            }
            print(f"  AUM: ${float(metrics['totalNetAssets']['value'])/1e9:.2f}B")
            print(f"  Daily net inflow: ${float(metrics['dailyNetInflow']['value'])/1e6:.1f}M")
        except Exception as e:
            print(f"  ❌ metrics failed: {e}")

    save_json(DATA / "etf_summary.json", all_summary)
    print(f"\n[OK] Done: {updated_at}")


if __name__ == "__main__":
    main()
