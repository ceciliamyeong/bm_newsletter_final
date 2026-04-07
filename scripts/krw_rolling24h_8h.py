#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

"""
KRW Rolling 24h Dashboard Pipeline (8h snapshots)
- Meaning: "Rolling 24h KRW traded value" snapshot updated every 8 hours.
- Exchanges: Upbit, Bithumb, Coinone
- Outputs:
  out/history/
    └─ krw_24h_latest.json

Notes:
- Exchange APIs typically provide rolling 24h traded value, not discrete 8h volume.
- Therefore, snapshots overlap. That is intended for "current market board" view.
"""

import sys
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import config
from logger import get_logger

log = get_logger("krw_volume")

# -------------------------
# Time / Paths
# -------------------------
KST = timezone(timedelta(hours=9))

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

LATEST_JSON = DATA_DIR / "krw_24h_latest.json"
COIN_NAMES_JSON = DATA_DIR / "coin_names_kr.json"
COIN_NAMES_EN_JSON = DATA_DIR / "coin_names_en.json"


# -------------------------
# API Endpoints
# -------------------------
UPBIT_MARKETS = "https://api.upbit.com/v1/market/all"
UPBIT_TICKER = "https://api.upbit.com/v1/ticker"

BITHUMB_TICKER_ALL = "https://api.bithumb.com/public/ticker/ALL_KRW"

COINONE_TICKER = "https://api.coinone.co.kr/public/v2/ticker_new/KRW"

CMC_MAP_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/map"
CMC_CACHE_MAX_AGE = 24 * 3600  # 24 hours in seconds

# -------------------------
# HTTP helper
# -------------------------
def http_get(url: str, params=None):
    last_err = None
    for _ in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(1)
    raise RuntimeError(f"Failed request: {url} ({last_err})")

def now_kst() -> datetime:
    return datetime.now(tz=KST)

# -------------------------
# CMC coin name dictionary
# -------------------------
def fetch_cmc_coin_names():
    """Fetch symbol->english_name map from CMC. Uses cached file if < 24h old."""
    if COIN_NAMES_EN_JSON.exists():
        age = time.time() - COIN_NAMES_EN_JSON.stat().st_mtime
        if age < CMC_CACHE_MAX_AGE:
            log.info("CMC coin names cache is fresh (%.1fh old), skipping API call", age / 3600)
            return
    api_key = config.CMC_API_KEY
    if not api_key:
        log.warning("CMC_API_KEY missing, skipping coin names fetch")
        return
    try:
        r = requests.get(CMC_MAP_URL,
                         headers={"X-CMC_PRO_API_KEY": api_key},
                         params={"listing_status": "active", "limit": 5000},
                         timeout=20)
        r.raise_for_status()
        data = r.json().get("data", [])
        name_map = {item["symbol"]: item["name"] for item in data if item.get("symbol")}
        write_json(COIN_NAMES_EN_JSON, name_map)
        log.info("Saved %d CMC English coin names to %s", len(name_map), COIN_NAMES_EN_JSON.name)
    except Exception as e:
        log.warning("CMC coin names fetch failed: %s", e)


# -------------------------
# Fetch per exchange (pairs)
# returns List[(symbol, krw_24h_value)]
# symbol format: KRW-XXX
# -------------------------
def fetch_upbit_pairs() -> tuple[List[Tuple[str, float]], List[Dict], Dict[str, str]]:
    """Returns (volume_pairs, raw_tickers, name_map) — name_map: symbol->korean_name."""
    markets = http_get(UPBIT_MARKETS, {"isDetails": "false"})
    krw_markets = [m["market"] for m in markets if m.get("market", "").startswith("KRW-")]
    name_map = {m["market"].replace("KRW-", ""): m.get("korean_name", "")
                for m in markets if m.get("market", "").startswith("KRW-")}
    write_json(COIN_NAMES_JSON, name_map)
    log.info("Saved %d coin Korean names to %s", len(name_map), COIN_NAMES_JSON.name)
    out: List[Tuple[str, float]] = []
    raw_tickers: List[Dict] = []
    for i in range(0, len(krw_markets), 100):
        chunk = krw_markets[i:i+100]
        tickers = http_get(UPBIT_TICKER, {"markets": ",".join(chunk)})
        for t in tickers:
            sym = t.get("market")
            val = float(t.get("acc_trade_price_24h", 0) or 0)
            if sym:
                out.append((sym, val))
                raw_tickers.append(t)
        time.sleep(0.1)
    return out, raw_tickers, name_map

def fetch_bithumb_pairs() -> List[Tuple[str, float]]:
    j = http_get(BITHUMB_TICKER_ALL)
    data = j.get("data", {})
    out: List[Tuple[str, float]] = []
    for sym, v in data.items():
        if sym == "date":
            continue
        # Different docs sometimes use these keys; try safe fallbacks.
        vv = (v or {})
        val = (
            vv.get("acc_trade_value_24H")
            or vv.get("acc_trade_value_24h")
            or vv.get("acc_trade_value")
            or 0
        )
        out.append((f"KRW-{sym}", float(val or 0)))
    return out

def fetch_coinone_pairs() -> List[Tuple[str, float]]:
    j = http_get(COINONE_TICKER)
    out: List[Tuple[str, float]] = []
    for t in j.get("tickers", []):
        sym = (t.get("target_currency") or "").upper()
        # Coinone uses quote_volume for KRW quote volume (rolling window)
        val = float(t.get("quote_volume", 0) or 0)
        if sym:
            out.append((f"KRW-{sym}", val))
    return out

# -------------------------
# Aggregation
# -------------------------
def sum_total(pairs: List[Tuple[str, float]]) -> float:
    return float(sum(v for _, v in pairs))

def merge_maps(*pairs_lists: List[Tuple[str, float]]) -> Dict[str, float]:
    m: Dict[str, float] = {}
    for pairs in pairs_lists:
        for sym, val in pairs:
            if not sym:
                continue
            m[sym] = m.get(sym, 0.0) + float(val or 0.0)
    return m

def topn_from_map(m: Dict[str, float], n: int = 10) -> List[Tuple[str, float]]:
    items = sorted(m.items(), key=lambda x: x[1], reverse=True)
    return items[:n]

# -------------------------
# Stablecoin Intelligence
# -------------------------
STABLES = {"USDT", "USDC", "DAI", "PYUSD"}

def analyze_stables(combined_map: Dict[str, float], total_vol: float) -> Dict:
    stable_data: Dict[str, float] = {}
    total_stable_vol = 0.0

    for sym, vol in combined_map.items():
        # sym expected: "KRW-XXX"
        asset = sym.split("-", 1)[1] if "-" in sym else sym
        asset = asset.upper()
        if asset in STABLES:
            stable_data[asset] = float(vol or 0.0)
            total_stable_vol += float(vol or 0.0)

    dominance = (total_stable_vol / total_vol * 100.0) if total_vol > 0 else 0.0

    return {
        "total_stable_vol_24h": total_stable_vol,
        "stable_dominance_pct": dominance,
        "by_asset": stable_data
    }

# -------------------------
# IO
# -------------------------
def safe_read_json(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None

def write_json(path: Path, obj):
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

# -------------------------
# Main pipeline
# -------------------------
def run():
    ts = now_kst()
    ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S%z")  # e.g., 2026-01-24T09:05:00+0900
    ts_label = ts.strftime("%m/%d %H:%M KST")

    up, upbit_raw_tickers, upbit_name_map = fetch_upbit_pairs()
    bt = fetch_bithumb_pairs()
    co = fetch_coinone_pairs()
    fetch_cmc_coin_names()

    up_total = sum_total(up)
    bt_total = sum_total(bt)
    co_total = sum_total(co)

    combined_map = merge_maps(up, bt, co)
    combined_total = float(sum(combined_map.values()))

    stable_info = analyze_stables(combined_map, combined_total)

    def topn_pairs(pairs: List[Tuple[str, float]], n: int = 5) -> List[Dict]:
        items = sorted(pairs, key=lambda x: x[1], reverse=True)[:n]
        return [{"symbol": sym, "value": float(val)} for sym, val in items]

    up_top5 = topn_pairs(up, 5)
    bt_top5 = topn_pairs(bt, 5)
    co_top5 = topn_pairs(co, 5)

    top10_items = topn_from_map(combined_map, 10)
    top10_total = float(sum(v for _, v in top10_items))
    rest_total = max(0.0, combined_total - top10_total)
    top10_share = (top10_total / combined_total * 100.0) if combined_total > 0 else 0.0

    top10 = []
    for sym, val in top10_items:
        top10.append({
            "symbol": sym,
            "value": float(val),
            "share_pct": (float(val) / combined_total * 100.0) if combined_total > 0 else 0.0
        })

    # Upbit top/bottom gainers by 24h change rate (reused by render_letter.py)
    upbit_gainers = []
    if upbit_raw_tickers:
        sorted_by_change = sorted(upbit_raw_tickers,
                                  key=lambda x: x.get("signed_change_rate", 0),
                                  reverse=True)
        top3 = sorted_by_change[:3]
        bot3 = list(reversed(sorted_by_change[-3:]))
        for t in top3:
            sym = t.get("market", "").replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            kr = upbit_name_map.get(sym, sym)
            upbit_gainers.append({"symbol": sym, "korean_name": kr, "change_pct": round(pct, 2), "side": "top"})
        for t in bot3:
            sym = t.get("market", "").replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            kr = upbit_name_map.get(sym, sym)
            upbit_gainers.append({"symbol": sym, "korean_name": kr, "change_pct": round(pct, 2), "side": "bottom"})

    latest = {
        "schema": "krw_rolling24h_v1",
        "timestamp_kst": ts_iso,
        "timestamp_label": ts_label,
        "totals": {
            "combined_24h": combined_total,
            "upbit_24h": up_total,
            "bithumb_24h": bt_total,
            "coinone_24h": co_total,
        },
        "stablecoins": stable_info,
        "by_exchange_top": {
            "upbit_top5": up_top5,
            "bithumb_top5": bt_top5,
            "coinone_top5": co_top5
        },

        "top10": {
            "top10_total_24h": top10_total,
            "rest_total_24h": rest_total,
            "top10_share_pct": top10_share,
            "coins": top10
        },
        "upbit_gainers": upbit_gainers,
    }

    write_json(LATEST_JSON, latest)

    log.info("KRW 24h snapshot saved: total=%s top10=%.1f%% stable=%.1f%%",
             f"{combined_total:,.0f}", top10_share, stable_info['stable_dominance_pct'])

if __name__ == "__main__":
    run()
