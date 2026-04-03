#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BM20 Daily Index Calculator
============================
Generates bm20_latest.json and bm20_daily_data_latest.csv from Yahoo Finance data.

BM20 composition: BTC 30% / ETH 20% / XRP 5% / USDT 5% / BNB 5% / 15 others equal (~2.33% each)
Price source: yfinance (free, no API key required)
Kimchi premium: Upbit KRW-BTC vs Binance BTC-USD

Outputs (in data/):
  - bm20_latest.json         (index level, returns, kimchi premium)
  - bm20_daily_data_latest.csv (20 coins: symbol, prices, change%, weight, contribution)
"""

import sys
import os
import json
import time
import datetime as dt
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import pandas as pd
import yfinance as yf
import config
from logger import get_logger

log = get_logger("bm20_daily")

# ── Paths ──────────────────────────────────────────────
DATA = config.DATA_DIR
KST = timezone(timedelta(hours=9))
YMD = datetime.now(KST).strftime("%Y-%m-%d")

LATEST_JSON = DATA / "bm20_latest.json"
DAILY_CSV = DATA / "bm20_daily_data_latest.csv"

# Cache for kimchi premium
CACHE_DIR = DATA / "cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)
KP_CACHE = CACHE_DIR / "kimchi_last.json"


# ── Helpers ────────────────────────────────────────────
def fmt_pct(v, digits=2):
    try:
        if v is None:
            return "-"
        return f"{float(v):.{digits}f}%"
    except Exception:
        return "-"


def write_json(path: Path, obj: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def read_json(path: Path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ── Universe & Mapping ─────────────────────────────────
BM20_IDS = [
    # Fixed weight (5 coins)
    "bitcoin", "ethereum", "ripple", "tether", "binancecoin",
    # Equal weight (15 coins)
    "solana", "usd-coin", "dogecoin", "tron", "cardano",
    "hyperliquid", "chainlink", "sui", "avalanche-2", "stellar",
    "bitcoin-cash", "hedera-hashgraph", "litecoin", "shiba-inu", "toncoin",
]

YF_MAP = {
    "bitcoin": "BTC-USD", "ethereum": "ETH-USD", "ripple": "XRP-USD",
    "tether": "USDT-USD", "binancecoin": "BNB-USD", "solana": "SOL-USD",
    "usd-coin": "USDC-USD", "dogecoin": "DOGE-USD", "tron": "TRX-USD",
    "cardano": "ADA-USD", "hyperliquid": "HYPE32196-USD", "chainlink": "LINK-USD",
    "sui": "SUI20947-USD", "avalanche-2": "AVAX-USD", "stellar": "XLM-USD",
    "bitcoin-cash": "BCH-USD", "hedera-hashgraph": "HBAR-USD",
    "litecoin": "LTC-USD", "shiba-inu": "SHIB-USD", "toncoin": "TON11419-USD",
}

SYMBOL_MAP = {
    "bitcoin": "BTC", "ethereum": "ETH", "ripple": "XRP", "tether": "USDT",
    "binancecoin": "BNB", "solana": "SOL", "usd-coin": "USDC", "dogecoin": "DOGE",
    "tron": "TRX", "cardano": "ADA", "hyperliquid": "HYPE", "chainlink": "LINK",
    "sui": "SUI", "avalanche-2": "AVAX", "stellar": "XLM", "bitcoin-cash": "BCH",
    "hedera-hashgraph": "HBAR", "litecoin": "LTC", "shiba-inu": "SHIB", "toncoin": "TON",
}

FIXED_WEIGHTS = {
    "bitcoin": 0.30, "ethereum": 0.20, "ripple": 0.05,
    "tether": 0.05, "binancecoin": 0.05,
}


# ── Price fetching ─────────────────────────────────────
def fetch_yf_prices(ids: list[str]) -> pd.DataFrame:
    """Fetch current and previous day prices for all BM20 coins via yfinance."""
    pairs = {cid: YF_MAP.get(cid) for cid in ids}
    tickers = [t for t in pairs.values() if t]
    if not tickers:
        raise RuntimeError("No Yahoo tickers mapped.")

    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=4)

    raw = yf.download(
        tickers=tickers, start=str(start), end=str(end + timedelta(days=1)),
        interval="1d", auto_adjust=True, progress=False, group_by="ticker"
    )

    def pick_close(df):
        if isinstance(df.columns, pd.MultiIndex):
            lvl1 = set(df.columns.get_level_values(1))
            use = "Close" if "Close" in lvl1 else ("Adj Close" if "Adj Close" in lvl1 else None)
            return df.xs(use, axis=1, level=1) if use else pd.DataFrame()
        else:
            if "Close" in df.columns:
                return df[["Close"]]
            if "Adj Close" in df.columns:
                return df[["Adj Close"]]
        return pd.DataFrame()

    close = pick_close(raw)

    # Individual fallback if batch download fails
    if close is None or close.empty:
        cols = {}
        for t in tickers:
            h = yf.download(
                tickers=t, start=str(start), end=str(end + timedelta(days=1)),
                interval="1d", auto_adjust=True, progress=False
            )
            if h is not None and not h.empty:
                col = "Close" if "Close" in h.columns else ("Adj Close" if "Adj Close" in h.columns else None)
                if col:
                    cols[t] = h[col]
        if cols:
            close = pd.DataFrame(cols)

    if close is None or close.empty:
        raise RuntimeError("yfinance returned empty close prices.")

    close = close.ffill().dropna(how="all")

    if close.shape[0] < 2:
        log.warning("yfinance close rows=%d (1D change may be 0%%)", close.shape[0])

    last = close.iloc[-1]
    prev = close.iloc[-2] if close.shape[0] >= 2 else close.iloc[-1]
    rev = {v: k for k, v in pairs.items() if v in last.index}

    rows = []
    for tkr, cur in last.dropna().items():
        cid = rev.get(tkr)
        if not cid:
            continue
        pre = float(prev.get(tkr, cur))
        chg24 = (float(cur) / float(pre) - 1.0) * 100.0 if pre else 0.0
        rows.append({
            "id": cid, "sym": SYMBOL_MAP.get(cid, cid.upper()),
            "current_price": float(cur), "previous_price": float(pre),
            "price_change_pct": chg24
        })

    # Fill missing coins with NaN
    got = {r["id"] for r in rows}
    for m in ids:
        if m in got:
            continue
        rows.append({
            "id": m, "sym": SYMBOL_MAP.get(m, m.upper()),
            "current_price": float("nan"), "previous_price": float("nan"),
            "price_change_pct": float("nan")
        })
    return pd.DataFrame(rows)


# ── Kimchi premium ─────────────────────────────────────
def _http_get(url, params=None, retry=3, timeout=12):
    last = None
    for i in range(retry):
        try:
            r = requests.get(url, params=params, timeout=timeout,
                             headers={"User-Agent": "BM20/1.0"})
            if r.status_code == 429:
                time.sleep(1.0 * (i + 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(0.6 * (i + 1))
    raise last


def get_kimchi() -> tuple:
    """Calculate kimchi premium: Upbit KRW-BTC vs Binance BTC-USD."""
    try:
        u = _http_get("https://api.upbit.com/v1/ticker", {"markets": "KRW-BTC"})
        btc_krw = float(u[0]["trade_price"])
    except Exception:
        cached = read_json(KP_CACHE)
        if cached:
            return cached.get("kimchi_pct"), {**cached, "is_cache": True}
        return None, {"is_cache": True, "usdkrw": 1510.0}

    # Global BTC price from Binance
    btc_usd = None
    for base in ["https://api.binance.com", "https://data-api.binance.vision"]:
        try:
            j = _http_get(f"{base}/api/v3/ticker/price", {"symbol": "BTCUSDT"})
            btc_usd = float(j["price"])
            break
        except Exception:
            continue

    if btc_usd is None:
        try:
            y = yf.Ticker("BTC-USD").history(period="2d")["Close"]
            btc_usd = float(y.iloc[-1])
        except Exception:
            cached = read_json(KP_CACHE)
            if cached:
                return cached.get("kimchi_pct"), {**cached, "is_cache": True}
            return None, {"is_cache": True, "usdkrw": 1510.0}

    # USD/KRW exchange rate
    try:
        h = yf.Ticker("USDKRW=X").history(period="2d")
        usdkrw = float(h["Close"].dropna().iloc[-1])
        if not (900 <= usdkrw <= 2000):
            raise ValueError
    except Exception:
        usdkrw = 1510.0

    kp = ((btc_krw / usdkrw) - btc_usd) / btc_usd * 100
    meta = {
        "btc_krw": round(btc_krw, 2),
        "btc_usd": round(btc_usd, 2),
        "usdkrw": round(usdkrw, 2),
        "kimchi_pct": round(kp, 6),
        "is_cache": False,
        "ts": int(time.time()),
    }
    write_json(KP_CACHE, meta)
    return kp, meta


# ── Weights ────────────────────────────────────────────
def compute_weights(ids_all: list[str]) -> dict[str, float]:
    """BTC 30%, ETH 20%, XRP/USDT/BNB 5% each, rest 15 equal split."""
    fixed_sum = sum(FIXED_WEIGHTS.values())  # 0.65
    ids_rest = [cid for cid in ids_all if cid not in FIXED_WEIGHTS]
    n = len(ids_rest)
    w_rest = (1.0 - fixed_sum) / max(1, n)
    w = {cid: FIXED_WEIGHTS.get(cid, w_rest) for cid in ids_all}
    # Micro-correction to ensure sum == 1.0
    s = sum(w.values())
    if abs(s - 1.0) > 1e-12:
        w[ids_all[-1]] += (1.0 - s)
    return w


# ── BM20 level (SSOT) ─────────────────────────────────
def _load_series_ssot() -> tuple:
    """Load SSOT index series from bm20_series.json or backfill CSV."""
    import csv
    candidates = [
        DATA / "backfill_current_basket.csv",
        config.ROOT / "bm20_series.json",
        DATA / "bm20_series.json",
    ]
    for p in candidates:
        try:
            if not p.exists():
                continue
            if p.name.endswith(".csv"):
                rows = []
                with p.open("r", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        d = (row.get("date") or "").strip()[:10]
                        v = row.get("index") or row.get("level") or row.get("bm20Level")
                        if not d or v is None:
                            continue
                        try:
                            rows.append({"date": d, "level": float(v)})
                        except Exception:
                            continue
                if rows:
                    rows.sort(key=lambda x: x["date"])
                    return rows, str(p)
            else:
                obj = read_json(p)
                if isinstance(obj, list) and obj:
                    rows = []
                    for it in obj:
                        if not isinstance(it, dict):
                            continue
                        d = str(it.get("date", "")).strip()[:10]
                        v = it.get("level")
                        if d and v is not None:
                            try:
                                rows.append({"date": d, "level": float(v)})
                            except Exception:
                                pass
                    if rows:
                        rows.sort(key=lambda x: x["date"])
                        return rows, str(p)
        except Exception:
            continue
    return None, None


def _level_on_or_before(rows, target_ymd: str):
    for r in reversed(rows):
        if r["date"] <= target_ymd:
            return float(r["level"])
    return None


def _to_ratio(x):
    """Normalize percent or ratio to ratio (e.g. 12.3% -> 0.123)."""
    if x is None:
        return None
    try:
        v = float(x)
        return v / 100.0 if abs(v) >= 2.0 else v
    except Exception:
        return None


# ── Main ───────────────────────────────────────────────
def main():
    log.info("BM20 Daily: %s", YMD)

    # 1) Fetch prices
    df = fetch_yf_prices(BM20_IDS)
    log.info("Prices fetched: %d coins", len(df))

    # 2) Weights
    weights_map = compute_weights(df["id"].tolist())
    df["weight_ratio"] = df["id"].map(weights_map).astype(float)

    # 3) Contribution
    df["contribution"] = (df["current_price"] - df["previous_price"]) * df["weight_ratio"]

    today_value = float((df["current_price"] * df["weight_ratio"]).sum())
    prev_value = float((df["previous_price"] * df["weight_ratio"]).sum())
    if prev_value == 0 or pd.isna(prev_value):
        prev_value = today_value

    # 4) BM20 level from SSOT
    port_ret_1d = 0.0
    denom_ok = True
    for _, row in df.iterrows():
        cid = row["id"]
        w = float(weights_map.get(cid, 0.0))
        p0 = float(row.get("previous_price", 0.0))
        p1 = float(row.get("current_price", 0.0))
        if w == 0:
            continue
        if p0 <= 0 or p1 <= 0 or pd.isna(p0) or pd.isna(p1):
            denom_ok = False
            continue
        port_ret_1d += w * ((p1 / p0) - 1.0)

    rows_ssot, ssot_src = _load_series_ssot()
    if rows_ssot:
        last_date = rows_ssot[-1]["date"]
        last_level = float(rows_ssot[-1]["level"])
        if last_date == YMD:
            prev_dt = (dt.datetime.strptime(YMD, "%Y-%m-%d") - dt.timedelta(days=1)).strftime("%Y-%m-%d")
            prev_level = _level_on_or_before(rows_ssot, prev_dt) or last_level
            bm20_now = prev_level * (1.0 + port_ret_1d) if denom_ok else last_level
            bm20_prev_level = prev_level
        else:
            bm20_prev_level = last_level
            bm20_now = last_level * (1.0 + port_ret_1d) if denom_ok else last_level
            rows_ssot.append({"date": YMD, "level": float(bm20_now)})
        log.info("BM20 level from SSOT: %s", ssot_src)
    else:
        # Fallback: simple weighted average ratio
        BASE_INDEX_START = 100.0
        bm20_now = today_value
        bm20_prev_level = prev_value
        log.warning("No SSOT found, using raw weighted average as level")

    # 5) Kimchi premium
    kimchi_pct, kp_meta = get_kimchi()
    usdkrw = kp_meta.get("usdkrw", 1510.0) if kp_meta else 1510.0
    log.info("Kimchi premium: %s", fmt_pct(kimchi_pct))

    # 6) Returns
    bm20ChangePct = None
    if bm20_prev_level not in (None, 0):
        bm20ChangePct = (float(bm20_now) / float(bm20_prev_level)) - 1.0

    # 7) Write bm20_latest.json
    latest_obj = {
        "asOf": YMD,
        "bm20Level": round(float(bm20_now), 6),
        "bm20PrevLevel": round(float(bm20_prev_level), 6) if bm20_prev_level is not None else None,
        "bm20PointChange": round(float(bm20_now - bm20_prev_level), 6) if bm20_prev_level is not None else None,
        "bm20ChangePct": bm20ChangePct,
        "returns": {
            "1D": bm20ChangePct,
        },
        "breadth": {
            "up": int((df["price_change_pct"] > 0).sum()),
            "down": int((df["price_change_pct"] < 0).sum()),
        },
        "kimchi_premium_pct": kimchi_pct,
        "kimchi_meta": {
            "usdkrw": usdkrw,
            "btc_krw": kp_meta.get("btc_krw") if kp_meta else None,
            "btc_usd": kp_meta.get("btc_usd") if kp_meta else None,
        },
    }

    write_json(LATEST_JSON, latest_obj)
    log.info("Written: %s", LATEST_JSON.name)

    # 8) Update bm20_series.json (SSOT)
    if rows_ssot:
        SERIES_JSON = DATA / "bm20_series.json"
        write_json(SERIES_JSON, rows_ssot)
        log.info("Written: %s (%d entries)", SERIES_JSON.name, len(rows_ssot))

    # 9) Write CSV
    df_out = df[["sym", "id", "current_price", "previous_price", "price_change_pct", "weight_ratio", "contribution"]]
    df_out = df_out.rename(columns={"sym": "symbol"})
    df_out.to_csv(DAILY_CSV, index=False, encoding="utf-8")
    log.info("Written: %s", DAILY_CSV.name)

    # 10) Summary
    log.info("BM20 Level: %.2f | 1D: %s | Kimchi: %s", bm20_now, fmt_pct(bm20ChangePct * 100 if bm20ChangePct else 0), fmt_pct(kimchi_pct))


if __name__ == "__main__":
    main()
