#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Render letter.html by replacing placeholders in letter_newsletter_template.html.

Design goals
- Never leave {{PLACEHOLDER}} strings in output: fill with real values or "—"
- Be resilient to small schema changes (missing keys, renamed columns)
- Keep templates mail-friendly: pure string replacement, no JS

Inputs (all in data/):
- templates/letter_newsletter_template.html
- data/bm20_latest.json
- data/bm20_daily_data_latest.csv
- data/krw_24h_latest.json
- data/btc_usd_series.json (optional)
- data/bm20_history.json (optional, for sentiment)
- data/etf_summary.json (optional)

Output
- output/letter.html
"""

from __future__ import annotations

import sys
import json
import re
from pathlib import Path
from typing import Any, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import requests
from datetime import datetime, timezone, timedelta
import config
from logger import get_logger

log = get_logger("render")

ROOT = Path(__file__).resolve().parent.parent

DATA = ROOT / "data"
TEMPLATE = ROOT / "templates" / "letter_newsletter_template.html"

BM20_JSON = DATA / "bm20_latest.json"
DAILY_CSV = DATA / "bm20_daily_data_latest.csv"
KRW_JSON = DATA / "krw_24h_latest.json"
COIN_NAMES_JSON = DATA / "coin_names_kr.json"
COIN_NAMES_EN_JSON = DATA / "coin_names_en.json"
BTC_JSON = DATA / "btc_usd_series.json"  # optional

BM20_HISTORY_JSON = DATA / "bm20_history.json"  # optional
ETF_JSON          = DATA / "etf_summary.json"  # optional

NEWS_ONELINER_TXT = DATA / "news_one_liner.txt"
NEWS_ONELINER_NOTE_TXT = DATA / "news_one_liner_note.txt"

OUT = ROOT / "output" / "letter.html"

GREEN = "#16a34a"
RED = "#dc2626"
INK = "#0f172a"
MUTED = "#64748b"

# 1x1 transparent gif to avoid broken image boxes in email clients
TRANSPARENT_GIF = "data:image/gif;base64,R0lGODlhAQABAAAAACH5BAEKAAEALAAAAAABAAEAAAICTAEAOw=="


# ─────────────────────────────────────────────────────────
# 실시간 데이터: CoinGecko 티커 + 업비트 Top/Bottom + 프리미엄
# ─────────────────────────────────────────────────────────

def _kst_now() -> str:
    kst = timezone(timedelta(hours=9))
    return datetime.now(kst).strftime("%Y.%m.%d %H:%M")


def load_ticker_from_csv() -> dict[str, str]:
    """Read BTC/ETH/XRP prices from bm20_daily_data_latest.csv (pre-fetched by bm20_daily.py)."""
    fb = {"PRICE": chr(8212), "CHANGE": chr(8212), "COLOR": "ticker-down"}
    fallback = {
        **{f"TICKER_BTC_{k}": v for k, v in fb.items()},
        **{f"TICKER_ETH_{k}": v for k, v in fb.items()},
        **{f"TICKER_XRP_{k}": v for k, v in fb.items()},
        "TICKER_TIME": _kst_now(),
    }
    try:
        df = pd.read_csv(DAILY_CSV)
        df["symbol"] = df["symbol"].astype(str).str.upper()
        result = {}
        for sym in ("BTC", "ETH", "XRP"):
            row = df[df["symbol"] == sym].head(1)
            if row.empty:
                result[f"TICKER_{sym}_PRICE"] = chr(8212)
                result[f"TICKER_{sym}_CHANGE"] = chr(8212)
                result[f"TICKER_{sym}_COLOR"] = "ticker-down"
                continue
            price = float(row.iloc[0]["current_price"])
            chg = float(row.iloc[0]["price_change_pct"])

            if price >= 1_000:
                p_str = f"${price:,.0f}"
            elif price >= 1:
                p_str = f"${price:,.2f}"
            else:
                p_str = f"${price:.4f}"

            arrow = chr(9650) if chg >= 0 else chr(9660)  # ▲ ▼
            cls = "ticker-up" if chg >= 0 else "ticker-down"
            result[f"TICKER_{sym}_PRICE"] = p_str
            result[f"TICKER_{sym}_CHANGE"] = f"{arrow}{abs(chg):.1f}%"
            result[f"TICKER_{sym}_COLOR"] = cls

        result["TICKER_TIME"] = _kst_now()
        log.info("Ticker from CSV (no API call)")
        return result
    except Exception as e:
        log.warning("Load ticker from CSV failed: %s", e)
        return fallback


def fmt_vol_krw(v: float) -> str:
    """거래대금 KRW 단위 포맷: 조/억 단위"""
    if v >= 1_000_000_000_000:
        return f"{v/1_000_000_000_000:.1f}조"
    if v >= 100_000_000:
        return f"{v/100_000_000:.0f}억"
    return f"{v:,.0f}"


def load_upbit_top_bottom_from_file(n: int = 3) -> dict[str, str]:
    """Read upbit top/bottom gainers from krw_24h_latest.json (pre-fetched)."""
    FB = {**{f"UPBIT_TOP{i}_SYMBOL": chr(8212) for i in range(1, n+1)},
          **{f"UPBIT_TOP{i}_CHG":    chr(8212) for i in range(1, n+1)},
          **{f"UPBIT_BOT{i}_SYMBOL": chr(8212) for i in range(1, n+1)},
          **{f"UPBIT_BOT{i}_CHG":    chr(8212) for i in range(1, n+1)}}
    try:
        krw = load_json_optional(KRW_JSON)
        if not krw:
            return FB
        gainers = krw.get("upbit_gainers", [])
        if not gainers:
            return FB
        tops = [g for g in gainers if g.get("side") == "top"][:n]
        bots = [g for g in gainers if g.get("side") == "bottom"][:n]
        result = {}
        for i, g in enumerate(tops, 1):
            sym = g.get("symbol", chr(8212))
            kr = g.get("korean_name", "")
            result[f"UPBIT_TOP{i}_SYMBOL"] = f"{kr}({sym})" if kr else sym
            result[f"UPBIT_TOP{i}_CHG"] = f"+{g['change_pct']:.1f}%"
        for i, g in enumerate(bots, 1):
            sym = g.get("symbol", chr(8212))
            kr = g.get("korean_name", "")
            result[f"UPBIT_BOT{i}_SYMBOL"] = f"{kr}({sym})" if kr else sym
            result[f"UPBIT_BOT{i}_CHG"] = f"{g['change_pct']:.1f}%"
        FB.update(result)
    except Exception as e:
        log.warning("Load upbit top/bottom from file failed: %s", e)
    return FB


def fetch_exchange_vol_top3() -> dict[str, str]:
    """업비트·빗썸·코인원 거래대금 Top3 — krw_24h_latest.json by_exchange_top 에서 읽기"""
    FB = {
        **{f"UPBIT_VOL{i}_SYM":   "—" for i in range(1, 4)},
        **{f"UPBIT_VOL{i}_AMT":   "—" for i in range(1, 4)},
        **{f"BITHUMB_VOL{i}_SYM": "—" for i in range(1, 4)},
        **{f"BITHUMB_VOL{i}_AMT": "—" for i in range(1, 4)},
        **{f"COINONE_VOL{i}_SYM": "—" for i in range(1, 4)},
        **{f"COINONE_VOL{i}_AMT": "—" for i in range(1, 4)},
    }
    try:
        krw = load_json_optional(KRW_JSON)
        if not krw:
            return FB
        by_ex = krw.get("by_exchange_top", {})

        mapping = [
            ("upbit_top5",   "UPBIT"),
            ("bithumb_top5", "BITHUMB"),
            ("coinone_top5", "COINONE"),
        ]
        result = {}
        for key, prefix in mapping:
            entries = by_ex.get(key, [])[:3]
            for i, entry in enumerate(entries, 1):
                sym = entry.get("symbol", "—").replace("KRW-", "")
                val = float(entry.get("value", 0))
                result[f"{prefix}_VOL{i}_SYM"] = sym
                result[f"{prefix}_VOL{i}_AMT"] = fmt_vol_krw(val)
        FB.update(result)
    except Exception as e:
        log.warning("Exchange vol top3 failed: %s", e)
    return FB


def fetch_premium_data(usdkrw: float | None) -> dict[str, str]:
    """Kimchi premium vs Coinbase premium. Reads Upbit/BTC prices from bm20_latest.json, only Coinbase is live."""
    FB = {"KIMCHI_PREM_PCT": "—", "CB_PREMIUM_PCT": "—",
          "PREMIUM_COMMENT": "프리미엄 데이터를 가져올 수 없습니다."}
    try:
        # Read pre-fetched prices from bm20_latest.json (no Upbit/yfinance API calls)
        bm20 = load_json_optional(BM20_JSON)
        km = (bm20.get("kimchi_meta", {}) or {}) if bm20 else {}
        upbit_btc_krw = km.get("btc_krw")
        cg_usd = km.get("btc_usd")
        fx = usdkrw if (usdkrw and usdkrw > 100) else 1510.0

        if not upbit_btc_krw or not cg_usd:
            log.warning("Premium: btc_krw/btc_usd missing in bm20_latest.json")
            return FB

        upbit_btc_krw = float(upbit_btc_krw)
        cg_usd = float(cg_usd)  # 환율 힌트 없으면 하드코딩 폴백
        cb_usd = float(
            requests.get("https://api.coinbase.com/v2/prices/BTC-USD/spot", timeout=10).json()["data"]["amount"])
        upbit_usd  = upbit_btc_krw / fx
        kimchi_pct = (upbit_usd - cg_usd) / cg_usd * 100  # 한국 vs 글로벌
        cb_pct     = (cb_usd - cg_usd) / cg_usd * 100     # 미국(코베) vs 글로벌

        def _c(v: float) -> str:
            color = GREEN if v >= 0 else RED
            sign = "+" if v >= 0 else "-"
            return f'<span style="color:{color};font-weight:900;">{sign}{abs(v):.2f}%</span>'

        if kimchi_pct > 1 and cb_pct > 0:
            comment = "김치·코인베이스 프리미엄 동시 양전 → 글로벌 대비 국내 수요 강세 신호."
        elif kimchi_pct > 1 and cb_pct <= 0:
            comment = "김치 프리미엄 양전, 코인베이스 디스카운트 → 국내 단독 매수세 주의."
        elif kimchi_pct < -0.5:
            comment = "김치 역프리미엄 → 국내 매도 압력 또는 원화 약세 영향 가능성."
        else:
            comment = f"김치 {kimchi_pct:+.2f}% / 코인베이스 {cb_pct:+.2f}% — 중립 구간."

        return {"KIMCHI_PREM_PCT": _c(kimchi_pct), "CB_PREMIUM_PCT": _c(cb_pct),
                "PREMIUM_COMMENT": comment}
    except Exception as e:
        log.warning("Premium fetch failed: %s", e)
        return FB


# ─────────────────────────────────────────────────────────
# 워드프레스 REST API: 태그 기반 뉴스 수집
# ─────────────────────────────────────────────────────────

def _strip_html(text: str) -> str:
    """HTML 태그 제거 + 공백 정리"""
    import re as _re
    return _re.sub(r"<[^>]+>", "", text or "").strip()


def load_etf_summary() -> dict[str, str]:
    """data/etf_summary.json → ETF 플레이스홀더 딕셔너리"""
    FB = {
        "{{ETF_BTC_INFLOW}}": "—", "{{ETF_BTC_AUM}}": "—", "{{ETF_BTC_CUM}}": "—", "{{ETF_BTC_HOLDINGS}}": "—",
        "{{ETF_ETH_INFLOW}}": "—", "{{ETF_ETH_AUM}}": "—", "{{ETF_ETH_CUM}}": "—", "{{ETF_ETH_HOLDINGS}}": "—",
        "{{ETF_SOL_INFLOW}}": "—", "{{ETF_SOL_AUM}}": "—", "{{ETF_SOL_CUM}}": "—", "{{ETF_SOL_HOLDINGS}}": "—",
        "{{ETF_BTC_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_ETH_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_SOL_INFLOW_COLOR}}": "color:#64748b;",
        "{{ETF_COMMENT}}": "ETF 데이터를 불러올 수 없습니다.",
        "{{ETF_ASOF}}": "—",
    }
    if not ETF_JSON.exists():
        log.warning("ETF json not found: %s", ETF_JSON)
        return FB
    try:
        raw = json.loads(ETF_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("ETF json parse error: %s", e)
        return FB

    def _fmt_usd(val, digits=0) -> str:
        """숫자 → 억달러 단위 포맷"""
        try:
            v = float(val)
        except Exception:
            return "—"
        billions = v / 1_000_000_000
        if abs(billions) >= 1:
            return f"${billions:+.1f}B" if digits == 0 else f"${billions:.1f}B"
        millions = v / 1_000_000
        return f"${millions:+.0f}M"

    def _fmt_aum(val) -> str:
        try:
            v = float(val)
            b = v / 1_000_000_000
            return f"${b:.1f}B"
        except Exception:
            return "—"

    def _fmt_holdings(val, sym) -> str:
        try:
            v = float(val)
            return f"{v:,.0f} {sym}"
        except Exception:
            return "—"

    def _inflow_html(val) -> str:
        """inflow 값을 색깔 span으로 감싸서 반환."""
        text = _fmt_usd(val)
        try:
            v = float(val)
            if v > 0:   color = "#16a34a"
            elif v < 0: color = "#dc2626"
            else:        color = "#64748b"
        except Exception:
            color = "#64748b"
        return f'<span style="color:{color};font-weight:900;">{text}</span>'

    def _parse(coin: str, sym: str) -> dict:
        d = raw.get(coin, {})
        inflow_raw = d.get("dailyNetInflow", None)
        return {
            f"{{{{ETF_{sym}_INFLOW}}}}":       _inflow_html(inflow_raw),
            f"{{{{ETF_{sym}_AUM}}}}":           _fmt_aum(d.get("totalNetAssets")),
            f"{{{{ETF_{sym}_CUM}}}}":           _fmt_usd(d.get("cumNetInflow"), digits=0),
            f"{{{{ETF_{sym}_HOLDINGS}}}}":      _fmt_holdings(d.get("totalTokenHoldings"), sym),
            f"{{{{ETF_{sym}_INFLOW_COLOR}}}}":  "",
        }

    result = {}
    result.update(_parse("btc", "BTC"))
    result.update(_parse("eth", "ETH"))
    result.update(_parse("sol", "SOL"))

    # ETF 코멘트 자동 생성
    try:
        btc_v = float(raw.get("btc", {}).get("dailyNetInflow", 0))
        eth_v = float(raw.get("eth", {}).get("dailyNetInflow", 0))
        if btc_v > 0 and eth_v > 0:
            comment = f"BTC·ETH ETF 동시 순유입 — 기관 수급 전반적 우호."
        elif btc_v > 0 and eth_v <= 0:
            comment = f"BTC ETF 순유입, ETH 소폭 유출 — BTC 집중 매수 구간."
        elif btc_v < 0 and eth_v < 0:
            comment = f"BTC·ETH ETF 동시 순유출 — 기관 단기 차익실현 신호."
        else:
            comment = f"ETF 혼조세 — 방향성 확인 필요."
    except Exception:
        comment = "—"

    result["{{ETF_COMMENT}}"] = comment
    result["{{ETF_ASOF}}"] = str(raw.get("updatedAt", "—"))[:10]
    return result


# ------------------ small IO helpers ------------------

def load_json(p: Path) -> Any:
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def load_json_optional(p: Path) -> Any | None:
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def fmt_level(x: float) -> str:
    return f"{float(x):,.2f}"

def fmt_num(x: float, digits: int = 2) -> str:
    return f"{float(x):,.{digits}f}"

def fmt_share_pct(x: float) -> str:
    x = float(x)
    # 0~1 비율로 들어오면 100 곱하기 (0.016 같은 경우)
    # 1~100 범위면 이미 % 단위
    if abs(x) < 1.0:
        x *= 100.0
    return f"{x:.1f}%"

def fmt_krw_big(x: float) -> str:
    x = float(x)
    jo = 1_0000_0000_0000  # 1조
    eok = 1_0000_0000      # 1억
    if x >= jo:
        return f"{x/jo:.2f}조원"
    if x >= eok:
        return f"{x/eok:.1f}억원"
    return f"{x:,.0f}원"

def pct_to_display(x: float) -> float:
    """Accept ratio(<=1.5) or pct; return pct number."""
    x = float(x)
    if abs(x) <= 1.5:
        x *= 100.0
    return x

def colored_change_html(pct_value: float, digits: int = 2, wrap_parens: bool = False) -> str:
    v = float(pct_value)
    if v > 0:
        arrow, color = "▲", GREEN
    elif v < 0:
        arrow, color = "▼", RED
    else:
        arrow, color = "", INK

    s = f"{v:+.{digits}f}%"
    text = f"{arrow} {s}".strip()
    if wrap_parens:
        text = f"({text})"
    return f'<span style="color:{color};font-weight:900;">{text}</span>'

def load_daily_df() -> pd.DataFrame:
    if not DAILY_CSV.exists():
        raise FileNotFoundError(f"Missing {DAILY_CSV}")
    df = pd.read_csv(DAILY_CSV)

    # normalize symbol
    if "symbol" not in df.columns:
        for c in ("ticker", "asset"):
            if c in df.columns:
                df = df.rename(columns={c: "symbol"})
                break

    # normalize price_change_pct
    if "price_change_pct" not in df.columns:
        for c in ("change_pct", "pct_change", "return_1d_pct", "return_1d"):
            if c in df.columns:
                df = df.rename(columns={c: "price_change_pct"})
                break

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce")
    df = df.dropna(subset=["price_change_pct"])
    return df

def compute_best_worst_breadth(df: pd.DataFrame, n=3) -> Tuple[str, str, str, int, int]:
    best = df.sort_values("price_change_pct", ascending=False).head(n)
    worst = df.sort_values("price_change_pct", ascending=True).head(n)

    best_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in best.itertuples()])
    worst_txt = "<br/>".join([f"{r.symbol} {r.price_change_pct:+.2f}%" for r in worst.itertuples()])

    up = int((df["price_change_pct"] > 0).sum())
    down = int((df["price_change_pct"] < 0).sum())
    breadth = f"상승 {up} · 하락 {down}"
    return best_txt, worst_txt, breadth, up, down

def compute_moves_top3(df: pd.DataFrame) -> Tuple[str, str, str]:
    top = df.sort_values("price_change_pct", ascending=False).head(3)
    moves = [f"{r.symbol} {r.price_change_pct:+.2f}%" for r in top.itertuples()]
    while len(moves) < 3:
        moves.append("—")
    return moves[0], moves[1], moves[2]

# ------------------ sentiment + xrp share helpers ------------------

def synth_market_one_line(bm20_dir: str, breadth: str, krw_total: str, kimchi_txt: str) -> str:
    # simple, readable, stable
    parts = []
    if bm20_dir and bm20_dir != "보합":
        parts.append(f"BM20 {bm20_dir}")
    parts.append(breadth)
    if krw_total != "—":
        parts.append(f"KRW 24h {krw_total}")
    if kimchi_txt != "—":
        parts.append(f"김치 {kimchi_txt}")
    return " · ".join(parts) if parts else "—"

def synth_treemap_one_line(best3: str, worst3: str) -> str:
    # Use first line of Best3/Worst3 for quick interpretation
    b = (best3.split("<br/>")[0] if best3 and best3 != "—" else "").strip()
    w = (worst3.split("<br/>")[0] if worst3 and worst3 != "—" else "").strip()
    if b and w:
        return f"상승 선두: {b} / 약세 선두: {w}"
    if b:
        return f"상승 선두: {b}"
    if w:
        return f"약세 선두: {w}"
    return "—"

# ------------------ placeholders ------------------

def _aas_bar_html(onchain: float, social: float, momentum: float) -> str:
    """기여도 바 HTML 생성. 0%인 항목은 td 자체를 생략."""
    segs = [
        (onchain,  "#2563eb"),
        (social,   "#f97316"),
        (momentum, "#16a34a"),
    ]
    tds = ""
    for pct, color in segs:
        if pct <= 0:
            continue
        label = f"{pct}%" if pct >= 4 else ""
        tds += (
            f'<td width="{pct}%" style="background-color:{color};height:28px;'
            f"font-family:-apple-system,BlinkMacSystemFont,'Apple SD Gothic Neo',"
            f"'Noto Sans KR','Malgun Gothic',Arial,sans-serif;"
            f'font-size:9px;font-weight:bold;'
            f'color:#fff;text-align:center;vertical-align:middle;">{label}</td>'
        )
    return (
        '<table role="presentation" width="100%" cellspacing="0" cellpadding="0" '
        'border="0" style="table-layout:fixed;">'
        f'<tr>{tds}</tr></table>'
    )


def _aas_note_tag(text: str) -> str:
    """Comment 텍스트에 따라 색깔 태그 HTML 반환.
    온체인(파랑): 고래 매집, 과매도
    소셜(주황):   관심 집중, 버즈
    모멘텀(초록): 추세 추종, 상승 모멘텀
    """
    t = text.strip()
    SOCIAL_KEYWORDS   = ("관심", "버즈", "소셜")
    MOMENTUM_KEYWORDS = ("추세", "모멘텀", "상승")

    # 이모지 매핑
    if "고래" in t or "매집" in t:
        emoji = "🐋 "
    elif "과매도" in t:
        emoji = "📉 "
    elif any(k in t for k in SOCIAL_KEYWORDS):
        emoji = "🔥 "
    elif any(k in t for k in MOMENTUM_KEYWORDS):
        emoji = "🚀 "
    else:
        emoji = ""

    # 소셜
    if any(k in t for k in SOCIAL_KEYWORDS):
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:13px;font-weight:bold;color:#c2410c;"
    # 모멘텀
    elif any(k in t for k in MOMENTUM_KEYWORDS):
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:13px;font-weight:bold;color:#15803d;"
    # 기본 온체인(파랑)
    else:
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:13px;font-weight:bold;color:#1d4ed8;"
    return f'<span style="{style}">{emoji}{t}</span>'


def fetch_aas_data() -> dict[str, str]:
    """GitHub에서 AAS 데이터를 가져와 실 JSON 키값(대문자 시작)에 맞춰 가공.
    
    KST 오늘 날짜로 먼저 시도 → 없으면 어제 날짜로 재시도.
    (봇 서버가 UTC 기준이라 파일명이 KST 기준 하루 전일 수 있음)
    """
    kst_now = datetime.now(timezone(timedelta(hours=9)))
    # KST 오늘, 어제 순서로 시도
    date_candidates = [
        kst_now.strftime("%Y-%m-%d"),
        (kst_now - timedelta(days=1)).strftime("%Y-%m-%d"),
    ]

    ph = {}
    # 기본값 설정 (데이터 호출 실패 시 레이아웃 유지용)
    for i in range(1, 4):
        ph.update({
            f"{{{{AAS_COIN_{i}}}}}" : "—",
            f"{{{{AAS_SCORE_{i}}}}}" : "0.00",
            f"{{{{AAS_SCORE_PERCENT_{i}}}}}" : "0",
            f"{{{{AAS_CHG_{i}}}}}" : "0.00",
            f"{{{{AAS_NOTE_{i}}}}}" : "—",
            f"{{{{AAS_NOTE_TAG_{i}}}}}": _aas_note_tag("—"),
            f"{{{{AAS_ONCHAIN_{i}}}}}" : "33.3",
            f"{{{{AAS_SOCIAL_{i}}}}}" : "33.3",
            f"{{{{AAS_MOMENTUM_{i}}}}}" : "33.4",
            f"{{{{AAS_BAR_{i}}}}}": _aas_bar_html(33.3, 33.3, 33.4),
        })

    # Private repo access token
    aas_token = config.AAS_BOT_TOKEN
    headers = {"Authorization": f"token {aas_token}"} if aas_token else {}

    data = None
    used_date = None
    errors = []
    for date_str in date_candidates:
        url = f"https://raw.githubusercontent.com/Blockmedia-DataTeam/AAS-Bot/main/reports/daily/{date_str}/newsletter_aas_top3_{date_str}.json"
        try:
            r = requests.get(url, timeout=10, headers=headers)
            r.raise_for_status()
            data = r.json()
            used_date = date_str
            log.info("AAS data fetched for %s", date_str)
            break
        except Exception as e:
            errors.append((date_str, e))

    if data is None and errors:
        for date_str, e in errors:
            log.warning("AAS fetch failed for %s: %s", date_str, e)

    if data is None:
        log.warning("AAS data unavailable for all candidate dates. Using defaults.")
        ph["{{AAS_BODY}}"] = '''<tr><td style="padding:32px 24px;text-align:center;">
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:28px;margin:0 0 12px 0;">🔧</p>
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:14px;font-weight:900;color:#0d1117;margin:0 0 6px 0;">오늘의 코생지 데이터를 준비 중입니다</p>
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;color:#64748b;line-height:1.7;margin:0;">분석 데이터는 매일 업데이트됩니다.<br/>내일 아침 뉴스레터에서 확인해주세요!</p>
</td></tr>'''
        return ph

    coin_names_kr = load_json_optional(COIN_NAMES_JSON) or {}
    coin_names_en = load_json_optional(COIN_NAMES_EN_JSON) or {}

    for i, item in enumerate(data[:3], 1):
        score = float(item.get("AAS", 0))
        score_pct = min(100, int((score / 3.0) * 100))

        note_text = item.get("Comment", "—")
        sym = item.get("Symbol", "—")
        kr = coin_names_kr.get(sym, "")
        en = coin_names_en.get(sym, "")
        if kr:
            coin_label = f"{kr}({sym})"
        elif en:
            coin_label = f"{en}({sym})"
        else:
            coin_label = sym
        ph[f"{{{{AAS_COIN_{i}}}}}"] = coin_label
        ph[f"{{{{AAS_SCORE_{i}}}}}"] = f"{score:.2f}"
        ph[f"{{{{AAS_SCORE_PERCENT_{i}}}}}"] = str(score_pct)
        ph[f"{{{{AAS_CHG_{i}}}}}"] = f"{float(item.get('24H(%)', 0)):+.2f}"
        ph[f"{{{{AAS_NOTE_{i}}}}}"] = note_text
        ph[f"{{{{AAS_NOTE_TAG_{i}}}}}"] = _aas_note_tag(note_text)

        onchain  = float(item.get("Onchain",  33.3))
        social   = float(item.get("Social",   33.3))
        momentum = float(item.get("Momentum", 33.4))
        ph[f"{{{{AAS_ONCHAIN_{i}}}}}"]  = str(onchain)
        ph[f"{{{{AAS_SOCIAL_{i}}}}}"]   = str(social)
        ph[f"{{{{AAS_MOMENTUM_{i}}}}}"] = str(momentum)
        ph[f"{{{{AAS_BAR_{i}}}}}"]      = _aas_bar_html(onchain, social, momentum)

    ph["{{AAS_BODY}}"] = ""  # 데이터 있을 때는 빈값 (템플릿 그대로 사용)
    return ph

def build_placeholders() -> dict[str, str]:
    bm20 = load_json(BM20_JSON)
    krw = load_json(KRW_JSON)
    df = load_daily_df()

    # BTC series (optional)
    btc_usd_txt = "—"
    btc_1d_html = "—"
    if BTC_JSON.exists():
        series = load_json(BTC_JSON)
        try:
            if isinstance(series, list) and len(series) >= 2:
                btc_last = float(series[-1].get("price", series[-1].get("close", 0)))
                btc_prev = float(series[-2].get("price", series[-2].get("close", 0)))
                if btc_last and btc_prev:
                    btc_1d = (btc_last / btc_prev - 1) * 100.0
                    btc_usd_txt = f"{btc_last:,.0f}"
                    btc_1d_html = colored_change_html(btc_1d, digits=2, wrap_parens=False)
        except Exception: pass

    # BM20
    asof = bm20.get("asOf") or bm20.get("asof") or bm20.get("date") or bm20.get("timestamp") or ""
    level = bm20.get("bm20Level", None)
    r1d_raw = (bm20.get("returns", {}) or {}).get("1D", None)

    bm20_1d_pct = None
    bm20_1d_html = "—"
    direction = "보합"
    if r1d_raw is not None:
        bm20_1d_pct = pct_to_display(r1d_raw)
        bm20_1d_html = colored_change_html(bm20_1d_pct, digits=2, wrap_parens=False)
        if bm20_1d_pct > 0: direction = "반등"
        elif bm20_1d_pct < 0: direction = "약세"

    best3, worst3, breadth, up, down = compute_best_worst_breadth(df, n=3)
    move1, move2, move3 = compute_moves_top3(df)

    # Comment chip
    chip_color = GREEN if (bm20_1d_pct or 0) > 0 else (RED if (bm20_1d_pct or 0) < 0 else INK)
    comment_chip = f'<span style="font-weight:900;color:{chip_color};">{direction}</span>'
    comment = f"BM20 {direction}, {breadth}"

    # Kimchi & KRW
    kimchi_p = bm20.get("kimchi_premium_pct", None)
    kimchi_html = colored_change_html(float(kimchi_p)) if kimchi_p is not None else "—"
    usdkrw = (bm20.get("kimchi_meta", {}) or {}).get("usdkrw", None)
    usdkrw_txt = fmt_num(usdkrw, 2) if usdkrw is not None else "—"

    totals = (krw.get("totals", {}) or {})
    combined = totals.get("combined_24h", None)
    krw_total_txt = fmt_krw_big(combined) if combined is not None else "—"
    
    upbit_v, bith_v, coin_v = totals.get("upbit_24h"), totals.get("bithumb_24h"), totals.get("coinone_24h")
    upbit_share = (float(upbit_v)/float(combined)*100) if combined and upbit_v else None
    bith_share = (float(bith_v)/float(combined)*100) if combined and bith_v else None
    coin_share = (float(coin_v)/float(combined)*100) if combined and coin_v else None

    # Sentiment & Korea Signals
    sentiment_label, sentiment_score = ("—", "—")
    hist_obj = load_json_optional(BM20_HISTORY_JSON)
    if hist_obj:
        try:
            latest_entry = hist_obj[-1] if isinstance(hist_obj, list) else hist_obj.get("latest", hist_obj)
            sent_data = latest_entry.get("sentiment", {})
            sentiment_label = str(sent_data.get("status") or sent_data.get("sentiment_label") or "—")
            score = sent_data.get("value") or sent_data.get("sentiment_score")
            if score is not None: sentiment_score = f"{float(score):.0f}"
        except Exception: pass

    # News placeholders are filled by the mailing system, not here

    # Global Index

    # SUBSCRIBE URL
    subscribe_url = "https://blockmedia.co.kr/kr"

    ph = {
        "{{BM20_LEVEL}}": fmt_level(level) if level is not None else "—",
        "{{BM20_ASOF}}": str(asof)[:10] if asof else "—",
        "{{BM20_1D}}": bm20_1d_html,
        "{{BM20_BREADTH}}": breadth,
        "{{BM20_COMMENT}}": comment,
        "{{BM20_CHIP}}": comment_chip,
        "{{BTC_USD}}": btc_usd_txt,
        "{{BTC_1D}}": btc_1d_html,
        "{{SENTIMENT_LABEL}}": sentiment_label,
        "{{SENTIMENT_SCORE}}": sentiment_score,
        "{{MARKET_ONE_LINE}}": synth_market_one_line(direction, breadth, krw_total_txt, kimchi_html),
        "{{TREEMAP_ONE_LINE}}": synth_treemap_one_line(best3, worst3),
        "{{MOVE_1}}": move1, "{{MOVE_2}}": move2, "{{MOVE_3}}": move3,
        "{{KRW_TOTAL_24H}}": krw_total_txt,
        "{{KRW_ASOF_KST}}": (str(asof)[:10] if asof else "—"),
        "{{USDKRW}}": f"₩{usdkrw_txt}" if usdkrw_txt != "—" else "—",
        "{{UPBIT_SHARE_24H}}": fmt_share_pct(upbit_share) if upbit_share else "—",
        "{{BITHUMB_SHARE_24H}}": fmt_share_pct(bith_share) if bith_share else "—",
        "{{COINONE_SHARE_24H}}": fmt_share_pct(coin_share) if coin_share else "—",
        "{{LETTER_DATETIME}}": (lambda t: f"{t.year}년 {t.month}월 {t.day}일, {t.strftime('%H:%M')}")(datetime.now(timezone(timedelta(hours=9)))),

    }

    # 🚀 AAS 데이터 업데이트 (여기서 BONK, PEPE 데이터가 주입됩니다)
    ph.update(fetch_aas_data())

    # ETF & 실시간 티커 데이터 업데이트
    ph.update(load_etf_summary())
    usdkrw_f = float(str(usdkrw).replace(",", "")) if usdkrw else None
    for k, v in load_ticker_from_csv().items(): ph["{{" + k + "}}"] = v
    for k, v in load_upbit_top_bottom_from_file(n=3).items(): ph["{{" + k + "}}"] = v
    for k, v in fetch_exchange_vol_top3().items(): ph["{{" + k + "}}"] = v
    for k, v in fetch_premium_data(usdkrw_f).items(): ph["{{" + k + "}}"] = v

    ph["SUBSCRIBE_URL"] = subscribe_url
    return ph

def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing {TEMPLATE}")
    html = TEMPLATE.read_text(encoding="utf-8")
    ph = build_placeholders()
    # 긴 키부터 치환 (겹침 방지)
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))
    
    # Placeholders left for the mailing system to fill
    EXTERNAL_PLACEHOLDERS = {
        "{{UNSUB_URL}}", "{{SUBSCRIBE_URL}}", "{{INTRO_TITLE}}", "{{INTRO_TEXT}}",
        "{{HEADLINE_NEWS_TITLE}}", "{{HEADLINE_NEWS_EXCERPT}}",
        "{{NEWS1_CATEGORY}}", "{{NEWS1_TITLE}}", "{{NEWS1_EXCERPT}}", "{{NEWS1_LINK}}",
        "{{NEWS2_CATEGORY}}", "{{NEWS2_TITLE}}", "{{NEWS2_EXCERPT}}", "{{NEWS2_LINK}}",
        "{{NEWS3_CATEGORY}}", "{{NEWS3_TITLE}}", "{{NEWS3_EXCERPT}}", "{{NEWS3_LINK}}",
    }
    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)) - EXTERNAL_PLACEHOLDERS)
    if left: log.warning("Unfilled placeholders: %s", left)
    
    OUT.write_text(html, encoding="utf-8")
    log.info("Output ready: %s", OUT)

if __name__ == "__main__":
    render()
