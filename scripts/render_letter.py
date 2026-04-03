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
- data/k_xrp_share_24h_latest.json (optional)
- data/etf_summary.json (optional)
- data/nasdaq_series.json (optional)
- data/kospi_series.json (optional)
- data/top_news_latest.json (optional)

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

ROOT = Path(__file__).resolve().parent.parent

DATA = ROOT / "data"
TEMPLATE = ROOT / "templates" / "letter_newsletter_template.html"

BM20_JSON = DATA / "bm20_latest.json"
DAILY_CSV = DATA / "bm20_daily_data_latest.csv"
KRW_JSON = DATA / "krw_24h_latest.json"
BTC_JSON = DATA / "btc_usd_series.json"  # optional

BM20_HISTORY_JSON = DATA / "bm20_history.json"  # optional
XRP_KR_SHARE_JSON = DATA / "k_xrp_share_24h_latest.json"  # optional
ETF_JSON          = DATA / "etf_summary.json"  # optional
KRW_SNAPSHOTS_JSON = DATA / "krw_24h_snapshots.json"  # optional
NASDAQ_JSON       = DATA / "nasdaq_series.json"  # optional
KOSPI_JSON        = DATA / "kospi_series.json"   # optional

NEWS_ONELINER_TXT = DATA / "news_one_liner.txt"
NEWS_ONELINER_NOTE_TXT = DATA / "news_one_liner_note.txt"
TOP_NEWS_JSON = DATA / "top_news_latest.json"

# WordPress settings
WP_BASE_URL                 = config.WP_BASE_URL + "/wp-json/wp/v2"
WP_TAG_NEWSLETTER           = "뉴스레터"
WP_TAG_NEWSLETTER_LEAD      = "뉴스레터-리드"
WP_TAG_ID_NEWSLETTER        = config.WP_NEWSLETTER_TAG_ID
WP_TAG_ID_NEWSLETTER_LEAD   = 80405

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
                p_str = f""
            elif price >= 1:
                p_str = f""
            else:
                p_str = f""

            arrow = chr(9650) if chg >= 0 else chr(9660)  # ▲ ▼
            cls = "ticker-up" if chg >= 0 else "ticker-down"
            result[f"TICKER_{sym}_PRICE"] = p_str
            result[f"TICKER_{sym}_CHANGE"] = f"{arrow}{abs(chg):.1f}%"
            result[f"TICKER_{sym}_COLOR"] = cls

        result["TICKER_TIME"] = _kst_now()
        print("INFO: Ticker from CSV (no API call)")
        return result
    except Exception as e:
        print(f"WARN: load ticker from CSV failed: {e}")
        return fallback


def fetch_yahoo_ticker() -> dict[str, str]:
    """BTC·ETH·XRP 현재가 + 24h 변동률 (Yahoo Finance — yfinance)"""
    import yfinance as yf

    SYMBOLS = {"BTC-USD": "BTC", "ETH-USD": "ETH", "XRP-USD": "XRP"}
    fb = {"PRICE": "—", "CHANGE": "—", "COLOR": "ticker-down"}
    fallback = {
        **{f"TICKER_BTC_{k}": v for k, v in fb.items()},
        **{f"TICKER_ETH_{k}": v for k, v in fb.items()},
        **{f"TICKER_XRP_{k}": v for k, v in fb.items()},
        "TICKER_TIME": _kst_now(),
    }

    try:
        tickers = yf.Tickers(" ".join(SYMBOLS.keys()))
        result = {}
        for yf_sym, sym in SYMBOLS.items():
            try:
                info  = tickers.tickers[yf_sym].fast_info
                price = float(info.last_price)
                prev  = float(info.previous_close)
                chg   = (price - prev) / prev * 100 if prev else 0.0

                if price >= 1_000:
                    p_str = f"${price:,.0f}"
                elif price >= 1:
                    p_str = f"${price:,.2f}"
                else:
                    p_str = f"${price:.4f}"

                arrow = "▲" if chg >= 0 else "▼"
                cls   = "ticker-up" if chg >= 0 else "ticker-down"
                result[f"TICKER_{sym}_PRICE"]  = p_str
                result[f"TICKER_{sym}_CHANGE"] = f"{arrow}{abs(chg):.1f}%"
                result[f"TICKER_{sym}_COLOR"]  = cls
            except Exception as e:
                print(f"WARN: Yahoo ticker {yf_sym} failed: {e}")
                result[f"TICKER_{sym}_PRICE"]  = "—"
                result[f"TICKER_{sym}_CHANGE"] = "—"
                result[f"TICKER_{sym}_COLOR"]  = "ticker-down"

        result["TICKER_TIME"] = _kst_now()
        print("INFO: Ticker via Yahoo Finance")
        return result

    except Exception as e:
        print(f"WARN: Yahoo Finance fetch failed: {e}")
        return fallback


# 하위 호환 alias (기존 호출부 변경 불필요)
fetch_coingecko_ticker = fetch_yahoo_ticker


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
            result[f"UPBIT_TOP{i}_SYMBOL"] = g.get("symbol", chr(8212))
            result[f"UPBIT_TOP{i}_CHG"] = f"+{g['change_pct']:.1f}%"
        for i, g in enumerate(bots, 1):
            result[f"UPBIT_BOT{i}_SYMBOL"] = g.get("symbol", chr(8212))
            result[f"UPBIT_BOT{i}_CHG"] = f"{g['change_pct']:.1f}%"
        FB.update(result)
    except Exception as e:
        print(f"WARN: load upbit top/bottom from file failed: {e}")
    return FB


def fetch_upbit_top_bottom(n: int = 3) -> dict[str, str]:
    """업비트 KRW 전체 마켓 24h 등락률 Top/Bottom n"""
    FB = {**{f"UPBIT_TOP{i}_SYMBOL": "—" for i in range(1,n+1)},
          **{f"UPBIT_TOP{i}_CHG":    "—" for i in range(1,n+1)},
          **{f"UPBIT_BOT{i}_SYMBOL": "—" for i in range(1,n+1)},
          **{f"UPBIT_BOT{i}_CHG":    "—" for i in range(1,n+1)}}
    try:
        mkts = [m["market"] for m in
                requests.get("https://api.upbit.com/v1/market/all",
                             params={"isDetails":"false"}, timeout=10).json()
                if m["market"].startswith("KRW-")]
        tickers = []
        for i in range(0, len(mkts), 100):
            tickers += requests.get("https://api.upbit.com/v1/ticker",
                                    params={"markets": ",".join(mkts[i:i+100])},
                                    timeout=10).json()
        tickers.sort(key=lambda x: x.get("signed_change_rate", 0), reverse=True)
        result = {}
        for i, t in enumerate(tickers[:n], 1):
            sym = t["market"].replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            result[f"UPBIT_TOP{i}_SYMBOL"] = sym
            result[f"UPBIT_TOP{i}_CHG"]    = f"+{pct:.1f}%"
        for i, t in enumerate(reversed(tickers[-n:]), 1):
            sym = t["market"].replace("KRW-", "")
            pct = float(t.get("signed_change_rate", 0)) * 100
            result[f"UPBIT_BOT{i}_SYMBOL"] = sym
            result[f"UPBIT_BOT{i}_CHG"]    = f"{pct:.1f}%"
        return result
    except Exception as e:
        print(f"WARN: Upbit top/bottom failed: {e}")
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
        print(f"WARN: exchange vol top3 failed: {e}")
    return FB


def fetch_premium_data(usdkrw: float | None) -> dict[str, str]:
    """김치 프리미엄 vs 코인베이스 프리미엄 계산"""
    FB = {"KIMCHI_PREM_PCT": "—", "CB_PREMIUM_PCT": "—",
          "PREMIUM_COMMENT": "프리미엄 데이터를 가져올 수 없습니다.", "PREMIUM_ASOF": "—"}
    try:
        upbit_btc_krw = float(
            requests.get("https://api.upbit.com/v1/ticker",
                         params={"markets":"KRW-BTC"}, timeout=10).json()[0]["trade_price"])
        # Yahoo Finance로 BTC USD 기준가 조회
        import yfinance as yf
        yf_btc = yf.Ticker("BTC-USD").fast_info
        cg_usd = float(yf_btc.last_price)
        fx = usdkrw if (usdkrw and usdkrw > 100) else 1510.0  # 환율 힌트 없으면 하드코딩 폴백
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

        kst_now = datetime.now(timezone(timedelta(hours=9)))
        asof = f"{kst_now.month}월 {kst_now.day}일 {'오전' if kst_now.hour < 12 else '오후'} {kst_now.hour if kst_now.hour <= 12 else kst_now.hour - 12}시 {kst_now.minute:02d}분 기준"

        return {"KIMCHI_PREM_PCT": _c(kimchi_pct), "CB_PREMIUM_PCT": _c(cb_pct),
                "PREMIUM_COMMENT": comment, "PREMIUM_ASOF": asof}
    except Exception as e:
        print(f"WARN: Premium fetch failed: {e}")
        return FB



# ─────────────────────────────────────────────────────────
# 워드프레스 REST API: 태그 기반 뉴스 수집
# ─────────────────────────────────────────────────────────

def _wp_get_tag_id(tag_name: str) -> int | None:
    """태그 이름으로 워드프레스 태그 ID 조회"""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/tags",
            params={"search": tag_name, "per_page": 5},
            timeout=10,
        )
        res.raise_for_status()
        for t in res.json():
            if t.get("name") == tag_name:
                return int(t["id"])
        print(f"WARN: WP tag '{tag_name}' not found")
    except Exception as e:
        print(f"WARN: WP tag lookup failed ({tag_name}): {e}")
    return None


def _strip_html(text: str) -> str:
    """HTML 태그 제거 + 공백 정리"""
    import re as _re
    return _re.sub(r"<[^>]+>", "", text or "").strip()


def fetch_wp_newsletter_lead() -> dict[str, str]:
    """
    태그 '뉴스레터-리드' (ID: 80405) 최신 포스트 1개에서
    NEWS_HEADLINE, NEWS_ONE_LINER_NOTE 수집.
    없으면 '뉴스레터' (ID: 28978) 최신 1개로 fallback — 오류 없이 계속 진행.
    """
    FB = {
        "NEWS_HEADLINE": "—",
        "NEWS_ONE_LINER_NOTE": "—",
    }

    def _parse_post(post: dict) -> dict[str, str]:
        # excerpt 사용 — 기자 이름 없이 깔끔한 발췌문
        excerpt = _strip_html(post["excerpt"]["rendered"])
        if len(excerpt) > 150:
            excerpt = excerpt[:150].rstrip() + "…"
        return {
            "NEWS_HEADLINE":       _strip_html(post["title"]["rendered"]),
            "NEWS_ONE_LINER_NOTE": excerpt,
        }

    # 1차: 뉴스레터-리드 시도
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER_LEAD, "per_page": 1, "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터-리드 포스트 사용")
            return _parse_post(posts[0])
        print("WARN: 뉴스레터-리드 포스트 없음 → 뉴스레터 최신 1개로 fallback")
    except Exception as e:
        print(f"WARN: 뉴스레터-리드 fetch 실패: {e} → fallback 시도")

    # 2차: 뉴스레터 최신 1개 fallback
    try:
        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": WP_TAG_ID_NEWSLETTER, "per_page": 1, "orderby": "date", "status": "publish"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()
        if posts:
            print("INFO: 뉴스레터 최신 1개로 헤드라인 대체")
            return _parse_post(posts[0])
        print("WARN: 뉴스레터 포스트도 없음 → 기본값 사용")
    except Exception as e:
        print(f"WARN: 뉴스레터 fallback fetch 실패: {e}")

    return FB


def fetch_wp_newsletter_news() -> list[dict[str, str]]:
    """
    태그 '뉴스레터' (ID: 28978) 최신 포스트 3개에서
    title, excerpt, link, category 수집
    excerpt 우선순위: bm_post_summary(AI 요약) → 기본 excerpt
    """
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    try:
        tag_id = WP_TAG_ID_NEWSLETTER

        res = requests.get(
            f"{WP_BASE_URL}/posts",
            params={"tags": tag_id, "per_page": 3, "orderby": "date", "status": "publish",
                    "_embed": 1, "_fields": "id,title,excerpt,link,_embedded,meta"},
            timeout=10,
        )
        res.raise_for_status()
        posts = res.json()

        if len(posts) < 3:
            raise ValueError(f"'{WP_TAG_NEWSLETTER}' 태그 발행 포스트가 {len(posts)}개뿐입니다. 3개 필요.")

        # bm_post_summary 메타 필드가 REST API에 노출 안 될 경우 개별 요청으로 fallback
        def _get_summary(post: dict) -> str:
            # 1순위: meta 필드에 bm_post_summary가 있는 경우
            meta = post.get("meta", {}) or {}
            summary = meta.get("bm_post_summary", "")
            if summary and summary.strip():
                print(f"INFO: bm_post_summary 사용 (post {post.get('id')})")
                s = summary.strip()
                return s[:150].rstrip() + "…" if len(s) > 150 else s

            # 2순위: 개별 포스트 API로 메타 재요청
            try:
                r2 = requests.get(
                    f"{WP_BASE_URL}/posts/{post['id']}",
                    params={"_fields": "meta"},
                    timeout=8,
                )
                summary2 = (r2.json().get("meta", {}) or {}).get("bm_post_summary", "")
                if summary2 and summary2.strip():
                    print(f"INFO: bm_post_summary 개별요청 성공 (post {post.get('id')})")
                    s2 = summary2.strip()
                    return s2[:150].rstrip() + "…" if len(s2) > 150 else s2
            except Exception as e:
                print(f"WARN: bm_post_summary 개별요청 실패 (post {post.get('id')}): {e}")

            # 3순위: 기존 excerpt fallback
            excerpt = _strip_html(post["excerpt"]["rendered"])
            if len(excerpt) > 150:
                excerpt = excerpt[:150].rstrip() + "…"
            print(f"INFO: excerpt fallback 사용 (post {post.get('id')})")
            return excerpt

        result = []
        for post in posts[:3]:
            # 카테고리명 추출 (_embed 사용)
            try:
                cats = post.get("_embedded", {}).get("wp:term", [[]])[0]
                cat_name = cats[0]["name"] if cats else ""
            except Exception:
                cat_name = ""

            result.append({
                "title":    _strip_html(post["title"]["rendered"]),
                "excerpt":  _get_summary(post),
                "link":     post.get("link", "#"),
                "category": cat_name,
            })
        return result

    except ValueError as e:
        print(f"ERROR: {e}")
        raise
    except Exception as e:
        print(f"WARN: fetch_wp_newsletter_news failed: {e}")
        return [empty, empty, empty]

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
        print(f"WARN: ETF json not found: {ETF_JSON}")
        return FB
    try:
        raw = json.loads(ETF_JSON.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"WARN: ETF json parse error: {e}")
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

def load_text_first_line(p: Path) -> str:
    if not p.exists():
        return "—"
    s = p.read_text(encoding="utf-8").strip()
    if not s:
        return "—"
    return (s.splitlines()[0].strip() or "—")

def load_top_news_3(p: Path):
    """Returns list of 3 dicts: {title, excerpt, link, category}"""
    empty = {"title": "—", "excerpt": "", "link": "#", "category": ""}
    if not p.exists():
        return [empty, empty, empty]
    try:
        obj = json.loads(p.read_text(encoding="utf-8"))
        items = obj.get("items", []) if isinstance(obj, dict) else (obj or [])
        result = []
        for x in items[:3]:
            if isinstance(x, dict):
                result.append({
                    "title":    x.get("title", "—") or "—",
                    "excerpt":  x.get("excerpt", "") or "",
                    "link":     x.get("link", "#") or "#",
                    "category": x.get("category", "") or "",
                })
            elif isinstance(x, str) and x.strip():
                result.append({**empty, "title": x.strip()})
        while len(result) < 3:
            result.append(empty)
        return result
    except Exception:
        return [empty, empty, empty]

# ------------------ formatting helpers ------------------

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

def tone_bg(pct_value: float) -> str:
    v = float(pct_value)
    if v > 0:
        return "#f0fdf4"
    if v < 0:
        return "#fef2f2"
    return "#fbfdff"

# ------------------ daily csv helpers ------------------

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

def compute_top10_concentration(df: pd.DataFrame) -> str:
    """
    If CSV has volume columns, compute Top10 volume concentration.
    Fallback: return "—".
    """
    vol_col = None
    for c in ("volume_24h", "quote_volume_24h", "turnover_24h", "krw_volume_24h"):
        if c in df.columns:
            vol_col = c
            break
    if not vol_col:
        return "—"
    s = pd.to_numeric(df[vol_col], errors="coerce").dropna()
    if s.empty:
        return "—"
    top10 = s.sort_values(ascending=False).head(10).sum()
    total = s.sum()
    if total <= 0:
        return "—"
    return fmt_share_pct(top10 / total)

# ------------------ index series + krw snapshots helpers ------------------

def load_krw_snapshots_top10() -> str:
    """out/history/krw_24h_snapshots.json 에서 top10 집중도"""
    try:
        import json as _json
        raw = _json.loads(KRW_SNAPSHOTS_JSON.read_text(encoding="utf-8")) if KRW_SNAPSHOTS_JSON.exists() else None
        if raw is None:
            return "—"
        item = raw[-1] if isinstance(raw, list) else raw
        pct = item.get("top10", {}).get("top10_share_pct")
        if pct is not None:
            return f"{float(pct):.1f}%"
    except Exception:
        pass
    return "—"


def load_index_series_1d(path: Path) -> str:
    """[{date, price}] 배열 마지막 두 항목으로 1D 등락 계산"""
    try:
        import json as _json
        if not path.exists():
            return "—"
        raw = _json.loads(path.read_text(encoding="utf-8"))
        if not raw or len(raw) < 2:
            return "—"
        prev = float(raw[-2]["price"])
        curr = float(raw[-1]["price"])
        if prev <= 0:
            return "—"
        chg = (curr - prev) / prev * 100
        arrow = "▲" if chg >= 0 else "▼"
        color = "#16a34a" if chg >= 0 else "#dc2626"
        return f'<span style="color:{color};font-weight:900;">{arrow}{abs(chg):.2f}%</span>'
    except Exception:
        return "—"

def load_index_series_price(path: Path) -> str:
    """[{date, price}] 배열 마지막 항목의 지수값 반환"""
    try:
        import json as _json
        if not path.exists():
            return "—"
        raw = _json.loads(path.read_text(encoding="utf-8"))
        if not raw:
            return "—"
        curr = float(raw[-1]["price"])
        return f"{curr:,.2f}"
    except Exception:
        return "—"


# ------------------ sentiment + xrp share helpers ------------------

def extract_sentiment(obj: Any) -> tuple[str, str]:
    """
    Extract sentiment label/score from a flexible bm20_history.json shape.
    Returns (label, score_str)
    """
    label = None
    score = None

    def pick(d: dict, keys: tuple[str, ...]) -> Any | None:
        for k in keys:
            if k in d and d.get(k) is not None:
                return d.get(k)
        return None

    if isinstance(obj, dict):
        label = pick(obj, ("sentiment_label", "sentimentLabel", "label", "market_sentiment_label", "sentiment"))
        score = pick(obj, ("sentiment_score", "sentimentScore", "score", "market_sentiment_score", "sentiment_index"))

        latest = obj.get("latest") if isinstance(obj.get("latest"), dict) else None
        if latest:
            if label is None:
                label = pick(latest, ("sentiment_label", "sentimentLabel", "label", "sentiment"))
            if score is None:
                score = pick(latest, ("sentiment_score", "sentimentScore", "score", "sentiment_index"))

        series = obj.get("series")
        if (label is None or score is None) and isinstance(series, list) and series and isinstance(series[-1], dict):
            last = series[-1]
            if label is None:
                label = pick(last, ("sentiment_label", "sentimentLabel", "label", "sentiment"))
            if score is None:
                score = pick(last, ("sentiment_score", "sentimentScore", "score", "sentiment_index"))

    label_txt = str(label).strip() if label is not None else "—"
    score_txt = "—"
    if score is not None:
        try:
            score_txt = f"{float(score):.0f}"
        except Exception:
            score_txt = str(score).strip() or "—"
    return label_txt, score_txt

def extract_xrp_kr_share(obj: Any) -> str:
    if not isinstance(obj, dict):
        return "—"

    def pick(d: dict, keys: tuple[str, ...]) -> Any | None:
        for k in keys:
            if k in d and d.get(k) is not None:
                return d.get(k)
        return None

    v = pick(obj, ("xrp_kr_share", "xrp_kr_share_pct", "share_pct", "share", "value"))
    if v is None and isinstance(obj.get("latest"), dict):
        v = pick(obj["latest"], ("xrp_kr_share", "xrp_kr_share_pct", "share_pct", "share", "value"))

    if v is None:
        return "—"

    try:
        return fmt_share_pct(float(v))
    except Exception:
        return str(v).strip() or "—"

# ------------------ synthetic one-line interpreters ------------------

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
        tds += (
            f'<td width="{pct}%" style="background-color:{color};height:28px;'
            f'font-family:Courier New,monospace;font-size:9px;font-weight:bold;'
            f'color:#fff;text-align:center;vertical-align:middle;">{pct}%</td>'
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
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#c2410c;"
    # 모멘텀
    elif any(k in t for k in MOMENTUM_KEYWORDS):
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#15803d;"
    # 기본 온체인(파랑)
    else:
        style = "font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;font-weight:bold;color:#1d4ed8;"
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
            print(f"INFO: AAS data fetched for {date_str}")
            break
        except Exception as e:
            errors.append((date_str, e))

    if data is None and errors:
        for date_str, e in errors:
            print(f"WARN: AAS fetch failed for {date_str}: {e}")

    if data is None:
        print("WARN: AAS data unavailable for all candidate dates. Using defaults.")
        ph["{{AAS_BODY}}"] = '''<tr><td style="padding:32px 24px;text-align:center;">
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:28px;margin:0 0 12px 0;">🔧</p>
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:14px;font-weight:900;color:#0d1117;margin:0 0 6px 0;">오늘의 코생지 데이터를 준비 중입니다</p>
  <p style="font-family:'맑은 고딕','Apple SD Gothic Neo',sans-serif;font-size:12px;color:#64748b;line-height:1.7;margin:0;">분석 데이터는 매일 업데이트됩니다.<br/>내일 아침 뉴스레터에서 확인해주세요!</p>
</td></tr>'''
        return ph

    for i, item in enumerate(data[:3], 1):
        score = float(item.get("AAS", 0))
        score_pct = min(100, int((score / 3.0) * 100))

        note_text = item.get("Comment", "—")
        ph[f"{{{{AAS_COIN_{i}}}}}"] = item.get("Symbol", "—")
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
    nasdaq_1d = load_index_series_1d(NASDAQ_JSON)
    kospi_1d  = load_index_series_1d(KOSPI_JSON)

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
        "{{NASDAQ_1D}}": nasdaq_1d,
        "{{NASDAQ_PRICE}}": load_index_series_price(NASDAQ_JSON),
        "{{KOSPI_1D}}": kospi_1d,
        "{{LETTER_DATE}}": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%d"),

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
        "{{UNSUB_URL}}", "{{SUBSCRIBE_URL}}", "{{INTRO_TEXT}}",
        "{{HEADLINE_NEWS_TITLE}}", "{{HEADLINE_NEWS_EXCERPT}}",
        "{{NEWS1_CATEGORY}}", "{{NEWS1_TITLE}}", "{{NEWS1_EXCERPT}}", "{{NEWS1_LINK}}",
        "{{NEWS2_CATEGORY}}", "{{NEWS2_TITLE}}", "{{NEWS2_EXCERPT}}", "{{NEWS2_LINK}}",
        "{{NEWS3_CATEGORY}}", "{{NEWS3_TITLE}}", "{{NEWS3_EXCERPT}}", "{{NEWS3_LINK}}",
    }
    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)) - EXTERNAL_PLACEHOLDERS)
    if left: print("WARN: Unfilled placeholders:", left)
    
    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")

if __name__ == "__main__":
    render()
