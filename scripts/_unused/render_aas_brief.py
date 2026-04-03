#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
render_aas_brief.py
───────────────────
AAS-Bot private repo에서 JSON + CSV + PNG를 읽어
aas_brief_template.html 의 플레이스홀더를 채워 aas_brief.html 을 생성합니다.

render_letter.py 와 동일한 방식 (순수 string replace, no JS)

Inputs (GitHub raw URL — private repo, AAS_BOT_TOKEN 환경변수 필요)
  reports/daily/{date}/newsletter_aas_top3_{date}.json
  reports/daily/{date}/daily_report_{date}.csv
  reports/daily/{date}/daily_score_{date}.png
  reports/daily/{date}/newsletter_contribution_top3_{date}.png

Output
  aas_brief.html
"""

from __future__ import annotations

import os
import re
import json
import base64
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone, timedelta
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "templates" / "aas_brief_template.html"
OUT      = ROOT / "output" / "aas_brief.html"

REPO     = "Blockmedia-DataTeam/AAS-Bot"
BRANCH   = "main"
BASE_RAW = f"https://raw.githubusercontent.com/{REPO}/{BRANCH}"
BASE_API = f"https://api.github.com/repos/{REPO}/contents"


def _token_headers() -> dict:
    token = os.environ.get("AAS_BOT_TOKEN", "")
    return {"Authorization": f"token {token}"} if token else {}


def _color_class(v: float) -> str:
    return "up" if v >= 0 else "dn"


def _fmt_chg(v: float) -> str:
    return f"+{v:.2f}%" if v >= 0 else f"{v:.2f}%"


def _fmt_price(p: float) -> str:
    if p >= 1000:  return f"{p:,.2f}"
    if p >= 1:     return f"{p:.4f}"
    if p >= 0.01:  return f"{p:.6f}"
    return f"{p:.8f}"


def _action_badge(rsi: float, comment: str) -> str:
    if "고래" in comment or "accum" in comment.lower():
        return '<span class="badge badge-accum">ACCUM 🐋</span>'
    if "관심" in comment or "specu" in comment.lower():
        return '<span class="badge badge-specu">SPECU 🗣️</span>'
    if "과열" in comment or "caution" in comment.lower():
        return '<span class="badge badge-caution">CAUTION</span>'
    if rsi <= 30:
        return '<span class="badge badge-over">OVERSOLD</span>'
    return '<span class="badge badge-specu">WATCH</span>'


def _comment_class(comment: str) -> str:
    if "고래" in comment: return "whale"
    if "관심" in comment: return "specu"
    if "과열" in comment: return "caution"
    return ""


def _date_candidates() -> list[str]:
    kst = timezone(timedelta(hours=9))
    today = datetime.now(kst).date()
    return [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(3)]


def _find_latest_date() -> str | None:
    try:
        r = requests.get(f"{BASE_API}/reports/daily", headers=_token_headers(), timeout=10)
        r.raise_for_status()
        folders = sorted([i["name"] for i in r.json() if i["type"] == "dir"], reverse=True)
        return folders[0] if folders else None
    except Exception as e:
        print(f"WARN: folder list failed: {e}")
        return None


def _fetch_raw(path: str) -> requests.Response | None:
    try:
        r = requests.get(f"{BASE_RAW}/{path}", headers=_token_headers(), timeout=15)
        r.raise_for_status()
        print(f"INFO: fetched {path}")
        return r
    except Exception as e:
        print(f"WARN: fetch failed {path}: {e}")
        return None


def _png_to_b64(path: str) -> str:
    r = _fetch_raw(path)
    if r is None:
        return ""
    return f"data:image/png;base64,{base64.b64encode(r.content).decode()}"


def _table_row(rank: int, sym: str, aas: float, price: float,
               chg: float, rsi: float, comment: str, hq: bool = False) -> str:
    return f"""
    <tr class="{'hq' if hq else ''}">
      <td><span class="rank-c {'top' if rank <= 3 else ''}">{rank}</span></td>
      <td class="sym">{sym}</td>
      <td class="aas">{aas:.2f}</td>
      <td class="price">{_fmt_price(price)}</td>
      <td class="chg {_color_class(chg)}">{_fmt_chg(chg)}</td>
      <td class="rsi">{rsi:.0f}</td>
      <td>{_action_badge(rsi, comment)}</td>
      <td class="cmt {_comment_class(comment)}">{comment}</td>
    </tr>"""


def render() -> None:
    if not TEMPLATE.exists():
        raise FileNotFoundError(f"Missing template: {TEMPLATE}")

    # 1. 최신 날짜 찾기
    date_str = _find_latest_date()
    if not date_str:
        for d in _date_candidates():
            if _fetch_raw(f"reports/daily/{d}/newsletter_aas_top3_{d}.json"):
                date_str = d
                break
    if not date_str:
        raise RuntimeError("코생지 데이터를 찾을 수 없습니다")

    print(f"INFO: using date {date_str}")
    base = f"reports/daily/{date_str}"

    # 2. JSON (Top 3)
    r_json = _fetch_raw(f"{base}/newsletter_aas_top3_{date_str}.json")
    top3   = r_json.json() if r_json else []

    # 3. CSV (전체)
    r_csv = _fetch_raw(f"{base}/daily_report_{date_str}.csv")
    if r_csv:
        df       = pd.read_csv(StringIO(r_csv.text))
        top10_df = df[df["Rank"] <= 10].copy()
    else:
        df = top10_df = pd.DataFrame()

    # 4. 차트 PNG → base64
    score_img   = _png_to_b64(f"{base}/daily_score_{date_str}.png")
    contrib_img = _png_to_b64(f"{base}/newsletter_contribution_top3_{date_str}.png")

    # 5. Major coins
    major_rows_html = ""
    if not df.empty:
        for sym in ["ETH", "SOL", "XRP"]:
            row = df[df["symbol"] == sym]
            if row.empty: continue
            row = row.iloc[0]
            rsi = float(row["RSI"])
            major_rows_html += _table_row(
                rank=int(row["Rank"]), sym=sym,
                aas=float(row["AAS_score"]), price=float(row["price"]),
                chg=float(row["24h"]), rsi=rsi,
                comment="과열 직전 (주의)" if rsi >= 70 else "관심 집중",
                hq=False
            )

    # 6. Top 10
    top3_map = {i.get("Symbol"): i.get("Comment", "") for i in top3}
    top10_rows_html = ""
    if not top10_df.empty:
        for _, row in top10_df.iterrows():
            sym = row["symbol"]
            aas = float(row["AAS_score"])
            chg = float(row["24h"])
            rsi = float(row["RSI"])
            comment = top3_map.get(sym,
                "고래 매집" if aas >= 1.5 else
                "관심 집중" if aas >= 1.0 else
                "모니터링"
            )
            top10_rows_html += _table_row(
                rank=int(row["Rank"]), sym=sym,
                aas=aas, price=float(row["price"]),
                chg=chg, rsi=rsi,
                comment=comment, hq=(aas >= 1.5)
            )

    # 7. 대시보드 통계
    if not top10_df.empty:
        chg_vals  = top10_df["24h"].astype(float)
        avg_top10 = chg_vals.mean()
        median    = chg_vals.median()
        excl1     = chg_vals.iloc[1:].mean() if len(chg_vals) > 1 else avg_top10
        best_row  = top10_df.loc[chg_vals.idxmax()]
        worst_row = top10_df.loc[chg_vals.idxmin()]
        best_str  = f"{best_row['symbol']} ({_fmt_chg(float(best_row['24h']))})"
        worst_str = f"{worst_row['symbol']} ({_fmt_chg(float(worst_row['24h']))})"
    else:
        avg_top10 = median = excl1 = 0.0
        best_str = worst_str = "—"

    btc_return = 0.0
    if not df.empty and "BTC" in df["symbol"].values:
        btc_row = df[df["symbol"] == "BTC"]
        if not btc_row.empty:
            btc_return = float(btc_row.iloc[0]["24h"])

    alpha_btc = avg_top10 - btc_return

    # 8. Insight
    hq_coins = [r["symbol"] for _, r in top10_df.iterrows()
                if float(r["AAS_score"]) >= 1.5] if not top10_df.empty else []
    hq_str   = ", ".join(hq_coins[:4]) if hq_coins else "—"
    oversold = sum(1 for _, r in top10_df.iterrows()
                   if float(r["RSI"]) <= 40) if not top10_df.empty else 0

    insight1 = f"<strong>시장 현황:</strong> 코생지 Top 10 평균 수익률 {_fmt_chg(avg_top10)}, 비트코인 대비 알파 {alpha_btc:+.2f}%p"
    insight2 = f"<strong>High Quality 종목:</strong> {hq_str} 등 코생지 1.5 이상 종목은 고래 매집 지속 중"
    insight3 = f"<strong>RSI 모니터링:</strong> Top 10 중 {oversold}종목이 RSI 40 이하 — 단기 변동성 주의"

    # 9. KST 날짜
    kst = timezone(timedelta(hours=9))
    day_kr = ["월","화","수","목","금","토","일"][datetime.now(kst).weekday()]
    report_date = f"{date_str} ({day_kr})"

    # 10. 플레이스홀더 치환
    ph = {
        "{{REPORT_DATE}}":       report_date,
        "{{AVG_TOP10}}":         _fmt_chg(avg_top10),
        "{{AVG_TOP10_COLOR}}":   _color_class(avg_top10),
        "{{MEDIAN_RETURN}}":     _fmt_chg(median),
        "{{MEDIAN_COLOR}}":      _color_class(median),
        "{{EXCL1_RETURN}}":      _fmt_chg(excl1),
        "{{EXCL1_COLOR}}":       _color_class(excl1),
        "{{BEST_PERFORMER}}":    best_str,
        "{{WORST_PERFORMER}}":   worst_str,
        "{{BTC_RETURN}}":        _fmt_chg(btc_return),
        "{{BTC_COLOR}}":         _color_class(btc_return),
        "{{ALPHA_BTC}}":         f"{alpha_btc:+.2f}%p",
        "{{ALPHA_COLOR}}":       _color_class(alpha_btc),
        "{{INSIGHT_1}}":         insight1,
        "{{INSIGHT_2}}":         insight2,
        "{{INSIGHT_3}}":         insight3,
        "{{MAJOR_ROWS}}":        major_rows_html,
        "{{TOP10_ROWS}}":        top10_rows_html,
        "{{SCORE_CHART_URL}}":   score_img,
        "{{CONTRIB_CHART_URL}}": contrib_img,
    }

    html = TEMPLATE.read_text(encoding="utf-8")
    for k in sorted(ph.keys(), key=len, reverse=True):
        html = html.replace(k, str(ph[k]))

    left = sorted(set(re.findall(r"\{\{[A-Z0-9_]+\}\}", html)))
    if left:
        print("WARN: unfilled placeholders:", left)

    OUT.write_text(html, encoding="utf-8")
    print(f"OK: wrote {OUT}")


if __name__ == "__main__":
    render()
