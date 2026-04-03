#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Render BM20 treemap as a PNG (email-safe asset).

- Size (area): weight_ratio
- Color: price_change_pct (1D)
- Label inside each tile: SYMBOL + 1D change (e.g., BTC\n-0.23%)

Input:
- bm20_daily_data_latest.csv

Output:
- assets/topcoins_treemap_latest.png
"""

from pathlib import Path
import pandas as pd
import plotly.express as px

ROOT = Path(__file__).resolve().parent.parent
CSV = ROOT / "data" / "bm20_daily_data_latest.csv"
OUT = ROOT / "output" / "treemap.png"

def main():
    if not CSV.exists():
        raise FileNotFoundError(f"Missing {CSV}")

    df = pd.read_csv(CSV)

    required = {"symbol", "weight_ratio", "price_change_pct"}
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["weight_ratio"] = pd.to_numeric(df["weight_ratio"], errors="coerce").fillna(0.0)
    df["price_change_pct"] = pd.to_numeric(df["price_change_pct"], errors="coerce").fillna(0.0)

    # Top 20 by weight
    df = df.sort_values("weight_ratio", ascending=False).head(20).copy()

    # Build label text to avoid Plotly showing rgb(...) strings
    df["label_text"] = df.apply(lambda r: f"{r['symbol']}<br>{r['price_change_pct']:+.2f}%", axis=1)

    fig = px.treemap(
        df,
        path=["symbol"],
        values="weight_ratio",
        color="price_change_pct",
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
    )

    # Force our own labels inside tiles
    fig.update_traces(
        text=df["label_text"],
        textinfo="text",
        textfont_size=18,
        hovertemplate="<b>%{label}</b><br>1D: %{color:+.2f}%<extra></extra>",
    )

    fig.update_layout(
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="white",
        coloraxis_colorbar_title="1D %",
    )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(OUT), width=900, height=520, scale=2)  # kaleido required
    print("Treemap PNG written:", OUT)

if __name__ == "__main__":
    main()
