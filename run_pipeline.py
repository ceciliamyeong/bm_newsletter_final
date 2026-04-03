#!/usr/bin/env python3
"""
Newsletter Pipeline Runner
===========================
Runs all data collection and rendering scripts in the correct order.
Each step is independent — if one fails, the pipeline continues
and render_letter.py uses fallbacks for missing data.

Usage:
    python run_pipeline.py          # run full pipeline
    python run_pipeline.py --step 5 # run from step 5 onwards
"""

import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone, timedelta

ROOT = Path(__file__).resolve().parent
PYTHON = ROOT / "venv" / "Scripts" / "python.exe"
if not PYTHON.exists():
    PYTHON = ROOT / "venv" / "bin" / "python"  # Linux/Mac
SCRIPTS = ROOT / "scripts"

KST = timezone(timedelta(hours=9))

PIPELINE = [
    # Step 1-2: Data collection (can run independently)
    (1, "krw_rolling24h_8h.py", "KRW exchange volumes (Upbit/Bithumb/Coinone)"),
    (2, "fetch_etf.py",         "ETF data (BTC/ETH/SOL)"),

    # Step 3-4: BM20 index (depends on step 1 for krw_24h_latest.json)
    (3, "bm20_daily.py",        "BM20 index (prices, level, kimchi premium)"),
    (4, "update_bm20_full.py",  "Market data (sentiment, K-share, XRP share)"),

    # Step 5: BTC series (depends on step 3 for bm20_latest.json + CSV)
    (5, "update_btc_series.py", "BTC price series"),

    # Step 6: Render (depends on all above)
    (6, "render_letter.py",     "Generate letter.html"),

    # Step 7: Deliver (depends on step 6)
    (7, "deliver_letter.py",    "Upload letter.html to WordPress"),
]


def run_step(step_num: int, script: str, description: str) -> bool:
    """Run a single pipeline step. Returns True on success."""
    script_path = SCRIPTS / script
    if not script_path.exists():
        print(f"  [SKIP] {script} not found")
        return False

    print(f"\n{'='*60}")
    print(f"  Step {step_num}: {description}")
    print(f"  Script: {script}")
    print(f"{'='*60}")

    start = time.time()
    result = subprocess.run(
        [str(PYTHON), str(script_path)],
        cwd=str(ROOT),
        capture_output=False,
    )
    elapsed = time.time() - start

    if result.returncode == 0:
        print(f"  [OK] {script} ({elapsed:.1f}s)")
        return True
    else:
        print(f"  [FAIL] {script} (exit code {result.returncode}, {elapsed:.1f}s)")
        return False


def main():
    start_step = 1
    if len(sys.argv) > 2 and sys.argv[1] == "--step":
        start_step = int(sys.argv[2])

    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    print(f"\n  Newsletter Pipeline - {now}")
    print(f"  Starting from step {start_step}\n")

    total_start = time.time()
    results = []

    for step_num, script, description in PIPELINE:
        if step_num < start_step:
            continue
        success = run_step(step_num, script, description)
        results.append((step_num, script, success))

    # Summary
    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"  Pipeline Summary ({total_elapsed:.1f}s total)")
    print(f"{'='*60}")
    for step_num, script, success in results:
        status = "OK" if success else "FAIL"
        print(f"  Step {step_num}: [{status}] {script}")

    failed = [s for s in results if not s[2]]
    if failed:
        print(f"\n  {len(failed)} step(s) failed.")
        sys.exit(1)
    else:
        output = ROOT / "output" / "letter.html"
        print(f"\n  All steps passed.")
        print(f"  Output: {output}")


if __name__ == "__main__":
    main()