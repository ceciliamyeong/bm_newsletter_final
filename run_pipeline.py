#!/usr/bin/env python3
"""
Newsletter Pipeline Runner
===========================
Runs all data collection and rendering scripts in the correct order.
Each step is independent - if one fails, the pipeline continues
and render_letter.py uses fallbacks for missing data.

Usage:
    python run_pipeline.py          # run full pipeline
    python run_pipeline.py --step 5 # run from step 5 onwards
"""

import sys
import time
import subprocess
from pathlib import Path

from logger import get_logger

log = get_logger("pipeline")

ROOT = Path(__file__).resolve().parent
PYTHON = ROOT / "venv" / "Scripts" / "python.exe"
if not PYTHON.exists():
    PYTHON = ROOT / "venv" / "bin" / "python"  # Linux/Mac
SCRIPTS = ROOT / "scripts"

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
    (6, "render_letter.py",     "Generate newsletter.html"),

    # Step 7: Deliver (depends on step 6)
    # (7, "deliver_letter.py",    "Upload newsletter.html to WordPress"),
]


def run_step(step_num: int, script: str, description: str) -> bool:
    """Run a single pipeline step. Returns True on success."""
    script_path = SCRIPTS / script
    if not script_path.exists():
        log.warning("Step %d: %s not found, skipping", step_num, script)
        return False

    log.info("Step %d: %s [%s]", step_num, description, script)

    start = time.time()
    try:
        result = subprocess.run(
            [str(PYTHON), str(script_path)],
            cwd=str(ROOT),
            capture_output=False,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        elapsed = time.time() - start
        log.error("Step %d: TIMEOUT after %.1fs", step_num, elapsed)
        return False

    elapsed = time.time() - start

    if result.returncode == 0:
        log.info("Step %d: OK (%.1fs)", step_num, elapsed)
        return True
    else:
        log.error("Step %d: FAIL exit=%d (%.1fs)", step_num, result.returncode, elapsed)
        return False


def main():
    start_step = 1
    if len(sys.argv) > 2 and sys.argv[1] == "--step":
        start_step = int(sys.argv[2])

    log.info("Pipeline started (from step %d)", start_step)

    total_start = time.time()
    results = []

    for step_num, script, description in PIPELINE:
        if step_num < start_step:
            continue
        success = run_step(step_num, script, description)
        results.append((step_num, script, success))

    total_elapsed = time.time() - total_start

    # Summary
    failed = [s for s in results if not s[2]]
    for step_num, script, success in results:
        status = "OK" if success else "FAIL"
        log.info("  Step %d: [%s] %s", step_num, status, script)

    if failed:
        log.warning("Pipeline finished: %d step(s) failed (%.1fs)", len(failed), total_elapsed)
    else:
        log.info("Pipeline finished: all steps passed (%.1fs)", total_elapsed)


if __name__ == "__main__":
    main()