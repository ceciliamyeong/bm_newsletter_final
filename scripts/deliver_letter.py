#!/usr/bin/env python3
"""
deliver_letter.py
==================
Upload letter.html to WordPress server via REST API.

Equivalent curl command:
  curl -X POST https://your-site.com/wp-json/htm/v1/upload \
    -H "X-HTM-API-Key: your-secret-key-here" \
    -H "Content-Type: text/html" \
    --data-binary @output/letter.html
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import time
import requests
import config

LETTER_HTML = config.OUTPUT_DIR / "letter.html"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def deliver():
    if not config.HTM_API_URL:
        print("[SKIP] HTM_API_URL not set, skipping delivery")
        return False

    if not config.HTM_API_KEY:
        print("[SKIP] HTM_API_KEY not set, skipping delivery")
        return False

    if not LETTER_HTML.exists():
        print(f"[FAIL] letter.html not found: {LETTER_HTML}")
        return False

    html = LETTER_HTML.read_bytes()
    print(f"[INFO] Uploading letter.html ({len(html)/1024:.1f} KB) to {config.HTM_API_URL}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(
                config.HTM_API_URL,
                headers={
                    "X-HTM-API-Key": config.HTM_API_KEY,
                    "Content-Type": "text/html",
                },
                data=html,
                timeout=30,
            )
            resp.raise_for_status()
            print(f"[OK] Delivered: {resp.status_code} {resp.text[:200]}")
            return True
        except Exception as e:
            print(f"[FAIL] Attempt {attempt}/{MAX_RETRIES}: {e}")
            if attempt < MAX_RETRIES:
                print(f"[INFO] Retrying in {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)

    print(f"[FAIL] All {MAX_RETRIES} attempts failed")
    return False


if __name__ == "__main__":
    deliver()
    # Always exit 0 — delivery is optional, should not break pipeline
    sys.exit(0)