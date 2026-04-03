#!/usr/bin/env python3
"""
deliver_letter.py
==================
Upload letter.html to WordPress server via REST API.
Retries up to 3 times with 5s delay between attempts.
"""

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import config
from logger import get_logger

log = get_logger("deliver")

LETTER_HTML = config.OUTPUT_DIR / "letter.html"
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def deliver():
    if not config.HTM_API_URL:
        log.warning("HTM_API_URL not set, skipping delivery")
        return False

    if not config.HTM_API_KEY:
        log.warning("HTM_API_KEY not set, skipping delivery")
        return False

    if not LETTER_HTML.exists():
        log.error("letter.html not found: %s", LETTER_HTML)
        return False

    html = LETTER_HTML.read_bytes()
    log.info("Uploading letter.html (%.1f KB) to %s", len(html) / 1024, config.HTM_API_URL)

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
            log.info("Delivered: %d %s", resp.status_code, resp.text[:200])
            return True
        except Exception as e:
            log.error("Attempt %d/%d failed: %s", attempt, MAX_RETRIES, e)
            if attempt < MAX_RETRIES:
                log.info("Retrying in %ds...", RETRY_DELAY)
                time.sleep(RETRY_DELAY)

    log.error("All %d attempts failed", MAX_RETRIES)
    return False


if __name__ == "__main__":
    deliver()
    # Always exit 0 - delivery is optional, should not break pipeline
    sys.exit(0)