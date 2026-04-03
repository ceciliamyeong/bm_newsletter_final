"""
Centralized logging configuration.
All scripts use get_logger(name) to get a logger with consistent format.

Output:
  - stdout (same as before)
  - logs/pipeline.log (with rotation: 5MB, keep 3 files)

Format:
  2026-04-03 07:00:15 [INFO] [bm20_daily] Prices fetched: 20 coins

Timezone: KST (Asia/Seoul, UTC+9)
"""

import logging
import sys
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)
LOG_FILE = LOG_DIR / "pipeline.log"

MAX_BYTES = 5 * 1024 * 1024  # 5 MB
BACKUP_COUNT = 3

KST = timezone(timedelta(hours=9))

_initialized = False


class KSTFormatter(logging.Formatter):
    """Log formatter that uses KST timezone."""

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=KST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S")


def _setup():
    """Initialize root logger with stdout + file handlers (called once)."""
    global _initialized
    if _initialized:
        return
    _initialized = True

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    # Suppress noisy third-party loggers
    for name in ("urllib3", "yfinance", "peewee", "curl_cffi"):
        logging.getLogger(name).setLevel(logging.WARNING)

    fmt = KSTFormatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

    # stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(fmt)

    # file handler with rotation
    file_handler = RotatingFileHandler(
        LOG_FILE, maxBytes=MAX_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(fmt)

    root.addHandler(stdout_handler)
    root.addHandler(file_handler)


def get_logger(name: str) -> logging.Logger:
    """Get a named logger. Call this from each script."""
    _setup()
    return logging.getLogger(name)