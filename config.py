"""
Centralized project configuration.
Loads environment variables from .env and provides paths.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

# ── Paths ─────────────────────────────────────────────
DATA_DIR = ROOT / "data"
OUTPUT_DIR = ROOT / "output"
TEMPLATES_DIR = ROOT / "templates"

# ── API keys ──────────────────────────────────────────
CMC_API_KEY = os.environ.get("CMC_API_KEY", "")
AAS_BOT_TOKEN = os.environ.get("AAS_BOT_TOKEN", "")
SOSOVALUE_API_KEY = os.environ.get("SOSOVALUE_API_KEY", "")

# ── Stibee (email delivery) ──────────────────────────
STIBEE_API_KEY = os.environ.get("STIBEE_API_KEY", "")
STIBEE_LIST_ID = os.environ.get("STIBEE_LIST_ID", "")
STIBEE_FROM_EMAIL = os.environ.get("STIBEE_FROM_EMAIL", "newsletter@blockmedia.co.kr")
STIBEE_FROM_NAME = os.environ.get("STIBEE_FROM_NAME", "블록미디어")

# ── WordPress ────────────────────────────────────────
WP_BASE_URL = os.environ.get("WP_BASE_URL", "https://blockmedia.co.kr")
WP_NEWSLETTER_TAG_ID = int(os.environ.get("NEWSLETTER_TAG_ID", "28978"))
