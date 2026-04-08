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

# ── Newsletter delivery API ──────────────────────────
HTM_API_URL = os.environ.get("HTM_API_URL", "")
HTM_API_KEY = os.environ.get("HTM_API_KEY", "")

# ── CGI trigger ──────────────────────────────────────
CGI_SECRET_KEY = os.environ.get("CGI_SECRET_KEY", "")
