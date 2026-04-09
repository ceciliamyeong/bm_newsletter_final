#!/blockmedia/newsletter/venv/bin/python3
"""
CGI endpoint: triggers the newsletter pipeline and returns generated HTML.

Usage:
    GET /newsletter/trigger
    Header: X-Trigger-Key: YOUR_SECRET_KEY

Returns:
    200 — newsletter.html content (text/html)
    403 — invalid or missing key
    409 — pipeline already running
    500 — pipeline failed or output file missing
    503 — pipeline failed, stale data not returned
    504 — pipeline timed out
"""

import os
import sys
import time
import fcntl
import signal
import subprocess
from pathlib import Path

os.umask(0o002)  # files created by Apache get group write (rw-rw-r--)

ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / "venv" / "bin" / "python"
OUTPUT = ROOT / "output" / "newsletter.html"
LOCK = ROOT / "output" / ".pipeline.lock"
PIPELINE_TIMEOUT = 180  # seconds

sys.path.insert(0, str(ROOT))
import logging
from logging.handlers import RotatingFileHandler
from logger import KSTFormatter, LOG_DIR

# CGI-safe logger: file only, no stdout (stdout = HTTP response in CGI)
log = logging.getLogger("trigger")
log.setLevel(logging.INFO)
log.propagate = False  # prevent root logger from writing to stdout

_fmt = KSTFormatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

_trigger_fh = RotatingFileHandler(
    LOG_DIR / "trigger.log", maxBytes=5*1024*1024, backupCount=3, encoding="utf-8"
)
_trigger_fh.setFormatter(_fmt)
log.addHandler(_trigger_fh)


def respond(status, body, content_type="text/plain; charset=utf-8"):
    """Send a CGI response and exit."""
    log.info("Response: %s", status)
    print(f"Status: {status}")
    print(f"Content-Type: {content_type}")
    print()
    print(body)
    sys.exit(0)


def main():
    client_ip = os.environ.get("REMOTE_ADDR", "unknown")
    log.info("Request from %s", client_ip)

    # Load secret key from .env
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
    expected_key = os.environ.get("CGI_SECRET_KEY", "")

    # Authenticate via header (not query string, to keep secrets out of logs)
    key = os.environ.get("HTTP_X_TRIGGER_KEY", "")

    if not expected_key or key != expected_key:
        log.warning("Auth failed from %s", client_ip)
        respond("403 Forbidden", "Forbidden")

    # Return cached file if fresh enough
    import config
    if OUTPUT.exists():
        age = time.time() - OUTPUT.stat().st_mtime
        if age < config.CACHE_TTL:
            log.info("Cache hit (age %.0fs, TTL %ds)", age, config.CACHE_TTL)
            html = OUTPUT.read_text(encoding="utf-8")
            respond("200 OK", html, "text/html; charset=utf-8")

    # Prevent concurrent runs
    lock_fd = open(LOCK, "w")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (BlockingIOError, OSError):
        lock_fd.close()
        log.warning("Pipeline already running, rejecting request")
        respond("409 Conflict", "Pipeline already running")

    # Record mtime before pipeline runs (to detect stale output)
    old_mtime = OUTPUT.stat().st_mtime if OUTPUT.exists() else None

    # Run pipeline
    log.info("Pipeline started")
    pipeline_start = time.time()
    try:
        result = subprocess.run(
            [str(PYTHON), str(ROOT / "run_pipeline.py")],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
            timeout=PIPELINE_TIMEOUT,
        )
    except subprocess.TimeoutExpired as e:
        # Kill the entire process group to avoid zombie children
        if e.cmd and hasattr(result, "pid"):
            os.killpg(os.getpgid(result.pid), signal.SIGKILL)
        lock_fd.close()
        log.error("Pipeline timed out after %ds", PIPELINE_TIMEOUT)
        respond("504 Gateway Timeout", "Pipeline timed out")
    finally:
        # Always release the lock
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
            lock_fd.close()
        except Exception:
            pass

    elapsed = time.time() - pipeline_start
    log.info("Pipeline finished (%.1fs)", elapsed)

    # Check that output was actually updated
    if not OUTPUT.exists():
        log.error("newsletter.html not found after pipeline")
        respond("500 Internal Server Error", "Pipeline finished but newsletter.html not found")

    new_mtime = OUTPUT.stat().st_mtime
    if old_mtime is not None and new_mtime == old_mtime:
        log.error("newsletter.html not updated (stale)")
        respond("503 Service Unavailable", "Pipeline failed to generate fresh newsletter. Retry later.")

    html = OUTPUT.read_text(encoding="utf-8")
    respond("200 OK", html, "text/html; charset=utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Unexpected error: %s", e)
        respond("500 Internal Server Error", f"Unexpected error: {e}")
