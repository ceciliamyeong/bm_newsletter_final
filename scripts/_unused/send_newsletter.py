"""
send_newsletter.py
──────────────────
스티비 API를 통해 letter.html을 뉴스레터로 발송합니다.

흐름:
  1. POST /emails        → 새 이메일 생성 (매일 새로 생성)
  2. POST /emails/{id}/content → HTML 콘텐츠 업데이트
  3. POST /emails/{id}/send    → 발송

환경변수:
  STIBEE_API_KEY    : 스티비 API 키 (필수)
  STIBEE_LIST_ID    : 스티비 주소록 ID (필수)
  STIBEE_FROM_EMAIL : 발신자 이메일 (필수)
  STIBEE_FROM_NAME  : 발신자 이름 (기본값: 블록미디어)
"""

import os
import sys
import json
import datetime
import requests
from pathlib import Path

# ── 설정 ──────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
LETTER_HTML = ROOT / "output" / "letter.html"

STIBEE_API_KEY    = os.environ.get("STIBEE_API_KEY", "")
STIBEE_LIST_ID    = os.environ.get("STIBEE_LIST_ID", "")
STIBEE_FROM_EMAIL = os.environ.get("STIBEE_FROM_EMAIL", "newsletter@blockmedia.co.kr")
STIBEE_FROM_NAME  = os.environ.get("STIBEE_FROM_NAME", "블록미디어")

BASE_URL = "https://api.stibee.com/v2"

# ── 유효성 체크 ────────────────────────────────────────
def check_env():
    missing = []
    if not STIBEE_API_KEY:  missing.append("STIBEE_API_KEY")
    if not STIBEE_LIST_ID:  missing.append("STIBEE_LIST_ID")
    if missing:
        print(f"[ERROR] 환경변수 누락: {', '.join(missing)}")
        sys.exit(1)

def load_html() -> str:
    if not LETTER_HTML.exists():
        print(f"[ERROR] letter.html 없음: {LETTER_HTML}")
        sys.exit(1)
    content = LETTER_HTML.read_text(encoding="utf-8")
    print(f"[OK] letter.html 로드 완료 ({len(content):,} bytes)")
    return content

# ── 제목 생성 ──────────────────────────────────────────
def extract_headline(html: str) -> str:
    """letter.html 에서 NEWS_HEADLINE 추출 (h1 또는 narrative-title 클래스)"""
    import re
    # narrative-title div 안의 텍스트 추출
    m = re.search(r'class="narrative-title"[^>]*>(.*?)</div>', html, re.DOTALL)
    if m:
        text = re.sub(r"<[^>]+>", "", m.group(1)).replace("\n", " ").strip()
        if text and text != "—":
            return text
    return ""


def make_subject(html: str = "") -> str:
    today = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    date_str = today.strftime("%m/%d")
    weekdays = ["월", "화", "수", "목", "금", "토", "일"]
    weekday  = weekdays[today.weekday()]

    headline = extract_headline(html)
    if headline:
        # 너무 길면 30자 자르기
        if len(headline) > 30:
            headline = headline[:30] + "…"
        return f"[블록미디어] {date_str}({weekday}) {headline}"

    return f"[블록미디어] {date_str}({weekday}) 오늘의 크립토 인사이트"

def json_headers():
    return {
        "AccessToken":  STIBEE_API_KEY,
        "Content-Type": "application/json",
    }

# ── Step 1: 새 이메일 생성 ─────────────────────────────
def create_email(subject: str) -> int:
    url = f"{BASE_URL}/emails"
    payload = {
        "subject":     subject,
        "senderEmail": STIBEE_FROM_EMAIL,
        "senderName":  STIBEE_FROM_NAME,
        "listId":      int(STIBEE_LIST_ID),
    }
    print(f"[→] 이메일 생성: {url}")
    resp = requests.post(url, headers=json_headers(), json=payload, timeout=30)
    print(f"[←] HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"[ERROR] 이메일 생성 실패: {resp.text}")
        sys.exit(1)
    email_id = resp.json().get("id") or resp.json().get("Id")
    print(f"[OK] 이메일 생성 완료 (ID: {email_id})")
    return email_id

# ── Step 2: HTML 콘텐츠 업데이트 ──────────────────────
def update_content(email_id: int, html: str):
    url = f"{BASE_URL}/emails/{email_id}/content"
    headers = {
        "AccessToken":  STIBEE_API_KEY,
        "Content-Type": "text/html",
    }
    print(f"[→] 콘텐츠 업데이트: {url}")
    resp = requests.post(url, headers=headers, data=html.encode("utf-8"), timeout=30)
    print(f"[←] HTTP {resp.status_code}")
    if resp.status_code != 200:
        print(f"[ERROR] 콘텐츠 업데이트 실패: {resp.text}")
        sys.exit(1)
    print("[OK] HTML 콘텐츠 업데이트 완료")

# ── Step 3: 발송 트리거 ────────────────────────────────
def send_email(email_id: int):
    url = f"{BASE_URL}/emails/{email_id}/send"
    print(f"[→] 발송 트리거: {url}")
    resp = requests.post(url, headers=json_headers(), timeout=30)
    print(f"[←] HTTP {resp.status_code}")
    if resp.status_code == 200:
        print(f"[✓] 발송 성공!")
        print(f"    응답: {resp.text}")
    else:
        print(f"[ERROR] 발송 실패: {resp.text}")
        sys.exit(1)

# ── 메인 ──────────────────────────────────────────────
if __name__ == "__main__":
    check_env()
    html     = load_html()
    subject  = make_subject(html)
    print(f"[제목] {subject}")
    email_id = create_email(subject)
    update_content(email_id, html)
    send_email(email_id)
