"""
fetch_top_news.py
─────────────────────────────────────────────────────────────────
Fetch latest 3 articles tagged '뉴스레터' from WordPress REST API
and save to data/top_news_latest.json.
"""

import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests
import config

# ── Settings ──────────────────────────────────────────────────────
WP_BASE_URL       = config.WP_BASE_URL
NEWSLETTER_TAG_ID = config.WP_NEWSLETTER_TAG_ID
MAX_ARTICLES      = 3
MAX_FETCH         = 6
REQUEST_TIMEOUT   = 15

OUT_FILE = config.DATA_DIR / "top_news_latest.json"

KST = timezone(timedelta(hours=9))


# ── Helpers ───────────────────────────────────────────────────────
def strip_html(text: str) -> str:
    text = re.sub(r"<[^>]+>", "", text or "")
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')
    return re.sub(r"\s+", " ", text).strip()


def truncate(text: str, max_len: int = 120) -> str:
    text = strip_html(text)
    if len(text) <= max_len:
        return text
    return text[:max_len].rsplit(" ", 1)[0] + "..."


# ── Tag ID lookup ────────────────────────────────────────────────
def resolve_tag_id(tag_name: str = "뉴스레터") -> int:
    """Resolve tag name to tag ID via WP API (fallback if ID=0)"""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/tags",
            params={"search": tag_name, "per_page": 5},
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        items = res.json()
        for item in items:
            if item.get("name") == tag_name:
                return int(item["id"])
        if items:
            return int(items[0]["id"])
    except Exception as e:
        print(f"WARN: tag ID lookup failed: {e}")
    return 0


# ── Category name lookup ────────────────────────────────────────
_cat_cache: dict[int, str] = {}

def get_category_name(cat_id: int) -> str:
    if cat_id in _cat_cache:
        return _cat_cache[cat_id]
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/categories/{cat_id}",
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        name = res.json().get("name", "")
        _cat_cache[cat_id] = name
        return name
    except Exception:
        return ""


# ── Thumbnail URL lookup ─────────────────────────────────────────
def get_thumbnail_url(featured_media_id: int) -> str:
    if not featured_media_id:
        return ""
    try:
        res = requests.get(
            f"{WP_BASE_URL}/wp-json/wp/v2/media/{featured_media_id}",
            timeout=REQUEST_TIMEOUT,
        )
        res.raise_for_status()
        data = res.json()
        sizes = data.get("media_details", {}).get("sizes", {})
        for size in ("medium_large", "medium", "full"):
            if size in sizes:
                return sizes[size].get("source_url", "")
        return data.get("source_url", "")
    except Exception as e:
        print(f"WARN: thumbnail lookup failed (media_id={featured_media_id}): {e}")
        return ""


# ── Fetch posts ──────────────────────────────────────────────────
def fetch_tagged_posts(tag_id: int) -> tuple[str, list[dict]]:
    """
    Fetch posts with newsletter tag and split into:
      - Empty body post -> today's quote (title only)
      - Posts with body  -> news articles (up to 3)
    Returns: (today_quote, articles)
    """
    params = {
        "tags":     tag_id,
        "per_page": MAX_FETCH,
        "orderby":  "date",
        "order":    "desc",
        "_fields":  "id,title,excerpt,content,link,date,featured_media,categories,acf",
    }
    res = requests.get(
        f"{WP_BASE_URL}/wp-json/wp/v2/posts",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    res.raise_for_status()
    posts = res.json()

    today_quote = ""
    articles    = []

    for post in posts:
        body  = strip_html(post.get("content", {}).get("rendered", ""))
        title = strip_html(post["title"].get("rendered", ""))

        # Empty body -> today's quote
        if not body.strip():
            if not today_quote:
                today_quote = title
            continue

        # Posts with body -> news articles
        if len(articles) >= MAX_ARTICLES:
            continue

        cat_ids  = post.get("categories", [])
        cat_name = get_category_name(cat_ids[0]) if cat_ids else ""

        acf = post.get("acf") or {}
        thumb_url = get_thumbnail_url(post.get("featured_media", 0))
        if not thumb_url:
            thumb_url = strip_html(acf.get("fifu_image_url") or "")

        articles.append({
            "id":        post["id"],
            "title":     title,
            "excerpt":   truncate(post["excerpt"].get("rendered", ""), 100),
            "link":      post["link"],
            "date":      post.get("date", ""),
            "category":  cat_name,
            "thumbnail": thumb_url,
        })

    return today_quote, articles


# ── Main ─────────────────────────────────────────────────────────
def main():
    global NEWSLETTER_TAG_ID

    if NEWSLETTER_TAG_ID == 0:
        print("INFO: NEWSLETTER_TAG_ID not set, resolving by tag name...")
        NEWSLETTER_TAG_ID = resolve_tag_id()
        if NEWSLETTER_TAG_ID == 0:
            print("ERROR: could not find newsletter tag")
            sys.exit(1)
        print(f"INFO: tag ID = {NEWSLETTER_TAG_ID}")

    print(f"INFO: fetching newsletter posts (tag_id={NEWSLETTER_TAG_ID})...")
    today_quote, articles = fetch_tagged_posts(NEWSLETTER_TAG_ID)

    if not today_quote:
        print("WARN: no today's quote found")
    else:
        print(f"INFO: today's quote -> {today_quote}")

    if not articles:
        print("WARN: no news articles found")

    output = {
        "fetched_at":  datetime.now(KST).isoformat(),
        "tag_id":      NEWSLETTER_TAG_ID,
        "today_quote": today_quote,
        "count":       len(articles),
        "items":       articles,
    }

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] saved: {OUT_FILE.name}")
    for i, a in enumerate(articles, 1):
        print(f"  news {i}. {a['title'][:60]}")


if __name__ == "__main__":
    main()