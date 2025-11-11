from typing import Dict, List
import feedparser
import requests


def _entry_has_local_news_category(entry: dict) -> bool:
    """Return True if the feed entry contains a Local News category.

    Feedparser normally exposes categories in the `tags` attribute as a
    list of dicts with a 'term' key. We accept either 'Local News' or
    bracketed '[Local News]' for compatibility with the fixture.
    """
    tags = entry.get("tags") or []
    if isinstance(tags, list):
        for t in tags:
            if not isinstance(t, dict):
                continue
            term = (t.get("term") or "").strip().lower()
            if not term:
                continue
            if term == "local news" or term == "[local news]" or "local news" in term:
                return True

    cat = entry.get("category") or ""
    if isinstance(cat, str) and "local news" in cat.lower():
        return True

    return False


def pd_education_feed_processor(session: requests.Session, feed_url: str) -> List[Dict]:
    """Fetch the Press Democrat feed and keep only Local News items.

    Args:
        session: requests.Session to use for fetching.
        feed_url: URL of the RSS/Atom feed.

    Returns:
        List of normalized headline dicts (title, link, summary, published)
        containing only items categorized as Local News.
    """
    resp = session.get(feed_url, timeout=15)
    resp.raise_for_status()
    parsed = feedparser.parse(resp.content)
    out: List[Dict] = []
    for e in parsed.entries:
        if not _entry_has_local_news_category(e):
            continue
        out.append(
            {
                "title": e.get("title", ""),
                "link": e.get("link", ""),
                "summary": e.get("summary", ""),
                "published": e.get("published", e.get("updated", "")),
            }
        )
    return out


# Backwards-compatible preprocessor alias
def pd_education_preprocessor(
    session, feed_url: str, publication_id: str | None = None, issn: str | None = None
):
    # reuse existing implementation
    try:
        return pd_education_feed_processor(session, feed_url)
    except Exception:
        return []
