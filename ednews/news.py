"""News aggregator for ad-hoc site processors and RSS feeds.

This module loads `news.json`, and for each configured site either
parses an RSS/Atom feed or delegates to a site-specific processor
that extracts headlines from HTML.

Only lightweight functionality is implemented: fetch, normalize,
and return a list of headline dicts with keys: title, link,
summary, and published (string when available).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

import feedparser
import requests

CONFIG_PATH = Path("news.json")


def load_config(path: Path | str | None = None) -> Dict:
    """Load the news configuration JSON.

    Args:
        path: Optional path to a JSON file. Defaults to `news.json` in cwd.

    Returns:
        Parsed JSON as a dict.
    """
    p = Path(path) if path else CONFIG_PATH
    with p.open("r", encoding="utf-8") as fh:
        return json.load(fh)


from ednews.processors.fcmat import fcmat_processor
from ednews.processors.pressdemocrat import pd_education_feed_processor


PROCESSORS = {"fcmat": fcmat_processor}


# feed-specific processors (take session, feed_url)
FEED_PROCESSORS = {"pd-education": pd_education_feed_processor}


def fetch_site(session: requests.Session, site_cfg: Dict) -> List[Dict]:
    """Fetch a single site configuration and return normalized headline dicts.

    site_cfg is expected to contain keys: title, link, feed, processor.
    If `feed` is provided and non-empty, parse it with feedparser.
    If `processor` is provided and matches a function in PROCESSORS,
    fetch the HTML and run the processor.
    """
    feed_url = site_cfg.get("feed", "")
    processor_name = site_cfg.get("processor")
    link = site_cfg.get("link")

    if feed_url:
        # If a feed-specific processor exists (e.g. to filter AP items),
        # prefer it. Otherwise fall back to the simple feedparser path.
        proc = FEED_PROCESSORS.get(processor_name) if processor_name else None
        if proc:
            return proc(session, feed_url)

        parsed = feedparser.parse(feed_url)
        out: List[Dict] = []
        for e in parsed.entries:
            out.append({
                "title": e.get("title", ""),
                "link": e.get("link", ""),
                "summary": e.get("summary", ""),
                "published": e.get("published", e.get("updated", "")),
            })
        return out

    if processor_name:
        fn = PROCESSORS.get(processor_name)
        if not fn:
            raise ValueError(f"Unknown processor: {processor_name}")
        resp = session.get(link, timeout=15)
        resp.raise_for_status()
        html = resp.text
        return fn(html, base_url=link)

    # Nothing configured
    return []


def fetch_all(session: requests.Session | None = None, cfg_path: str | Path | None = None, conn: object | None = None) -> Dict[str, List[Dict]]:
    """Load configuration and fetch headlines for all configured sites.

    Returns a mapping from site key to list of headline dicts.
    """
    cfg = load_config(cfg_path)
    sites = cfg.get("feeds", {})
    s = session or requests.Session()
    results: Dict[str, List[Dict]] = {}
    # Lazy import DB saving helpers to avoid circular imports at module import time
    save_fn = None
    if conn is not None:
        try:
            from ednews.db import save_news_items

            save_fn = save_news_items
        except Exception:
            save_fn = None

    for key, site in sites.items():
        try:
            items = fetch_site(s, site)
        except Exception:
            # best-effort: return empty list on failure
            items = []
        # persist if a DB connection and saver function are available
        if save_fn and conn is not None:
            try:
                save_fn(conn, key, items)
            except Exception:
                # don't let DB issues stop the run
                pass
        results[key] = items
    return results
