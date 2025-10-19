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
from bs4 import BeautifulSoup

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


def fcmat_processor(html: str, base_url: str | None = None) -> List[Dict]:
    """Parse FCMAT headlines page HTML and extract headline items.

    The fixture in `tests/fixtures/fcmat.html` contains a section
    with id "fcmatnewsupdates" and repeated `.col-lg-4` columns each
    containing an <h4><a> title and a .date-published paragraph.

    Returns a list of dicts: title, link, summary, published.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict] = []

    container = soup.select_one("section#fcmatnewsupdates")
    if not container:
        # fallback: search whole document for the column blocks
        blocks = soup.select(".col-lg-4")
    else:
        blocks = container.select(".col-lg-4")

    for b in blocks:
        a = b.select_one("h4 a")
        if not a:
            continue
        title = a.get_text(strip=True)
        link = a.get("href") or ""
        # Normalize relative links if base_url provided
        if base_url and link and link.startswith("/"):
            link = base_url.rstrip("/") + link

        date_p = b.select_one("p.date-published")
        published = date_p.get_text(separator=" ", strip=True) if date_p else ""

        # The summary is usually the first <p> after the date paragraph
        summary = ""
        if date_p:
            # find the next sibling paragraph inside the block
            next_p = date_p.find_next_sibling("p")
            if next_p:
                summary = next_p.get_text(strip=True)
        else:
            # fallback: first non-date paragraph
            p = b.select_one("p:not(.date-published)")
            if p:
                summary = p.get_text(strip=True)

        out.append({"title": title, "link": link, "summary": summary, "published": published})

    return out


PROCESSORS = {"fcmat": fcmat_processor}


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
