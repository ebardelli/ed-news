"""Shared pagination helpers for HTML-scraping preprocessors.

Two strategies are provided:
- paginate_wordpress: numbered WordPress-style pages (/page/2/, /page/3/, …)
- has_new_entries / open_db_conn: primitives for next-link paginators to reuse
"""

import hashlib
import logging
import sqlite3
from typing import Callable, List, Dict

from ednews import config

logger = logging.getLogger(__name__)

MAX_PAGES = 20


def open_db_conn():
    """Return a sqlite3 connection to the configured DB, or None on failure."""
    try:
        return sqlite3.connect(str(config.DB_PATH))
    except Exception:
        return None


def has_new_entries(entries: List[Dict], conn) -> bool:
    """Return True if any entry link is not yet in either items or headlines."""
    cur = conn.cursor()
    for entry in entries:
        link = (entry.get("link") or "").strip()
        if not link:
            continue
        h = hashlib.sha256(link.encode("utf-8")).hexdigest()
        cur.execute("SELECT 1 FROM items WHERE url_hash = ? LIMIT 1", (h,))
        if cur.fetchone():
            continue
        try:
            cur.execute("SELECT 1 FROM headlines WHERE link = ? LIMIT 1", (link,))
            if cur.fetchone():
                continue
        except Exception:
            pass
        return True
    return False


def paginate_wordpress(
    session,
    url: str,
    parse_page: Callable[[bytes], List[Dict]],
    max_pages: int = MAX_PAGES,
) -> List[Dict]:
    """Paginate a WordPress-style site, stopping when a page is fully known to the DB.

    Page 1 is fetched from ``url``; subsequent pages from ``url/page/N/``.
    When no DB connection is available (e.g. tests), all pages up to
    ``max_pages`` are fetched.
    """
    conn = open_db_conn()
    all_entries: List[Dict] = []
    seen_links: set = set()

    try:
        for page_num in range(1, max_pages + 1):
            page_url = (
                url if page_num == 1
                else url.rstrip("/") + f"/page/{page_num}/"
            )

            try:
                resp = session.get(page_url, timeout=20)
                resp.raise_for_status()
            except Exception as exc:
                logger.warning("paginate_wordpress: failed to fetch %s: %s", page_url, exc)
                break

            entries = parse_page(resp.content)
            if not entries:
                logger.debug("paginate_wordpress: no entries on page %d, stopping", page_num)
                break

            new_entries = [
                e for e in entries
                if (e.get("link") or "").strip() not in seen_links
            ]
            for e in new_entries:
                seen_links.add((e.get("link") or "").strip())

            all_entries.extend(new_entries)

            if conn and not has_new_entries(new_entries, conn):
                logger.debug(
                    "paginate_wordpress: page %d fully known to DB, stopping at %s",
                    page_num, page_url,
                )
                break
    finally:
        if conn:
            conn.close()

    return all_entries
