"""Database initialization and maintenance helpers for ed-news.

This module contains functions for creating and migrating the SQLite
schema, creating supporting views, and other maintenance tasks such as
vacuuming the database or synchronizing publications from a feeds list.

Moved into the `ednews.db` package to group DB related helpers.
"""
from datetime import datetime, timezone, timedelta
import logging
from email.utils import parsedate_to_datetime
import sqlite3
import time
import os

logger = logging.getLogger("ednews.manage_db")

try:
    from .. import config
except Exception:
    import config


def init_db(conn: sqlite3.Connection):
    """Initialize the database schema and create required tables/views.

    Creates the `items`, `articles`, `publications`, and `headlines`
    tables if they do not exist and attempts to create the
    `combined_articles` view used elsewhere in the project.
    """
    logger.info("Initializing database schema")
    # Some tests pass a DummyConn that only implements close(); be tolerant
    # and skip initialization if the object doesn't provide a cursor().
    if not hasattr(conn, "cursor"):
        logger.debug("init_db: connection object has no cursor(); skipping init")
        return None
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            feed_id TEXT,
            guid TEXT,
            title TEXT,
            link TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT,
            UNIQUE(guid, link, title, published)
        )
        """,
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            title TEXT,
            authors TEXT,
            abstract TEXT,
            crossref_xml TEXT,
            feed_id TEXT,
            publication_id TEXT,
            issn TEXT,
            published TEXT,
            fetched_at TEXT,
            UNIQUE(doi)
        )
        """,
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publications (
            feed_id TEXT,
            publication_id TEXT NOT NULL,
            feed_title TEXT,
            issn TEXT NOT NULL,
            PRIMARY KEY (publication_id, issn)
        )
        """,
    )

    # Table for saving scraped or fetched news headlines from news.json sites.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS headlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            title TEXT,
            text TEXT,
            link TEXT,
            first_seen TEXT,
            published TEXT,
            UNIQUE(link, title)
        )
        """,
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_headlines_source_first_seen ON headlines (source, first_seen)
        """,
    )

    # Table for auditing/recording maintenance runs
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS maintenance_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            command TEXT NOT NULL,
            status TEXT,
            started TEXT,
            finished TEXT,
            duration REAL,
            details TEXT
        )
        """,
    )

    try:
        # ensure the combined_articles view exists immediately after initializing schema
        create_combined_view(conn)
    except Exception:
        logger.exception("Failed to create combined_articles view during init_db")
    conn.commit()
    logger.debug("initialized database")


def create_combined_view(conn: sqlite3.Connection):
    """Create the combined_articles view used by higher-level code.

    The view exposes a normalized set of fields for rendering the site
    and for selecting recent articles.
    """
    logger.info("Creating combined_articles view")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE VIEW IF NOT EXISTS combined_articles AS
        SELECT
            articles.doi AS doi,
            COALESCE(articles.title, '') AS title,
            ('https://doi.org/' || articles.doi) AS link,
            COALESCE(publications.feed_title, feeds.feed_title, '') AS feed_title,
            COALESCE(articles.abstract, '') AS content,
            COALESCE(articles.published, articles.fetched_at) AS published,
            COALESCE(articles.authors, '') AS authors
        FROM articles
            LEFT JOIN publications on publications.feed_id = articles.feed_id
            LEFT JOIN publications as feeds on feeds.feed_id = articles.feed_id
        WHERE articles.doi IS NOT NULL
        """,
    )
    conn.commit()
    logger.debug("combined_articles view created")


def log_maintenance_run(conn: sqlite3.Connection, command: str, status: str, started: str | None = None, finished: str | None = None, duration: float | None = None, details: dict | None = None) -> int:
    """Insert an entry into maintenance_runs and return the new row id.

    `details` will be JSON-serialized. Tolerant to errors: logs and returns 0 on failure.
    """
    try:
        cur = conn.cursor()
        import json
        details_json = json.dumps(details) if details is not None else None
        cur.execute(
            "INSERT INTO maintenance_runs (command, status, started, finished, duration, details) VALUES (?, ?, ?, ?, ?, ?)",
            (command, status, started, finished, duration, details_json),
        )
        conn.commit()
        return cur.lastrowid if hasattr(cur, 'lastrowid') else 0
    except Exception:
        logger.exception("Failed to log maintenance run for command=%s", command)
        return 0


def sync_publications_from_feeds(conn, feeds_list) -> int:
    """Synchronize the publications table from a feeds list.

    feeds_list is expected to be the output of `ednews.feeds.load_feeds()` where
    each item is a tuple like (key, title, url, publication_id, issn).

    Returns the number of feeds successfully upserted.
    """
    if not feeds_list:
        return 0
    count = 0
    from . import upsert_publication

    for item in feeds_list:
        try:
            # item shape: (key, title, url, publication_id, issn)
            key = item[0] if len(item) > 0 else None
            title = item[1] if len(item) > 1 else None
            pub_id = item[3] if len(item) > 3 else None
            issn = item[4] if len(item) > 4 else None
            ok = upsert_publication(conn, key, pub_id, title, issn)
            if ok:
                count += 1
        except Exception:
            logger.exception("Failed to sync publication for feed item: %s", item)
            continue
    logger.info("Synchronized %d publications from feeds", count)
    return count


def fetch_latest_journal_works(conn: sqlite3.Connection, feeds, per_journal: int = 30, timeout: int = 10, delay: float = 0.05):
    """Fetch recent works for journals (by ISSN) and insert as articles.

    This function is intended as a maintenance/lookup operation and uses
    Crossref's API to fetch recent works for journals that have an ISSN in
    the provided feeds list.
    """
    import requests

    cur = conn.cursor()
    session = requests.Session()
    # allow callers to pass a timeout; otherwise use config defaults (connect, read)
    try:
        from ednews import config as _config
        connect_timeout = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
        read_timeout = getattr(_config, 'CROSSREF_TIMEOUT', 30)
        default_retries = getattr(_config, 'CROSSREF_RETRIES', 3)
        backoff = getattr(_config, 'CROSSREF_BACKOFF', 0.3)
        status_forcelist = getattr(_config, 'CROSSREF_STATUS_FORCELIST', [429, 500, 502, 503, 504])
    except Exception:
        # fallback values
        connect_timeout = 5
        read_timeout = 30
        default_retries = 3
        backoff = 0.3
        status_forcelist = [429, 500, 502, 503, 504]
    attempts = max(1, int(default_retries) + 1)
    inserted = 0
    skipped = 0
    logger.info("Fetching latest journal works for %s feeds", len(feeds) if hasattr(feeds, '__len__') else 'unknown')
    for item in feeds:
        if len(item) == 5:
            key, title, url, publication_id, issn = item
        elif len(item) == 4:
            key, title, url, publication_id = item
            issn = None
        else:
            continue
        if not (issn):
            continue
        try:
            ua = None
            try:
                ua = getattr(config, 'USER_AGENT', None)
            except Exception:
                ua = None
            headers = {"User-Agent": ua or "ed-news-fetcher/1.0", "Accept": "application/json"}
            mailto = os.environ.get("CROSSREF_MAILTO", "your_email@example.com")
            url = f"https://api.crossref.org/journals/{issn}/works"
            params = {"sort": "created", "order": "desc", "filter": "type:journal-article", "rows": min(per_journal, 100), "mailto": mailto}
            used_timeout = (connect_timeout, timeout if timeout and timeout > 0 else read_timeout)
            resp = None
            last_exc = None
            for attempt in range(1, attempts + 1):
                try:
                    resp = session.get(url, params=params, headers=headers, timeout=used_timeout)
                    if resp.status_code in status_forcelist:
                        last_exc = requests.HTTPError(f"status={resp.status_code}")
                        raise last_exc
                    resp.raise_for_status()
                    break
                except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                    last_exc = e
                    logger.warning("Request attempt %d/%d failed for ISSN=%s: %s", attempt, attempts, issn, e)
                except requests.HTTPError as e:
                    last_exc = e
                    code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    if code in status_forcelist:
                        logger.warning("HTTP %s on attempt %d/%d for ISSN=%s: will retry", code, attempt, attempts, issn)
                    else:
                        raise
                if attempt < attempts:
                    sleep_for = backoff * (2 ** (attempt - 1))
                    sleep_for = sleep_for + (0.1 * backoff)
                    time.sleep(sleep_for)

            if resp is None:
                raise last_exc if last_exc is not None else Exception("Failed to retrieve Crossref data")
            data = resp.json()
            items = data.get("message", {}).get("items", []) or []
            for it in items[:per_journal]:
                doi = (it.get("DOI") or "").strip()
                if not doi:
                    continue
                norm = doi
                if not norm:
                    continue
                try:
                    from ednews.db import article_exists, upsert_article, update_article_crossref
                    # If the DOI already exists, skip to avoid re-processing.
                    if article_exists(conn, norm):
                        skipped += 1
                        continue

                    try:
                        from ednews.crossref import fetch_crossref_metadata
                        cr = fetch_crossref_metadata(norm)
                    except Exception:
                        cr = None

                    authors_val = cr.get('authors') if cr and cr.get('authors') else None
                    abstract_val = cr.get('abstract') if cr and cr.get('abstract') else it.get('abstract')
                    published_val = cr.get('published') if cr and cr.get('published') else None

                    aid = upsert_article(
                        conn,
                        norm,
                        title=it.get('title'),
                        authors=authors_val,
                        abstract=abstract_val,
                        feed_id=key,
                        publication_id=issn,
                        issn=issn,
                        fetched_at=None,
                        published=published_val,
                    )
                    if aid:
                        inserted += 1

                    if cr and cr.get('raw'):
                        try:
                            update_article_crossref(conn, norm, authors=authors_val, abstract=abstract_val, raw=cr.get('raw'), published=published_val)
                        except Exception:
                            logger.debug("Failed to update crossref data for doi=%s after upsert", norm)
                except Exception:
                    logger.exception("Failed to upsert article doi=%s from journal %s", doi, issn)
            conn.commit()
        except Exception:
            logger.exception("Failed to fetch works for ISSN=%s (feed=%s)", issn, key)
    logger.info("ISSN lookup summary: inserted=%d skipped=%d", inserted, skipped)
    return inserted


def vacuum_db(conn: sqlite3.Connection):
    """Run VACUUM to defragment the SQLite database file."""
    try:
        cur = conn.cursor()
        cur.execute("VACUUM")
        conn.commit()
        logger.info("Database vacuumed")
        return True
    except Exception:
        logger.exception("VACUUM failed")
        return False


def cleanup_empty_articles(conn: sqlite3.Connection, older_than_days: int | None = None) -> int:
    """Delete articles that have no title and no abstract.

    If `older_than_days` is provided, only delete articles whose
    `fetched_at` or `published` timestamp is older than the cutoff.

    Returns the number of rows deleted.
    """
    try:
        cur = conn.cursor()
        params = []
        where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
        if older_than_days is not None:
            # compute cutoff ISO timestamp
            cutoff = (datetime.now(timezone.utc) - timedelta(days=int(older_than_days))).isoformat()
            where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
            params.extend([cutoff, cutoff])
        where_sql = " AND ".join(where_clauses)
        # Use DELETE ... WHERE ... and return rowcount
        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(params))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        conn.commit()
        logger.info("cleanup_empty_articles deleted %s rows (older_than_days=%s)", deleted, older_than_days)
        return deleted or 0
    except Exception:
        logger.exception("cleanup_empty_articles failed")
        return 0


def migrate_db(conn: sqlite3.Connection):
    """Placeholder for schema migrations.

    Currently a no-op. Add migration steps here as needed. Returns True
    if migrations applied or no-op succeeded.
    """
    logger.info("migrate_db: no migrations to apply")
    return True
