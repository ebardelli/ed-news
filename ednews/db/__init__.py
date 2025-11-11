"""Database helpers for ed-news.

This module provides convenience functions to open SQLite connections,
initialize schema, and perform common upsert and query operations on the
`items`, `articles`, and `publications` tables used by the project.

This package was created by moving the legacy top-level `ednews.db` module
into a `ednews.db` package so that maintenance helpers can live under
`ednews.db.manage_db`. The public API and import paths (`from ednews.db import ...`)
remain the same.
"""

import sqlite3
from datetime import datetime, timezone
import logging
from email.utils import parsedate_to_datetime
from .. import config

logger = logging.getLogger("ednews.db")


def get_connection(path: str | None = None):
    """Return a SQLite connection.

    If `path` is provided a connection to that file path is opened; otherwise
    an in-memory connection is returned. Exceptions are propagated after being
    logged.
    """
    try:
        if path:
            logger.debug("Opening SQLite connection to path: %s", path)
            return sqlite3.connect(path)
        logger.debug("Opening in-memory SQLite connection")
        # Explicit ':memory:' path to satisfy type checker (no arg form not typed)
        return sqlite3.connect(":memory:")
    except Exception:
        logger.exception("Failed to open SQLite connection (path=%s)", path)
        raise


# Backwards-compatible shims: initialization/maintenance helpers were moved
# into smaller modules under `ednews.db` (schema, maintenance, migrations).
# Provide package-level wrappers and also synthesize a `ednews.db.manage_db`
# module in sys.modules so older `import ednews.db.manage_db` code continues to work.
try:
    from .schema import (
        init_db as _init_db,
        create_combined_view as _create_combined_view,
    )
    from .maintenance import (
        sync_publications_from_feeds as _sync_publications_from_feeds,
        fetch_latest_journal_works as _fetch_latest_journal_works,
        vacuum_db as _vacuum_db,
        log_maintenance_run as _log_maintenance_run,
        cleanup_empty_articles as _cleanup_empty_articles,
        cleanup_filtered_titles as _cleanup_filtered_titles,
        rematch_publication_dois as _rematch_publication_dois,
        remove_feed_articles as _remove_feed_articles,
    )
    from .migrations import (
        migrate_db as _migrate_db,
        migrate_add_items_url_hash as _migrate_add_items_url_hash,
    )

    def init_db(conn: sqlite3.Connection):
        """Initialize DB schema (uses `ednews.db.schema.init_db`)."""
        return _init_db(conn)

    def create_combined_view(conn: sqlite3.Connection):
        """Create combined_articles view (uses `ednews.db.schema.create_combined_view`)."""
        return _create_combined_view(conn)

    def sync_publications_from_feeds(conn, feeds_list) -> int:
        """Synchronize publications (uses `ednews.db.maintenance.sync_publications_from_feeds`)."""
        return _sync_publications_from_feeds(conn, feeds_list)

    def fetch_latest_journal_works(
        conn: sqlite3.Connection,
        feeds,
        per_journal: int = 30,
        timeout: int = 10,
        delay: float = 0.05,
    ):
        return _fetch_latest_journal_works(
            conn, feeds, per_journal=per_journal, timeout=timeout, delay=delay
        )

    # Also expose migration and maintenance helpers at package level
    migrate_db = _migrate_db
    migrate_add_items_url_hash = _migrate_add_items_url_hash
    vacuum_db = _vacuum_db
    log_maintenance_run = _log_maintenance_run
    cleanup_empty_articles = _cleanup_empty_articles
    cleanup_filtered_titles = _cleanup_filtered_titles
    rematch_publication_dois = _rematch_publication_dois

    # Create a synthetic submodule `ednews.db.manage_db` so code that does
    # `import ednews.db.manage_db` or `from ednews.db import manage_db` keeps working.
    try:
        import sys
        import types
        from typing import Any

        mod_name = __name__ + ".manage_db"
        if mod_name not in sys.modules:
            manage_mod = types.ModuleType(mod_name)
            # Cast to Any so we can assign dynamic attributes without Pyright
            # complaining about unknown ModuleType attributes.
            manage_mod_any: Any = manage_mod  # type: ignore[assignment]
            manage_mod_any.init_db = _init_db
            manage_mod_any.create_combined_view = _create_combined_view
            manage_mod_any.sync_publications_from_feeds = _sync_publications_from_feeds
            manage_mod_any.fetch_latest_journal_works = _fetch_latest_journal_works
            manage_mod_any.migrate_db = _migrate_db
            manage_mod_any.migrate_add_items_url_hash = _migrate_add_items_url_hash
            manage_mod_any.vacuum_db = _vacuum_db
            manage_mod_any.log_maintenance_run = _log_maintenance_run
            manage_mod_any.cleanup_empty_articles = _cleanup_empty_articles
            manage_mod_any.cleanup_filtered_titles = _cleanup_filtered_titles
            manage_mod_any.rematch_publication_dois = _rematch_publication_dois
            manage_mod_any.remove_feed_articles = _remove_feed_articles
            # also expose at package level
            remove_feed_articles = _remove_feed_articles
            sys.modules[mod_name] = manage_mod
    except Exception:
        logger.exception(
            "Failed to synthesize ednews.db.manage_db module for backwards compatibility"
        )
except Exception:
    logger.debug(
        "ednews.db.manage modules not importable; init/mgmt wrappers not installed"
    )


# Database initialization and maintenance functions have been moved to
# `ednews.db.manage_db`. This module now focuses on the DB API used by the
# rest of the application (upsert_article, article_exists, etc.).


def upsert_article(
    conn,
    doi: str | None,
    title: str | None,
    authors: str | None,
    abstract: str | None,
    feed_id: str | None = None,
    publication_id: str | None = None,
    issn: str | None = None,
    fetched_at: str | None = None,
    published: str | None = None,
):
    """Insert or update an article row by DOI.

    Sanitizes inputs and attempts an INSERT with ON CONFLICT DO UPDATE. If
    that fails, a fallback INSERT OR REPLACE is attempted. Returns the
    article id on success or False/None on failure.
    """
    if not doi:
        return False
    # Prevent inserting articles whose titles are blacklisted via config
    try:
        from .. import config as _config

        if title and isinstance(title, str):
            tnorm = title.strip().lower()
            filters = getattr(_config, "TITLE_FILTERS", []) or []
            if any(tnorm == f.strip().lower() for f in filters):
                logger.info("Skipping upsert for filtered title=%s doi=%s", title, doi)
                return False
    except Exception:
        # If config can't be read, continue normally
        pass
    logger.debug(
        "Upserting article doi=%s feed_id=%s publication_id=%s",
        doi,
        feed_id,
        publication_id,
    )
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    used_fetched_at = fetched_at or now
    used_published = published

    # sanitize inputs to avoid SQLite binding errors (sqlite does not accept list/bytes)
    def _sanitize(val):
        if val is None:
            return None
        # decode bytes
        if isinstance(val, (bytes, bytearray)):
            try:
                return val.decode("utf-8")
            except Exception:
                return val.decode("utf-8", errors="replace")
        # join lists/tuples into a string
        if isinstance(val, (list, tuple, set)):
            try:
                return ", ".join(str(x) for x in val)
            except Exception:
                return str(val)
        # convert other non-str types to string
        if not isinstance(val, str):
            try:
                return str(val)
            except Exception:
                return None
        return val

    # apply sanitization
    doi = _sanitize(doi)
    title = _sanitize(title)
    authors = _sanitize(authors)
    abstract = _sanitize(abstract)
    feed_id = _sanitize(feed_id)
    publication_id = _sanitize(publication_id)
    issn = _sanitize(issn)
    used_fetched_at = _sanitize(used_fetched_at)
    used_published = _sanitize(used_published)
    try:
        cur.execute(
            """
            INSERT INTO articles (doi, title, authors, abstract, feed_id, publication_id, issn, fetched_at, published)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doi) DO UPDATE SET
                title=excluded.title,
                authors=excluded.authors,
                abstract=excluded.abstract,
                feed_id=COALESCE(excluded.feed_id, articles.feed_id),
                publication_id=COALESCE(excluded.publication_id, articles.publication_id),
                issn=COALESCE(excluded.issn, articles.issn),
                fetched_at=excluded.fetched_at,
                published=COALESCE(excluded.published, articles.published)
            """,
            (
                doi,
                title,
                authors,
                abstract,
                feed_id,
                publication_id,
                issn,
                used_fetched_at,
                used_published,
            ),
        )
        conn.commit()
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        aid = row[0] if row and row[0] else None
        logger.debug("Upsert successful for doi=%s id=%s", doi, aid)
        return aid
    except Exception:
        logger.exception(
            "Upsert failed, attempting fallback INSERT OR REPLACE for doi=%s", doi
        )
        try:
            cur.execute(
                """
                INSERT OR REPLACE INTO articles (id, doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, fetched_at, published)
                VALUES (
                    (SELECT id FROM articles WHERE doi = ?), ?, ?, ?, ?, COALESCE(?, (SELECT crossref_xml FROM articles WHERE doi = ?)), COALESCE(?, (SELECT feed_id FROM articles WHERE doi = ?)), COALESCE((SELECT publication_id FROM articles WHERE doi = ?), ?), COALESCE((SELECT issn FROM articles WHERE doi = ?), ?), ?, COALESCE(?, (SELECT published FROM articles WHERE doi = ?))
                )
                """,
                # Parameters must match the 16 '?' placeholders in the VALUES clause
                (
                    doi,  # (SELECT id FROM articles WHERE doi = ?)
                    doi,  # doi value for the doi column
                    title,
                    authors,
                    abstract,
                    None,  # crossref_xml explicit value
                    doi,  # (SELECT crossref_xml FROM articles WHERE doi = ?)
                    feed_id,  # feed_id explicit value for COALESCE
                    doi,  # (SELECT feed_id FROM articles WHERE doi = ?)
                    doi,  # (SELECT publication_id FROM articles WHERE doi = ?)
                    publication_id,  # provided publication_id fallback
                    doi,  # (SELECT issn FROM articles WHERE doi = ?)
                    issn,  # provided issn fallback
                    used_fetched_at,
                    used_published,  # provided published value for COALESCE
                    doi,  # (SELECT published FROM articles WHERE doi = ?)
                ),
            )
            conn.commit()
            cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
            row = cur.fetchone()
            aid = row[0] if row and row[0] else None
            logger.debug("Fallback upsert successful for doi=%s id=%s", doi, aid)
            return aid
        except Exception:
            logger.exception("Fallback upsert failed for doi=%s", doi)
            return False


def ensure_article_row(
    conn,
    doi: str,
    title: str | None = None,
    authors: str | None = None,
    abstract: str | None = None,
    feed_id: str | None = None,
    publication_id: str | None = None,
    issn: str | None = None,
) -> int | None:
    """Ensure an article row exists for the given DOI.

    Performs an INSERT OR IGNORE and returns the article id (or None).
    """
    cur = conn.cursor()
    if not doi:
        logger.debug("ensure_article_row called without doi; skipping")
        return None
    # Prevent inserting articles with blacklisted titles
    try:
        from .. import config as _config

        if title and isinstance(title, str):
            tnorm = title.strip().lower()
            filters = getattr(_config, "TITLE_FILTERS", []) or []
            if any(tnorm == f.strip().lower() for f in filters):
                logger.info(
                    "Skipping ensure_article_row for filtered title=%s doi=%s",
                    title,
                    doi,
                )
                return None
    except Exception:
        pass
    try:
        cur.execute(
            "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, feed_id, publication_id, issn, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                doi,
                title,
                authors,
                abstract,
                feed_id,
                publication_id,
                issn,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        aid = row[0] if row and row[0] else None
        logger.debug("ensure_article_row result for doi=%s id=%s", doi, aid)
        return aid
    except Exception:
        logger.exception("ensure_article_row failed for doi=%s", doi)
        return None


def article_exists(conn: sqlite3.Connection, doi: str) -> bool:
    """Return True if an article with the given DOI already exists in the articles table."""
    if not doi:
        return False
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM articles WHERE doi = ? LIMIT 1", (doi,))
        return bool(cur.fetchone())
    except Exception:
        logger.exception("Failed to check existence for doi=%s", doi)
        return False


def get_article_metadata(conn: sqlite3.Connection, doi: str) -> dict | None:
    """Return stored article metadata for a DOI as a dict.

    Returns a dict with keys 'authors', 'abstract', 'raw', 'published' when
    available, or None if the DOI isn't found.
    """
    if not doi:
        return None
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT authors, abstract, crossref_xml, published FROM articles WHERE doi = ? LIMIT 1",
            (doi,),
        )
        row = cur.fetchone()
        if not row:
            return None
        authors, abstract, crossref_xml, published = row
        out = {}
        if authors:
            out["authors"] = authors
        if abstract:
            out["abstract"] = abstract
        if crossref_xml:
            out["raw"] = crossref_xml
        if published:
            out["published"] = published
        return out
    except Exception:
        logger.exception("Failed to fetch article metadata for doi=%s", doi)
        return None


def get_article_by_title(conn: sqlite3.Connection, title: str) -> dict | None:
    """Return article row fields for an article whose title matches exactly (case-insensitive).

    Returns a dict with keys 'doi', 'title', 'authors', 'abstract', 'raw', 'published' when
    available, or None if no matching article is found.
    """
    if not title:
        return None
    try:
        cur = conn.cursor()
        # Use case-insensitive comparison on trimmed title
        cur.execute(
            "SELECT doi, title, authors, abstract, crossref_xml, published FROM articles WHERE lower(trim(title)) = lower(trim(?)) LIMIT 1",
            (title,),
        )
        row = cur.fetchone()
        if not row:
            return None
        doi, atitle, authors, abstract, crossref_xml, published = row
        out = {}
        if doi:
            out["doi"] = doi
        if atitle:
            out["title"] = atitle
        if authors:
            out["authors"] = authors
        if abstract:
            out["abstract"] = abstract
        if crossref_xml:
            out["raw"] = crossref_xml
        if published:
            out["published"] = published
        return out
    except Exception:
        logger.exception("Failed to fetch article by title=%s", title)
        return None


def enrich_articles_from_crossref(
    conn, fetcher, batch_size: int = 20, delay: float = 0.1, return_ids: bool = False
):
    """Enrich articles missing crossref_xml using the provided fetcher.

    The `fetcher` callable should accept a DOI and return a dict (as returned
    by `crossref.fetch_crossref_metadata`) or None. Returns either the count of
    updated articles or a list of updated article ids when `return_ids` is True.
    """
    cur = conn.cursor()
    logger.info("Enriching up to %s articles from Crossref", batch_size)
    # Use LEFT JOIN so articles that don't have a matching items row are still
    # considered for Crossref enrichment (previously an INNER JOIN excluded
    # articles that only exist in the `articles` table).
    cur.execute(
        "SELECT articles.doi FROM articles LEFT JOIN items on items.doi = articles.doi WHERE articles.crossref_xml IS NULL OR articles.crossref_xml = '' ORDER BY COALESCE(items.published, items.fetched_at, articles.fetched_at) DESC LIMIT ?",
        (batch_size,),
    )
    rows = cur.fetchall()
    updated = 0
    updated_ids: list[int] = []
    logger.debug("Found %s articles needing enrichment", len(rows))
    for r in rows:
        doi = r[0]
        if not doi:
            continue
        try:
            cr = fetcher(doi)
            if not cr:
                logger.debug("No crossref data for doi=%s", doi)
                continue
            authors = cr.get("authors")
            abstract = cr.get("abstract")
            raw = cr.get("raw")
            cur.execute(
                "UPDATE articles SET authors = COALESCE(?, authors), abstract = COALESCE(?, abstract), crossref_xml = ? WHERE doi = ?",
                (authors, abstract, raw, doi),
            )
            conn.commit()
            # lookup article id for targeted embedding updates
            try:
                cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
                row = cur.fetchone()
                if row and row[0]:
                    updated_ids.append(row[0])
            except Exception:
                logger.debug("Could not fetch id for doi=%s after update", doi)
            updated += 1
            logger.debug("Enriched doi=%s", doi)
        except Exception:
            logger.exception("Failed to enrich doi=%s from Crossref", doi)
    if return_ids:
        return updated_ids
    return updated


def get_missing_crossref_dois(
    conn: sqlite3.Connection, limit: int = 100, offset: int = 0
) -> list:
    """Return a list of DOIs for articles where crossref_xml is NULL or empty.

    Results are ordered by a best-effort recency using items.published, items.fetched_at,
    or articles.fetched_at (descending), similar to the enrichment query.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT articles.doi FROM articles LEFT JOIN items on items.doi = articles.doi WHERE articles.doi IS NOT NULL AND (articles.crossref_xml IS NULL OR articles.crossref_xml = '') ORDER BY COALESCE(items.published, items.fetched_at, articles.fetched_at) DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )
    rows = cur.fetchall()
    dois = [r[0] for r in rows if r and r[0]]
    logger.debug(
        "get_missing_crossref_dois found %d DOIs (limit=%s offset=%s)",
        len(dois),
        limit,
        offset,
    )
    return dois


def update_article_crossref(
    conn: sqlite3.Connection,
    doi: str,
    authors: str | None = None,
    abstract: str | None = None,
    raw: str | None = None,
    published: str | None = None,
) -> bool:
    """Update the articles row for a given DOI with Crossref-derived metadata.

    Fields provided as None will not overwrite existing values. Returns True if
    the update affected a row, False otherwise.
    """
    if not doi:
        logger.debug("update_article_crossref called without doi; skipping")
        return False
    try:
        cur = conn.cursor()
        # Build the update using COALESCE so None values don't clobber existing data
        cur.execute(
            """
            UPDATE articles SET
                authors = COALESCE(?, authors),
                abstract = COALESCE(?, abstract),
                crossref_xml = COALESCE(?, crossref_xml),
                published = COALESCE(?, published)
            WHERE doi = ?
            """,
            (authors, abstract, raw, published, doi),
        )
        conn.commit()
        updated = cur.rowcount if hasattr(cur, "rowcount") else None
        logger.debug("update_article_crossref doi=%s updated_rows=%s", doi, updated)
        return (updated is None) or (updated > 0)
    except Exception:
        logger.exception("Failed to update article crossref data for doi=%s", doi)
        return False


def upsert_publication(
    conn,
    feed_id: str | None,
    publication_id: str | None,
    feed_title: str | None,
    issn: str | None,
):
    """Insert or update a publications row.

    Primary key is (publication_id, issn) per schema. If publication_id is missing,
    fall back to using feed_id as an identifier when available.
    """
    if not publication_id and not feed_id:
        logger.debug(
            "upsert_publication called without publication_id or feed_id; skipping"
        )
        return False
    # prefer explicit issn; ensure non-null string for SQL binding
    try:
        cur = conn.cursor()
        # Use publication_id and issn as primary identifying keys when possible
        if publication_id and issn:
            cur.execute(
                """
                INSERT INTO publications (feed_id, publication_id, feed_title, issn)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(publication_id, issn) DO UPDATE SET
                    feed_id = COALESCE(excluded.feed_id, publications.feed_id),
                    feed_title = COALESCE(excluded.feed_title, publications.feed_title)
                """,
                (feed_id, publication_id, feed_title, issn),
            )
        else:
            # If we don't have an ISSN, try to upsert by publication_id alone by using publication_id as key
            # Use INSERT OR REPLACE fallback for older SQLite versions or missing ON CONFLICT support
            cur.execute(
                "INSERT OR REPLACE INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)",
                (feed_id, publication_id or feed_id, feed_title, issn or ""),
            )
        conn.commit()
        logger.debug(
            "upsert_publication succeeded for feed_id=%s publication_id=%s issn=%s",
            feed_id,
            publication_id,
            issn,
        )
        return True
    except Exception:
        logger.exception(
            "upsert_publication failed for feed_id=%s publication_id=%s issn=%s",
            feed_id,
            publication_id,
            issn,
        )
        return False


def sync_publications_from_feeds(conn, feeds_list) -> int:
    """Synchronize the publications table from a feeds list.

    feeds_list is expected to be the output of `ednews.feeds.load_feeds()` where
    each item is a tuple like (key, title, url, publication_id, issn).

    Returns the number of feeds successfully upserted.
    """
    # Delegate to the maintenance implementation to avoid duplicating logic
    try:
        from .maintenance import sync_publications_from_feeds as _sync

        return _sync(conn, feeds_list)
    except Exception:
        logger.exception(
            "Falling back to local sync_publications_from_feeds due to import error"
        )
        if not feeds_list:
            return 0
        count = 0
        for item in feeds_list:
            try:
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


def upsert_news_item(
    conn: sqlite3.Connection,
    source: str,
    title: str | None,
    text: str | None,
    link: str | None,
    published: str | None = None,
    first_seen: str | None = None,
) -> int | bool:
    """Insert or update a headlines row.

    Uses UNIQUE(link, title) to avoid duplicates. Returns the inserted/updated
    row id on success, False on failure.
    """
    if not (title or link):
        return False
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # Normalize first_seen: prefer provided, else use now. Store as ISO date/time string.
    if first_seen:
        try:
            try:
                # try parsing common ISO formats
                fs_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            except Exception:
                fs_dt = parsedate_to_datetime(first_seen)
            first_seen = fs_dt.isoformat()
        except Exception:
            first_seen = now
    else:
        first_seen = now

    # Normalize published: if provided, try to parse and store ISO date (YYYY-MM-DD or full iso); else use default
    if published:
        try:
            s = str(published).strip()
            pub_dt = None
            # Try ISO first
            try:
                pub_dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                pub_dt = None
            # Try RFC/email-style parsing next
            if pub_dt is None:
                try:
                    pub_dt = parsedate_to_datetime(s)
                except Exception:
                    pub_dt = None
            # Try several common human-readable formats (e.g. 'Sep 04, 2025')
            if pub_dt is None:
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                    "%b %d, %Y",
                    "%B %d, %Y",
                    "%d %b %Y",
                ):
                    try:
                        pub_dt = datetime.strptime(s, fmt)
                        # make timezone-aware as UTC for consistency
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        break
                    except Exception:
                        continue
            # If parsed, store ISO; otherwise keep original string
            if pub_dt is not None:
                published = pub_dt.isoformat()
            else:
                published = published
        except Exception:
            # leave as-is if parsing fails
            published = published
    else:
        published = config.DEFAULT_MISSING_DATE
    try:
        cur.execute(
            """
            INSERT INTO headlines (source, title, text, link, first_seen, published)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(link, title) DO UPDATE SET
                text = COALESCE(excluded.text, headlines.text),
                published = COALESCE(excluded.published, headlines.published)
            """,
            (source, title, text, link, first_seen, published),
        )
        conn.commit()
        cur.execute(
            "SELECT id FROM headlines WHERE link = ? AND title = ? LIMIT 1",
            (link, title),
        )
        row = cur.fetchone()
        return int(row[0]) if row and isinstance(row[0], int) else False
    except Exception:
        logger.exception(
            "Failed to upsert news_item source=%s title=%s link=%s", source, title, link
        )
        return False


def save_headlines(conn: sqlite3.Connection, source: str, items: list[dict]) -> int:
    """Save multiple headline items from a site into the database.

    Returns the number of successfully upserted items.
    """
    if not items:
        return 0
    count = 0
    for it in items:
        try:
            title = it.get("title")
            link = it.get("link")
            text = it.get("summary") or it.get("text") or None
            published = it.get("published")
            res = upsert_news_item(conn, source, title, text, link, published=published)
            if res:
                count += 1
        except Exception:
            logger.exception(
                "Failed to save headline for source=%s item=%s", source, it
            )
    logger.info("Saved %d/%d headlines for source=%s", count, len(items), source)
    return count


# Backwards-compatible name: save_news_items -> save_headlines
def save_news_items(conn: sqlite3.Connection, source: str, items: list[dict]) -> int:
    return save_headlines(conn, source, items)


def fetch_latest_journal_works(
    conn: sqlite3.Connection,
    feeds,
    per_journal: int = 30,
    timeout: int = 10,
    delay: float = 0.05,
):
    import requests, time, os

    cur = conn.cursor()
    session = requests.Session()
    # allow callers to pass a timeout; otherwise use config defaults (connect, read)
    config = None  # initialize to avoid possibly-unbound warning
    try:
        from ednews import config

        connect_timeout = getattr(config, "CROSSREF_CONNECT_TIMEOUT", 5)
        read_timeout = getattr(config, "CROSSREF_TIMEOUT", 30)
        default_retries = getattr(config, "CROSSREF_RETRIES", 3)
        backoff = getattr(config, "CROSSREF_BACKOFF", 0.3)
        status_forcelist = getattr(
            config, "CROSSREF_STATUS_FORCELIST", [429, 500, 502, 503, 504]
        )
    except Exception:
        # fallback values
        connect_timeout = 5
        read_timeout = 30
        default_retries = 3
        backoff = 0.3
        status_forcelist = [429, 500, 502, 503, 504]
    # We'll perform an explicit retry loop below so tests can monkeypatch
    # `requests.Session.get` and exercise retry handling.
    attempts = max(1, int(default_retries) + 1)
    inserted = 0
    skipped = 0
    logger.info(
        "Fetching latest journal works for %s feeds",
        len(feeds) if hasattr(feeds, "__len__") else "unknown",
    )
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
            # build headers and params (preserve existing mailto behaviour)
            ua = None
            try:
                ua = getattr(config, "USER_AGENT", None)
            except Exception:
                ua = None
            headers = {
                "User-Agent": ua or "ed-news-fetcher/1.0",
                "Accept": "application/json",
            }
            mailto = os.environ.get("CROSSREF_MAILTO", "your_email@example.com")
            url = f"https://api.crossref.org/journals/{issn}/works"
            params = {
                "sort": "created",
                "order": "desc",
                "filter": "type:journal-article",
                "rows": min(per_journal, 100),
                "mailto": mailto,
            }
            # allow callers' simple `timeout` param for compatibility; prefer tuple (connect, read)
            used_timeout = (
                connect_timeout,
                timeout if timeout and timeout > 0 else read_timeout,
            )
            # perform GET with an explicit retry loop
            resp = None
            last_exc = None
            for attempt in range(1, attempts + 1):
                try:
                    resp = session.get(
                        url, params=params, headers=headers, timeout=used_timeout
                    )
                    # If the response status code is in the forcelist, raise to trigger retry
                    if resp.status_code in status_forcelist:
                        last_exc = requests.HTTPError(f"status={resp.status_code}")
                        raise last_exc
                    resp.raise_for_status()
                    break
                except (
                    requests.exceptions.ReadTimeout,
                    requests.exceptions.ConnectionError,
                ) as e:
                    last_exc = e
                    logger.warning(
                        "Request attempt %d/%d failed for ISSN=%s: %s",
                        attempt,
                        attempts,
                        issn,
                        e,
                    )
                except requests.HTTPError as e:
                    # treat certain status codes as retryable
                    last_exc = e
                    code = (
                        getattr(e.response, "status_code", None)
                        if hasattr(e, "response")
                        else None
                    )
                    if code in status_forcelist:
                        logger.warning(
                            "HTTP %s on attempt %d/%d for ISSN=%s: will retry",
                            code,
                            attempt,
                            attempts,
                            issn,
                        )
                    else:
                        # non-retryable HTTP error — re-raise to be caught by outer except
                        raise

                # backoff between attempts (exponential)
                if attempt < attempts:
                    sleep_for = backoff * (2 ** (attempt - 1))
                    # small jitter
                    sleep_for = sleep_for + (0.1 * backoff)
                    time.sleep(sleep_for)

            if resp is None:
                # all attempts failed
                raise (
                    last_exc
                    if last_exc is not None
                    else Exception("Failed to retrieve Crossref data")
                )
            data = resp.json()
            data = resp.json()
            items = data.get("message", {}).get("items", []) or []
            for it in items[:per_journal]:
                doi = (it.get("DOI") or "").strip()
                if not doi:
                    continue
                norm = None
                # simple normalization
                if doi:
                    norm = doi
                if not norm:
                    continue
                try:
                    # If the DOI already exists, skip to avoid re-processing.
                    if article_exists(conn, norm):
                        skipped += 1
                        continue

                    # Attempt to enrich the article with Crossref metadata before inserting
                    try:
                        from ednews.crossref import fetch_crossref_metadata

                        cr = fetch_crossref_metadata(norm)
                    except Exception:
                        cr = None

                    # Use Crossref-provided authors/abstract/published when available,
                    # otherwise fall back to the values returned in the journal works list.
                    authors_val = (
                        cr.get("authors") if cr and cr.get("authors") else None
                    )
                    abstract_val = (
                        cr.get("abstract")
                        if cr and cr.get("abstract")
                        else it.get("abstract")
                    )
                    published_val = (
                        cr.get("published") if cr and cr.get("published") else None
                    )

                    aid = upsert_article(
                        conn,
                        norm,
                        title=it.get("title"),
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

                    # If we fetched raw Crossref data, store it (and re-apply authors/abstract/published
                    # defensively via update_article_crossref which uses COALESCE so it won't clobber existing values
                    # with None).
                    if cr and cr.get("raw"):
                        try:
                            update_article_crossref(
                                conn,
                                norm,
                                authors=authors_val,
                                abstract=abstract_val,
                                raw=cr.get("raw"),
                                published=published_val,
                            )
                        except Exception:
                            logger.debug(
                                "Failed to update crossref data for doi=%s after upsert",
                                norm,
                            )
                except Exception:
                    logger.exception(
                        "Failed to upsert article doi=%s from journal %s", doi, issn
                    )
            conn.commit()
        except Exception:
            logger.exception("Failed to fetch works for ISSN=%s (feed=%s)", issn, key)
    logger.info("ISSN lookup summary: inserted=%d skipped=%d", inserted, skipped)
    return inserted
