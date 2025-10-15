"""Database helpers for ed-news.

This module provides convenience functions to open SQLite connections,
initialize schema, and perform common upsert and query operations on the
`items`, `articles`, and `publications` tables used by the project.
"""

import sqlite3
from datetime import datetime, timezone
import logging

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
        return sqlite3.connect()
    except Exception:
        logger.exception("Failed to open SQLite connection (path=%s)", path)
        raise


def init_db(conn: sqlite3.Connection):
    """Initialize the database schema and create required tables/views.

    This will create the `items`, `articles`, and `publications` tables
    if they do not exist and attempt to create the `combined_articles`
    view used elsewhere in the project.
    """
    logger.info("Initializing database schema")
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
        """
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
        """
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
        """
    )

    try:
        # ensure the combined_articles view exists immediately after initializing schema
        create_combined_view(conn)
    except Exception:
        logger.exception("Failed to create combined_articles view during init_db")
    conn.commit()
    logger.debug("initialized database")


def upsert_article(conn, doi: str, title: str | None, authors: str | None, abstract: str | None, feed_id: str | None = None, publication_id: str | None = None, issn: str | None = None, fetched_at: str | None = None, published: str | None = None):
    """Insert or update an article row by DOI.

    Sanitizes inputs and attempts an INSERT with ON CONFLICT DO UPDATE. If
    that fails, a fallback INSERT OR REPLACE is attempted. Returns the
    article id on success or False/None on failure.
    """
    if not doi:
        return False
    logger.debug("Upserting article doi=%s feed_id=%s publication_id=%s", doi, feed_id, publication_id)
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
                return val.decode('utf-8')
            except Exception:
                return val.decode('utf-8', errors='replace')
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
            (doi, title, authors, abstract, feed_id, publication_id, issn, used_fetched_at, used_published),
        )
        conn.commit()
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        aid = row[0] if row and row[0] else None
        logger.debug("Upsert successful for doi=%s id=%s", doi, aid)
        return aid
    except Exception:
        logger.exception("Upsert failed, attempting fallback INSERT OR REPLACE for doi=%s", doi)
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


def ensure_article_row(conn, doi: str, title: str | None = None, authors: str | None = None, abstract: str | None = None, feed_id: str | None = None, publication_id: str | None = None, issn: str | None = None) -> int | None:
    """Ensure an article row exists for the given DOI.

    Performs an INSERT OR IGNORE and returns the article id (or None).
    """
    cur = conn.cursor()
    if not doi:
        logger.debug("ensure_article_row called without doi; skipping")
        return None
    try:
        cur.execute(
            "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, feed_id, publication_id, issn, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (doi, title, authors, abstract, feed_id, publication_id, issn, datetime.now(timezone.utc).isoformat()),
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


def enrich_articles_from_crossref(conn, fetcher, batch_size: int = 20, delay: float = 0.1, return_ids: bool = False):
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


def get_missing_crossref_dois(conn: sqlite3.Connection, limit: int = 100, offset: int = 0) -> list:
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
    logger.debug("get_missing_crossref_dois found %d DOIs (limit=%s offset=%s)", len(dois), limit, offset)
    return dois


def update_article_crossref(conn: sqlite3.Connection, doi: str, authors: str | None = None, abstract: str | None = None, raw: str | None = None, published: str | None = None) -> bool:
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
        updated = cur.rowcount if hasattr(cur, 'rowcount') else None
        logger.debug("update_article_crossref doi=%s updated_rows=%s", doi, updated)
        return (updated is None) or (updated > 0)
    except Exception:
        logger.exception("Failed to update article crossref data for doi=%s", doi)
        return False


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
        """
    )
    conn.commit()
    logger.debug("combined_articles view created")


def upsert_publication(conn, feed_id: str | None, publication_id: str | None, feed_title: str | None, issn: str | None):
    """Insert or update a publications row.

    Primary key is (publication_id, issn) per schema. If publication_id is missing,
    fall back to using feed_id as an identifier when available.
    """
    if not publication_id and not feed_id:
        logger.debug("upsert_publication called without publication_id or feed_id; skipping")
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
                (feed_id, publication_id or feed_id, feed_title, issn or ''),
            )
        conn.commit()
        logger.debug("upsert_publication succeeded for feed_id=%s publication_id=%s issn=%s", feed_id, publication_id, issn)
        return True
    except Exception:
        logger.exception("upsert_publication failed for feed_id=%s publication_id=%s issn=%s", feed_id, publication_id, issn)
        return False


def sync_publications_from_feeds(conn, feeds_list) -> int:
    """Synchronize the publications table from a feeds list.

    feeds_list is expected to be the output of `ednews.feeds.load_feeds()` where
    each item is a tuple like (key, title, url, publication_id, issn).

    Returns the number of feeds successfully upserted.
    """
    if not feeds_list:
        return 0
    count = 0
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
    import requests, time, os
    cur = conn.cursor()
    session = requests.Session()
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
            headers = {"User-Agent": "ed-news-fetcher/1.0", "Accept": "application/json"}
            mailto = os.environ.get("CROSSREF_MAILTO", "your_email@example.com")
            url = f"https://api.crossref.org/journals/{issn}/works"
            params = {"sort": "created", "order": "desc", "filter": "type:journal-article", "rows": min(per_journal, 100), "mailto": mailto}
            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
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

                    # If we fetched raw Crossref data, store it (and re-apply authors/abstract/published
                    # defensively via update_article_crossref which uses COALESCE so it won't clobber existing values
                    # with None).
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
