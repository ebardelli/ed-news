"""Article-related DB helpers split from ednews.db.__init__."""
import logging, sqlite3
from datetime import datetime, timezone
from .. import config

logger = logging.getLogger("ednews.db.articles")

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
    if not doi:
        return False
    try:
        from .. import config as _config
        if title and isinstance(title, str):
            tnorm = title.strip().lower()
            filters = getattr(_config, "TITLE_FILTERS", []) or []
            if any(tnorm == f.strip().lower() for f in filters):
                logger.info("Skipping upsert for filtered title=%s doi=%s", title, doi)
                return False
    except Exception:
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
    def _sanitize(val):
        if val is None:
            return None
        if isinstance(val, (bytes, bytearray)):
            try:
                return val.decode("utf-8")
            except Exception:
                return val.decode("utf-8", errors="replace")
        if isinstance(val, (list, tuple, set)):
            try:
                return ", ".join(str(x) for x in val)
            except Exception:
                return str(val)
        if not isinstance(val, str):
            try:
                return str(val)
            except Exception:
                return None
        return val
    doi = _sanitize(doi); title = _sanitize(title); authors = _sanitize(authors); abstract = _sanitize(abstract)
    feed_id = _sanitize(feed_id); publication_id = _sanitize(publication_id); issn = _sanitize(issn)
    used_fetched_at = _sanitize(used_fetched_at); used_published = _sanitize(used_published)
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
        return row[0] if row and row[0] else False
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
                (doi, doi, title, authors, abstract, None, doi, feed_id, doi, doi, publication_id, doi, issn, used_fetched_at, used_published, doi),
            )
            conn.commit()
            cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
            row = cur.fetchone()
            return row[0] if row and row[0] else False
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
    cur = conn.cursor()
    if not doi:
        logger.debug("ensure_article_row called without doi; skipping")
        return None
    try:
        from .. import config as _config
        if title and isinstance(title, str):
            tnorm = title.strip().lower()
            filters = getattr(_config, "TITLE_FILTERS", []) or []
            if any(tnorm == f.strip().lower() for f in filters):
                logger.info("Skipping ensure_article_row for filtered title=%s doi=%s", title, doi)
                return None
    except Exception:
        pass
    try:
        cur.execute(
            "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, feed_id, publication_id, issn, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (doi, title, authors, abstract, feed_id, publication_id, issn, datetime.now(timezone.utc).isoformat()),
        )
        conn.commit()
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        logger.exception("ensure_article_row failed for doi=%s", doi)
        return None

def article_exists(conn: sqlite3.Connection, doi: str) -> bool:
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
    if not title:
        return None
    try:
        cur = conn.cursor()
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
    cur = conn.cursor()
    logger.info("Enriching up to %s articles from Crossref", batch_size)
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
            authors = cr.get("authors"); abstract = cr.get("abstract"); raw = cr.get("raw")
            cur.execute(
                "UPDATE articles SET authors = COALESCE(?, authors), abstract = COALESCE(?, abstract), crossref_xml = ? WHERE doi = ?",
                (authors, abstract, raw, doi),
            )
            conn.commit()
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
    cur = conn.cursor()
    cur.execute(
        "SELECT articles.doi FROM articles LEFT JOIN items on items.doi = articles.doi WHERE articles.doi IS NOT NULL AND (articles.crossref_xml IS NULL OR articles.crossref_xml = '') ORDER BY COALESCE(items.published, items.fetched_at, articles.fetched_at) DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )
    rows = cur.fetchall()
    dois = [r[0] for r in rows if r and r[0]]
    logger.debug("get_missing_crossref_dois found %d DOIs (limit=%s offset=%s)", len(dois), limit, offset)
    return dois

def update_article_crossref(
    conn: sqlite3.Connection,
    doi: str,
    authors: str | None = None,
    abstract: str | None = None,
    raw: str | None = None,
    published: str | None = None,
) -> bool:
    if not doi:
        logger.debug("update_article_crossref called without doi; skipping")
        return False
    try:
        cur = conn.cursor()
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

__all__ = [
    "upsert_article",
    "ensure_article_row",
    "article_exists",
    "get_article_metadata",
    "get_article_by_title",
    "enrich_articles_from_crossref",
    "get_missing_crossref_dois",
    "update_article_crossref",
]
