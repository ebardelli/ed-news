import sqlite3
from datetime import datetime, timezone
import logging

logger = logging.getLogger("ednews.db")


def get_connection(path: str | None = None):
    if path:
        return sqlite3.connect(path)
    return sqlite3.connect()


def init_db(conn: sqlite3.Connection):
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
    conn.commit()
    logger.debug("initialized database")


def upsert_article(conn, doi: str, title: str | None, authors: str | None, abstract: str | None, feed_id: str | None = None, publication_id: str | None = None, issn: str | None = None, fetched_at: str | None = None, published: str | None = None):
    if not doi:
        return False
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    used_fetched_at = fetched_at or now
    used_published = published
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
        return aid
    except Exception:
        try:
            cur.execute(
                """
                INSERT OR REPLACE INTO articles (id, doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, fetched_at, published)
                VALUES (
                    (SELECT id FROM articles WHERE doi = ?), ?, ?, ?, ?, COALESCE(?, (SELECT crossref_xml FROM articles WHERE doi = ?)), COALESCE(?, (SELECT feed_id FROM articles WHERE doi = ?)), COALESCE((SELECT publication_id FROM articles WHERE doi = ?), ?), COALESCE((SELECT issn FROM articles WHERE doi = ?), ?), ?, COALESCE(?, (SELECT published FROM articles WHERE doi = ?))
                )
                """,
                (doi, doi, title, authors, abstract, None, doi, feed_id, doi, publication_id, doi, issn, used_fetched_at, used_published, doi),
            )
            conn.commit()
            cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
            row = cur.fetchone()
            aid = row[0] if row and row[0] else None
            return aid
        except Exception:
            return False


def ensure_article_row(conn, doi: str, title: str | None = None, authors: str | None = None, abstract: str | None = None, feed_id: str | None = None, publication_id: str | None = None, issn: str | None = None) -> int | None:
    cur = conn.cursor()
    if not doi:
        logging.getLogger("ednews.db").debug("ensure_article_row called without doi; skipping")
        return None
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
        return None


def enrich_articles_from_crossref(conn, fetcher, batch_size: int = 20, delay: float = 0.1):
    cur = conn.cursor()
    cur.execute("SELECT articles.doi FROM articles join items on items.doi = articles.doi WHERE crossref_xml IS NULL OR crossref_xml = '' ORDER BY COALESCE(items.published, items.fetched_at, articles.fetched_at) DESC LIMIT ?", (batch_size,))
    rows = cur.fetchall()
    updated = 0
    for r in rows:
        doi = r[0]
        if not doi:
            continue
        try:
            cr = fetcher(doi)
            if not cr:
                continue
            authors = cr.get("authors")
            abstract = cr.get("abstract")
            raw = cr.get("raw")
            cur.execute(
                "UPDATE articles SET authors = COALESCE(?, authors), abstract = COALESCE(?, abstract), crossref_xml = ? WHERE doi = ?",
                (authors, abstract, raw, doi),
            )
            conn.commit()
            updated += 1
        except Exception:
            pass
    return updated


def create_combined_view(conn: sqlite3.Connection):
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


def fetch_latest_journal_works(conn: sqlite3.Connection, feeds, per_journal: int = 30, timeout: int = 10, delay: float = 0.05):
    import requests, time, os
    cur = conn.cursor()
    session = requests.Session()
    inserted = 0
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
                    aid = upsert_article(conn, norm, title=it.get('title'), authors=None, abstract=it.get('abstract'), feed_id=key, publication_id=issn, issn=issn)
                    if aid:
                        inserted += 1
                except Exception:
                    pass
            conn.commit()
        except Exception:
            pass
    return inserted
