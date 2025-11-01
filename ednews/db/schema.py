"""Schema and view creation helpers (extracted from manage_db).
"""
from datetime import datetime, timezone
import logging
import sqlite3

logger = logging.getLogger("ednews.manage_db.schema")

def init_db(conn: sqlite3.Connection):
    """Initialize the database schema and create required tables/views.
    """
    logger.info("Initializing database schema")
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
            url_hash TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT,
            UNIQUE(url_hash),
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
        from . import migrations

        try:
            migrations.create_combined_view(conn)
        except Exception:
            logger.exception("Failed to create combined_articles view during init_db")
    except Exception:
        logger.debug("migrations module not available for create_combined_view")
    conn.commit()
    logger.debug("initialized database")


def create_combined_view(conn: sqlite3.Connection):
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
