import sqlite3
from ednews.db import maintenance


def _init_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    # Minimal schema pieces needed for this test
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        feed_id TEXT,
        doi TEXT,
        title TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        doi TEXT UNIQUE,
        title TEXT,
        authors TEXT,
        abstract TEXT,
        crossref_xml TEXT,
        feed_id TEXT,
        publication_id TEXT,
        issn TEXT,
        fetched_at TEXT,
        published TEXT
    )
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS publications (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        feed_id TEXT,
        publication_id TEXT
    )
    """)
    conn.commit()


def test_sync_creates_articles_for_item_dois(tmp_path):
    conn = sqlite3.connect(":memory:")
    _init_schema(conn)
    cur = conn.cursor()
    # Insert two items with DOIs; no article rows yet
    cur.execute("INSERT INTO items (feed_id, doi, title) VALUES (?, ?, ?)", ("feed-a", "10.1000/xyz123", "Test Article A"))
    cur.execute("INSERT INTO items (feed_id, doi, title) VALUES (?, ?, ?)", ("feed-a", "10.1000/abc456", "Test Article B"))
    conn.commit()

    results = maintenance.sync_articles_from_items(conn, feed_keys=["feed-a"], dry_run=False)
    assert results["total_created"] == 2

    cur.execute("SELECT doi, title FROM articles ORDER BY doi")
    rows = cur.fetchall()
    assert len(rows) == 2
    assert ("10.1000/abc456", "Test Article B") in rows
    assert ("10.1000/xyz123", "Test Article A") in rows
