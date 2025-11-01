import sqlite3
from ednews import feeds
from ednews.db import manage_db


def test_save_entries_dedup(tmp_path):
    conn = sqlite3.connect(":memory:")
    # Initialize DB with current schema (init_db creates url_hash column)
    manage_db.init_db(conn)

    # Create two entries with the same link but pretend to be from different feeds
    entry1 = {"guid": "g1", "title": "Title A", "link": "https://example.com/same", "published": "2025-01-01", "summary": "s1"}
    entry2 = {"guid": "g2", "title": "Title A duplicate", "link": "https://example.com/same", "published": "2025-01-02", "summary": "s2"}

    # Save first feed entries
    inserted1 = feeds.save_entries(conn, "feed1", "Feed 1", [entry1])
    # Save second feed entries (same link)
    inserted2 = feeds.save_entries(conn, "feed2", "Feed 2", [entry2])

    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM items WHERE link = ?", ("https://example.com/same",))
    cnt = cur.fetchone()[0]

    # Only one row for that link should exist (dedup across feeds)
    assert cnt == 1
    # First call should have inserted 1, second should insert 0
    assert inserted1 == 1
    assert inserted2 == 0
