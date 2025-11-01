import sqlite3
from ednews.db import manage_db


def test_migrate_resolves_duplicate_url_hash_keep_earliest():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()

    # Create an items table WITHOUT unique constraints to simulate an older/dirty DB
    cur.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            feed_id TEXT,
            guid TEXT,
            title TEXT,
            link TEXT,
            url_hash TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()

    # Same url_hash for both rows
    url_hash = 'deadbeefdeadbeef'

    # Insert two rows with the same url_hash but different published dates and DOIs
    # Row A: earlier published date, no DOI
    cur.execute(
        "INSERT INTO items (feed_id, guid, title, link, url_hash, published, fetched_at, summary) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("f1", "g1", "Title One", "https://example.com/a", url_hash, "2020-01-01T00:00:00", "2020-01-02T00:00:00", "s1"),
    )
    # Row B: later published date, has DOI
    cur.execute(
        "INSERT INTO items (feed_id, guid, title, link, url_hash, published, fetched_at, summary, doi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("f2", "g2", "Title Two", "https://example.com/a", url_hash, "2021-02-02T00:00:00", "2021-02-03T00:00:00", "s2", "10.1000/xyz"),
    )
    conn.commit()

    # Ensure both rows inserted
    cur.execute("SELECT COUNT(1) FROM items WHERE url_hash = ?", (url_hash,))
    assert cur.fetchone()[0] == 2

    # Run migration which should detect duplicates, keep the earliest (row A), copy DOI from row B into A, and delete row B
    res = manage_db.migrate_add_items_url_hash(conn)
    # resolved_duplicates should be >= 1
    assert res.get('resolved_duplicates', 0) >= 1

    # Now only one row should remain for that url_hash
    cur.execute("SELECT id, doi, published, fetched_at FROM items WHERE url_hash = ?", (url_hash,))
    rows = cur.fetchall()
    assert len(rows) == 1
    kept_id, kept_doi, kept_pub, kept_fetched = rows[0]

    # DOI should have been copied from the deleted duplicate
    assert kept_doi == '10.1000/xyz'
    # Published date should be the earliest one
    assert kept_pub == '2020-01-01T00:00:00'
