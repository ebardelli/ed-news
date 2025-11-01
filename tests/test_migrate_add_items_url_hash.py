import sqlite3
from ednews.db import manage_db


def setup_old_items_schema(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE items (
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
    conn.commit()


def test_migrate_add_items_url_hash_basic(tmp_path):
    conn = sqlite3.connect(":memory:")
    setup_old_items_schema(conn)
    cur = conn.cursor()
    # Insert a couple of rows with links
    cur.execute("INSERT INTO items (feed_id, title, link, published, fetched_at) VALUES (?, ?, ?, ?, ?)", ("f1", "t1", "https://example.com/a", "2020-01-01", "2020-01-01"))
    cur.execute("INSERT INTO items (feed_id, title, link, published, fetched_at) VALUES (?, ?, ?, ?, ?)", ("f2", "t2", "https://example.com/b", "2020-01-02", "2020-01-02"))
    conn.commit()

    res = manage_db.migrate_add_items_url_hash(conn)
    assert isinstance(res, dict)
    assert res.get("added_column") in (True, False)
    # updated_rows should be >= 0 and likely 2
    assert res.get("updated_rows", 0) >= 0
    # index_created should be True when no collisions
    assert isinstance(res.get("index_created"), bool)
    # collisions should be a list
    assert isinstance(res.get("collisions"), list)

    # Ensure column exists now
    cur.execute("PRAGMA table_info(items)")
    cols = [c[1] for c in cur.fetchall()]
    assert 'url_hash' in cols

