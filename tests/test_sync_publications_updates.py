import sqlite3

from ednews.db import sync_publications_from_feeds


def test_sync_publications_updates_existing():
    conn = sqlite3.connect(":memory:")
    # initialize schema
    from ednews.db.schema import init_db

    init_db(conn)
    cur = conn.cursor()
    # Insert an existing publication row
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "oldpid", "Old Title", "1234-5678"))
    conn.commit()

    # New feeds list contains updated metadata for feed f1
    feeds = [
        ("f1", "New Title", "http://example.com/feed", "newpid", "8765-4321"),
    ]

    count = sync_publications_from_feeds(conn, feeds)
    assert count == 1
    cur.execute("SELECT publication_id, feed_title, issn FROM publications WHERE feed_id = ?", ("f1",))
    row = cur.fetchone()
    assert row[0] == "newpid"
    assert row[1] == "New Title"
    assert row[2] == "8765-4321"
