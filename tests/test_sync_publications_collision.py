import sqlite3

from ednews.db import sync_publications_from_feeds


def test_sync_publications_handles_unique_collision():
    conn = sqlite3.connect(":memory:")
    from ednews.db.schema import init_db

    init_db(conn)
    cur = conn.cursor()
    # Insert two existing publications for two feeds
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("a", "pid1", "Title A", "1111-1111"))
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("b", "pid2", "Title B", "2222-2222"))
    conn.commit()

    # Now a feeds list that attempts to change feed 'a' to use pid2/2222-2222
    feeds = [
        ("a", "New Title A", "http://a", "pid2", "2222-2222"),
    ]

    # This should not raise an IntegrityError; final state should have feed 'a'
    # mapped to pid2/2222-2222 and feed 'b' removed or adjusted so constraints hold.
    count = sync_publications_from_feeds(conn, feeds)
    assert count == 1
    cur.execute("SELECT feed_id, publication_id, issn FROM publications ORDER BY feed_id")
    rows = cur.fetchall()
    # Ensure there is at most one row with publication_id pid2/2222-2222 and feed a points to it
    assert any(r[0] == 'a' and r[1] == 'pid2' and r[2] == '2222-2222' for r in rows)
