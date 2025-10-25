import sqlite3
from ednews import feeds, db as eddb


def test_save_entries_filters_empty():
    conn = sqlite3.connect(':memory:')
    eddb.init_db(conn)

    entries = [
        {"guid": "g1", "title": "", "link": "", "published": None, "summary": ""},
        {"guid": "g2", "title": None, "link": None, "published": None, "summary": None},
        {"guid": "g_ed", "title": "Editorial Board", "link": "http://example.com/ed", "published": "2025-10-19", "summary": ""},
        {"guid": "g3", "title": "Real Title", "link": "http://example.com/a", "published": "2025-10-19", "summary": ""},
    ]

    inserted = feeds.save_entries(conn, feed_id="f1", feed_title="F1", entries=entries)
    # Only the real entry should be inserted
    assert inserted == 1

    cur = conn.cursor()
    cur.execute("SELECT guid, title, link FROM items ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == 1
    guid, title, link = rows[0]
    assert guid == 'g3'
    assert title == 'Real Title'
    assert link == 'http://example.com/a'

    conn.close()
