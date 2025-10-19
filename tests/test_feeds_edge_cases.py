import sqlite3
from ednews import feeds, db as eddb


def test_entry_with_content_block_counts():
    # content is in content blocks
    e = {"guid": "c1", "title": "", "link": "", "content": [{"value": "Full text here"}], "summary": ""}
    assert feeds.entry_has_content(e)


def test_entry_with_html_summary_counts():
    e = {"guid": "s1", "title": "", "link": "", "summary": "<p>This is <strong>HTML</strong> summary.</p>"}
    # extract_abstract_from_entry should strip html but produce text, so entry_has_content should be True
    assert feeds.entry_has_content(e)


def test_entry_with_only_link_counts_and_is_saved():
    conn = sqlite3.connect(':memory:')
    eddb.init_db(conn)
    e = {"guid": "l1", "title": "", "link": "http://example.com/onlylink", "summary": "", "published": None}
    inserted = feeds.save_entries(conn, feed_id="f2", feed_title="F2", entries=[e])
    assert inserted == 1
    cur = conn.cursor()
    cur.execute("SELECT link FROM items WHERE guid = ?", ('l1',))
    row = cur.fetchone()
    assert row and row[0] == 'http://example.com/onlylink'
    conn.close()


def test_entry_with_no_content_is_skipped():
    conn = sqlite3.connect(':memory:')
    eddb.init_db(conn)
    e = {"guid": "empty1", "title": "", "link": "", "summary": ""}
    inserted = feeds.save_entries(conn, feed_id="f3", feed_title="F3", entries=[e])
    assert inserted == 0
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM items")
    count = cur.fetchone()[0]
    assert count == 0
    conn.close()
