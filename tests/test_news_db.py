import sqlite3
from ednews import db
from datetime import datetime


def test_upsert_and_save_news_item(tmp_path):
    # create an on-disk sqlite DB to exercise schema
    db_path = tmp_path / "test_news.db"
    conn = sqlite3.connect(str(db_path))
    db.init_db(conn)

    # upsert a single item
    res = db.upsert_news_item(conn, "fcmat", "Test Title", "Some text", "https://example.com/1", published="2025-10-01T12:00:00Z")
    assert res is not False

    # upsert same item again with updated text
    res2 = db.upsert_news_item(conn, "fcmat", "Test Title", "Updated text", "https://example.com/1", published="2025-10-01T12:00:00Z")
    assert res2 is not False

    # save multiple items
    items = [
        {"title": "A", "link": "https://example.com/a", "summary": "a"},
        {"title": "B", "link": "https://example.com/b", "summary": "b"},
    ]
    cnt = db.save_news_items(conn, "fcmat", items)
    assert cnt == 2

    # ensure rows present
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM news_items")
    n = cur.fetchone()[0]
    assert n >= 3
    conn.close()
