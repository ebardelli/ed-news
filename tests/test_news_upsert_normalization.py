import sqlite3
from pathlib import Path
from datetime import datetime

from ednews import db as eddb


def get_published_from_db(conn, title):
    cur = conn.cursor()
    cur.execute("SELECT published FROM news_items WHERE title = ? LIMIT 1", (title,))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def test_upsert_news_item_normalizes_dates(tmp_path):
    db_path = tmp_path / "news_norm.db"
    conn = sqlite3.connect(str(db_path))
    eddb.init_db(conn)

    examples = [
        ("fmt1", "Sep 04, 2025"),
        ("iso", "2025-09-04T12:00:00Z"),
        ("fmt2", "4 Sep 2025"),
    ]

    for title, pub in examples:
        res = eddb.upsert_news_item(conn, "testsrc", title, "summary", "https://example.org/" + title, published=pub)
        assert res, f"upsert failed for {title}"

    # read back and ensure each published string is a valid ISO timestamp
    for title, _ in examples:
        pub_stored = get_published_from_db(conn, title)
        assert pub_stored is not None, f"no stored published for {title}"
        # datetime.fromisoformat accepts offsets; replace trailing Z with +00:00 for compatibility
        s = str(pub_stored).replace('Z', '+00:00')
        try:
            dt = datetime.fromisoformat(s)
        except Exception:
            # also accept a YYYY-MM-DD date substring
            assert False, f"stored published for {title} is not ISO-parseable: {pub_stored}"
        assert isinstance(dt, datetime)

    conn.close()
