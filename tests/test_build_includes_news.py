from pathlib import Path
import sqlite3

from ednews import db as eddb
from ednews import build


def test_build_includes_news_headlines(tmp_path):
    db_path = tmp_path / "site.db"
    conn = sqlite3.connect(str(db_path))
    eddb.init_db(conn)

    # insert some news items
    eddb.upsert_news_item(conn, "fcmat", "Headline One", "excerpt one", "https://example.com/1", published="2025-10-17T12:00:00Z")
    eddb.upsert_news_item(conn, "fcmat", "Headline Two", "excerpt two", "https://example.com/2", published="2025-10-16T12:00:00Z")
    conn.close()

    # call read_news_headlines on the db path
    headlines = build.read_news_headlines(db_path, limit=5)
    assert isinstance(headlines, list)
    assert len(headlines) >= 2
    titles = [h["title"] for h in headlines]
    assert "Headline One" in titles

    # Render templates into an output dir using the DB file (monkeypatch default DB_FILE path by temporary assign)
    out_dir = tmp_path / "out"
    # call build.build but need to temporarily point DB_FILE to our db_path
    original_db = build.DB_FILE
    try:
        build.DB_FILE = db_path
        build.build(out_dir)
        idx = out_dir / "index.html"
        assert idx.exists()
        txt = idx.read_text(encoding="utf-8")
        assert "Headline One" in txt
        assert "Headline Two" in txt
    finally:
        build.DB_FILE = original_db
