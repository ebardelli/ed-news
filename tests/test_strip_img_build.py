import sqlite3
from pathlib import Path

from ednews import build


def test_strip_img_from_articles_and_headlines(tmp_path: Path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Create a minimal combined_articles table expected by read_articles
    cur.execute(
        """
        CREATE TABLE combined_articles (
            doi TEXT,
            title TEXT,
            link TEXT,
            feed_title TEXT,
            content TEXT,
            published TEXT,
            authors TEXT
        )
        """
    )

    img_content = '<p>Intro <img src="https://example.com/a.jpg" alt="x"> more text</p>'
    cur.execute(
        "INSERT INTO combined_articles (doi, title, link, feed_title, content, published, authors) VALUES (?,?,?,?,?,?,?)",
        ("doi1", "T", "http://t", "Feed", img_content, "2025-11-01T12:00:00Z", "A"),
    )

    # Create a minimal headlines table expected by read_news_headlines
    cur.execute(
        """
        CREATE TABLE headlines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT,
            text TEXT,
            published TEXT,
            first_seen TEXT
        )
        """
    )

    img_text = 'Headline with <img src="https://example.com/h.jpg"> embedded'
    cur.execute(
        "INSERT INTO headlines (title, link, text, published, first_seen) VALUES (?,?,?,?,?)",
        ("H", "http://h", img_text, "2025-11-01T13:00:00Z", None),
    )

    conn.commit()
    conn.close()

    # Call the read helpers and assert <img> is removed
    articles = build.read_articles(db_path, limit=10)
    assert articles, "expected at least one article"
    assert "<img" not in (articles[0].get("content") or "").lower()

    headlines = build.read_news_headlines(db_path, limit=10)
    assert headlines, "expected at least one headline"
    assert "<img" not in (headlines[0].get("text") or "").lower()
