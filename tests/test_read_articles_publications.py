from pathlib import Path
import sqlite3
import pytest
from datetime import datetime, timedelta

from ednews import build


def make_db(path: Path):
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
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
    conn.commit()
    return conn


def test_read_articles_publications_latest_dates(tmp_path):
    db = tmp_path / "test.db"
    conn = make_db(db)
    cur = conn.cursor()

    # feed A: latest date = today (2 articles on that date)
    # feed B: latest date = yesterday (1 article)
    # feed C: latest date = 10 days ago (1 article)
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)
    older = today - timedelta(days=10)

    articles = [
        ("doi-a1", "A1", "", "Feed A", "content", f"{today.isoformat()}T10:00:00", "auth"),
        ("doi-a2", "A2", "", "Feed A", "content", f"{today.isoformat()}T12:00:00", "auth"),
        ("doi-b1", "B1", "", "Feed B", "content", f"{yesterday.isoformat()}T09:00:00", "auth"),
        ("doi-c1", "C1", "", "Feed C", "content", f"{older.isoformat()}T08:00:00", "auth"),
    ]

    cur.executemany(
        "INSERT INTO combined_articles (doi, title, link, feed_title, content, published, authors) VALUES (?, ?, ?, ?, ?, ?, ?)",
        articles,
    )
    conn.commit()
    conn.close()

    # Request top 2 publications -> should be Feed A and Feed B
    res = build.read_articles(db, publications=2)
    feed_titles = {r["feed_title"] for r in res}

    assert "Feed A" in feed_titles
    assert "Feed B" in feed_titles
    assert "Feed C" not in feed_titles

    # Feed A should include both articles (both on the latest date for Feed A)
    a_articles = [r for r in res if r["feed_title"] == "Feed A"]
    assert len(a_articles) == 2

    # Feed B should include its one article
    b_articles = [r for r in res if r["feed_title"] == "Feed B"]
    assert len(b_articles) == 1
