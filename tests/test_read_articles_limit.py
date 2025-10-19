from pathlib import Path
import sqlite3
import pytest

from ednews import build


def make_db_from_sql(tmp_path: Path, sql_fixture: Path) -> Path:
    db_path = tmp_path / "test_limit.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    sql = sql_fixture.read_text(encoding='utf-8')
    cur.executescript(sql)
    conn.commit()
    conn.close()
    return db_path


def test_read_articles_limit_includes_same_date_extras(tmp_path):
    fixture_sql = Path(__file__).parent / "fixtures" / "read_articles_limit.sql"
    db = make_db_from_sql(tmp_path, fixture_sql)

    res = build.read_articles(db, limit=20)

    # The fixture has 18 articles on 2025-10-16 and 7 on 2025-10-15 (total 25).
    # The 20th most recent article falls on 2025-10-15, so the function should
    # include all 7 articles from that date -> total 25 returned.
    assert isinstance(res, list)
    assert len(res) == 25

    # Verify published dates are >= 2025-10-15 (i.e., included date boundary)
    dates = {r.get('raw', {}).get('published', r.get('published')) for r in res}
    # At least one entry should include the 2025-10-15 timestamp
    assert any('2025-10-15' in str(d) for d in dates)


def test_read_articles_uses_default_limit(tmp_path, monkeypatch):
    # Build a DB with many distinct-date articles and ensure default limit is applied
    fixture_sql = Path(__file__).parent / "fixtures" / "read_articles_limit.sql"
    db = make_db_from_sql(tmp_path, fixture_sql)

    # Ensure config default is the expected value
    from ednews import config
    default = config.ARTICLES_DEFAULT_LIMIT

    res = build.read_articles(db)
    assert isinstance(res, list)
    # result should be at least the default (may be expanded by same-date rule)
    assert len(res) >= default


def test_read_articles_enforces_same_date_cap(tmp_path):
    """Simulate a DB where a single date has many articles and assert the
    configured cap is enforced (limit + ARTICLES_MAX_SAME_DATE_EXTRA).
    """
    # Create an in-memory SQL script that populates combined_articles view
    # via a temporary table and inserts many rows with the same published date.
    sql = []
    sql.append("CREATE TABLE articles (doi TEXT, title TEXT, link TEXT, feed_title TEXT, content TEXT, published TEXT, authors TEXT);")
    # Insert 1,000 rows all on the same date (2025-10-15), plus a few recent ones
    for i in range(1000):
        sql.append("INSERT INTO articles (doi, title, link, feed_title, content, published, authors) VALUES ('doi-%d', 'Title %d', 'http://example/%d', 'BigFeed', 'x', '2025-10-15T12:00:00', 'Author');" % (i, i, i))
    # Add a few newer articles so the nth article date is 2025-10-15
    for i in range(5):
        sql.append("INSERT INTO articles (doi, title, link, feed_title, content, published, authors) VALUES ('new-%d', 'New %d', 'http://example/new/%d', 'Other', 'x', '2025-10-16T12:00:00', 'Author');" % (i, i, i))

    # Create a combined_articles view that selects from articles
    sql.append("CREATE VIEW combined_articles AS SELECT doi, title, link, feed_title, content, published, authors FROM articles;")

    db_path = tmp_path / "cap_test.db"
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.executescript("\n".join(sql))
    conn.commit()
    conn.close()

    # Now call read_articles with a small limit and assert the cap
    from ednews import config
    limit = 20
    res = build.read_articles(db_path, limit=limit)

    assert isinstance(res, list)
    max_allowed = limit + getattr(config, 'ARTICLES_MAX_SAME_DATE_EXTRA', 200)
    assert len(res) <= max_allowed
    # Also warn: if truncation happened, at least one returned article should be from 2025-10-15
    assert any('2025-10-15' in str(r.get('raw', {}).get('published', '')) for r in res)
