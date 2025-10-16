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
