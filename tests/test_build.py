import pytest
from ednews import build
from pathlib import Path
import sqlite3


def test_read_planet_missing(tmp_path):
    # create a fake planet.ini
    p = tmp_path / 'planet.ini'
    p.write_text('title = Example')
    res = build.read_planet(p)
    assert isinstance(res, dict)
    assert 'title' in res


def test_read_articles_empty_view(tmp_path, monkeypatch):
    # Create an empty sqlite DB
    db = tmp_path / 'ednews.db'
    conn = sqlite3.connect(str(db))
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, doi TEXT)')
    conn.commit()
    conn.close()
    res = build.read_articles(db)
    assert isinstance(res, list)
