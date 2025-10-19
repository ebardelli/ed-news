import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from ednews import news
from ednews import db as eddb


class DummyResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, resp_text):
        self.resp_text = resp_text

    def get(self, url, timeout=None):
        return DummyResponse(self.resp_text)


def test_fetch_all_and_save(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "fcmat.html"
    html = fixture.read_text(encoding="utf-8")

    # prepare DB
    db_path = tmp_path / "news.db"
    conn = eddb.get_connection(str(db_path))
    eddb.init_db(conn)

    # create a dummy session that returns the fixture
    session = DummySession(html)

    # load config from project root news.json by pointing to it explicitly
    results = news.fetch_all(session=session, cfg_path=None, conn=conn)
    # results should contain the 'fcmat' key
    assert isinstance(results, dict)
    assert "fcmat" in results
    items = results["fcmat"]
    assert isinstance(items, list)
    assert len(items) >= 3

    # check DB rows
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM news_items")
    n = cur.fetchone()[0]
    assert n >= len(items)

    conn.close()
