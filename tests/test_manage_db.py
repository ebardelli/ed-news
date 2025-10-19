import sqlite3
import json
from types import SimpleNamespace
import pytest
from datetime import datetime, timezone, timedelta

from ednews.db import manage_db


def test_vacuum_db_runs_successfully(tmp_path):
    # create an on-disk sqlite file and initialize schema
    p = tmp_path / "ednews.db"
    conn = sqlite3.connect(str(p))
    manage_db.init_db(conn)
    # insert a simple row to ensure file has content
    cur = conn.cursor()
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "p1", "Feed", "1234-5678"))
    conn.commit()
    # vacuum should succeed and return True
    res = manage_db.vacuum_db(conn)
    conn.close()
    assert res is True


def make_response(status_code=200, json_obj=None):
    class Resp:
        def __init__(self, status_code, json_obj):
            self.status_code = status_code
            self._json = json_obj or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP {self.status_code}")

        def json(self):
            return self._json

    return Resp(status_code, json_obj)


def test_fetch_latest_journal_works_retries_and_inserts(monkeypatch):
    # simulate a Crossref works response
    sample_items = [
        {"DOI": "10.1000/xyz", "title": "Test Article", "abstract": "Abs"}
    ]
    sample_json = {"message": {"items": sample_items}}

    calls = {"count": 0}

    def fake_get(url, params=None, headers=None, timeout=None):
        calls["count"] += 1
        # first two calls: simulate retryable network timeout
        if calls["count"] < 3:
            import requests

            raise requests.exceptions.ReadTimeout("timeout")
        return make_response(status_code=200, json_obj=sample_json)

    # monkeypatch requests.Session.get used inside the function
    import requests

    original_Session = requests.Session

    class DummySession:
        def __init__(self):
            pass

        def get(self, *args, **kwargs):
            return fake_get(*args, **kwargs)

    monkeypatch.setattr(requests, "Session", lambda: DummySession())

    # prepare an in-memory DB and feeds list containing an item with an ISSN
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    feeds = [("feed-key", "Title", "http://example.com", "pubid", "1234-5678")]

    # run with small per_journal so it finishes quickly
    inserted = manage_db.fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1, delay=0)
    # cleanup
    conn.close()
    # ensure the fake_get retried before success
    assert calls["count"] >= 3
    # function should report inserted >= 1
    assert inserted >= 1


def test_cleanup_empty_articles_removes_rows():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # empty article (should be deleted)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/empty', None, None, now))
    # article with title (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/has', 'T', None, now))
    # article with abstract (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/has2', None, 'abs', now))
    conn.commit()
    deleted = manage_db.cleanup_empty_articles(conn)
    assert deleted == 1
    cur.execute('SELECT doi FROM articles')
    rows = {r[0] for r in cur.fetchall()}
    assert '10.0/empty' not in rows
    assert '10.0/has' in rows and '10.0/has2' in rows
    conn.close()


def test_cleanup_empty_articles_respects_age():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    recent_ts = datetime.now(timezone.utc).isoformat()
    # old empty article (should be deleted when older_than_days=1)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/old', None, None, old_ts))
    # recent empty article (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/new', None, None, recent_ts))
    conn.commit()
    deleted = manage_db.cleanup_empty_articles(conn, older_than_days=1)
    assert deleted == 1
    cur.execute('SELECT doi FROM articles')
    rows = {r[0] for r in cur.fetchall()}
    assert '10.0/old' not in rows
    assert '10.0/new' in rows
    conn.close()
