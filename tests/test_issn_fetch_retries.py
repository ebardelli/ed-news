import sqlite3
import json
import requests
from types import SimpleNamespace

import pytest

from ednews.db import fetch_latest_journal_works, init_db


class DummyResp:
    def __init__(self, data, status=200):
        self._data = data
        self.status_code = status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"status={self.status_code}")

    def json(self):
        return self._data


def make_feeds(issn):
    # feed tuple: (key, title, url, publication_id, issn)
    return [("feedkey", "Feed Title", "http://example.com", issn, issn)]


def test_fetch_retries_then_success(monkeypatch, tmp_path):
    # simulate a ReadTimeout on first call, then a successful response
    calls = {'n': 0}

    # Patch the adapter send method so urllib3/requests retry logic is exercised
    from requests.adapters import HTTPAdapter

    original_send = getattr(HTTPAdapter, 'send', None)

    def fake_send(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        calls['n'] += 1
        if calls['n'] == 1:
            raise requests.exceptions.ReadTimeout("timed out")
        # create a real requests.Response with JSON body
        resp = requests.Response()
        resp.status_code = 200
        resp._content = json.dumps({"message": {"items": [{"DOI": "10.1234/example.doi", "title": "Test Title"}]}}).encode('utf-8')
        return resp

    monkeypatch.setattr('requests.adapters.HTTPAdapter.send', fake_send, raising=True)

    conn = sqlite3.connect(':memory:')
    init_db(conn)
    feeds = [("feedkey", "Feed Title", "http://example.com", "pubid", "0000-0000")]
    inserted = fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1)
    # ensure adapter send was called at least twice (one timeout, one success)
    assert calls['n'] >= 2
    # also inspect articles table to see if a row exists
    cur = conn.cursor()
    cur.execute('SELECT doi, title FROM articles')
    rows = cur.fetchall()
    assert len(rows) == 1
    assert rows[0][0] == '10.1234/example.doi'
    assert inserted == 1


def test_fetch_persistent_timeout(monkeypatch):
    # simulate persistent ReadTimeouts
    def fake_send_always(self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None):
        raise requests.exceptions.ReadTimeout("timed out")

    monkeypatch.setattr('requests.adapters.HTTPAdapter.send', fake_send_always, raising=True)

    conn = sqlite3.connect(':memory:')
    init_db(conn)
    feeds = [("feedkey", "Feed Title", "http://example.com", "pubid", "0000-0000")]
    inserted = fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1)
    # persistent timeouts should not raise, and should result in zero inserts
    assert inserted == 0
