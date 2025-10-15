import sqlite3
from ednews.db import init_db, ensure_article_row, fetch_latest_journal_works
import json
import requests
from unittest import mock


def make_mock_response(items):
    class MockResp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return {"message": {"items": self._data}}

    return MockResp(items)


def test_fetch_latest_journal_works_skips_existing(tmp_path, monkeypatch):
    # prepare an in-memory sqlite connection
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    # insert an existing article with a DOI
    existing_doi = "10.1000/existingdoi"
    ensure_article_row(conn, existing_doi, title="Existing", authors="A", abstract="X", feed_id="feed1", publication_id="issn:1234", issn="1234")

    # prepare feeds list with ISSN entry
    feeds = [("feed1", "Title", "http://example.org", "pubid", "1234")]

    # mock requests.Session.get to return an item list containing the existing DOI
    mock_items = [{"DOI": existing_doi, "title": ["Existing"]}]

    def fake_get(url, params=None, headers=None, timeout=None):
        return make_mock_response(mock_items)

    monkeypatch.setattr("requests.Session.get", fake_get)

    # Run the lookup; since the DOI already exists, inserted should be 0
    inserted = fetch_latest_journal_works(conn, feeds, per_journal=10, timeout=1, delay=0)
    assert inserted == 0

    # Ensure the article still exists and there's exactly one row for that DOI
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM articles WHERE doi = ?", (existing_doi,))
    cnt = cur.fetchone()[0]
    assert cnt == 1
