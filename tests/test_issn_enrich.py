import sqlite3
from unittest import mock

import pytest

from ednews.db import init_db, fetch_latest_journal_works, get_missing_crossref_dois


class DummyResp:
    def __init__(self, json_data=None, status=200, content=b""):
        self._json = json_data or {}
        self.status_code = status
        self.content = content

    def json(self):
        return self._json

    def raise_for_status(self):
        if not (200 <= self.status_code < 300):
            raise Exception("HTTP error")


def test_fetch_latest_journal_works_enriches_with_crossref(monkeypatch, tmp_path):
    # Prepare in-memory DB
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    # Mock the Crossref journals API to return one work with a DOI
    sample_work = {
        "message": {
            "items": [
                {"DOI": "10.1000/testdoi", "title": ["Test Article"], "abstract": "Short abstract"}
            ]
        }
    }

    def fake_get(url, params=None, headers=None, timeout=None):
        return DummyResp(json_data=sample_work)

    monkeypatch.setattr("requests.Session.get", lambda self, url, params=None, headers=None, timeout=None: fake_get(url, params, headers, timeout))

    # Mock fetch_crossref_metadata to return enriched data including raw XML/text
    fake_cr = {"authors": "Alice, Bob", "abstract": "Enriched abstract", "raw": "<xml>...</xml>", "published": "2025-10-01"}

    monkeypatch.setattr("ednews.crossref.fetch_crossref_metadata", lambda doi, timeout=10: fake_cr)

    # feeds list entry: (feed_id, title, url, publication_id, issn)
    feeds = [("feed1", "Feed One", "http://example.com", "pub1", "1234-5678")]

    # Run the ISSN lookup
    inserted = fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1, delay=0)

    assert inserted == 1

    # Verify the article row has Crossref data populated
    cur = conn.cursor()
    cur.execute("SELECT doi, authors, abstract, crossref_xml, published FROM articles WHERE doi = ?", ("10.1000/testdoi",))
    row = cur.fetchone()
    assert row is not None
    doi, authors, abstract, crossref_xml, published = row
    assert doi == "10.1000/testdoi"
    # Authors and abstract should reflect Crossref enrichment
    assert authors is not None and "Alice" in authors
    assert abstract is not None and "Enriched abstract" in abstract
    assert crossref_xml is not None and "<xml>" in crossref_xml
    assert published is not None and published.startswith("2025")

    conn.close()
