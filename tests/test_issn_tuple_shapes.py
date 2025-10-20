import sqlite3

import pytest


def test_fetch_latest_journal_works_accepts_extra_fields(monkeypatch):
    """Previously feeds with extra fields (e.g., processor) were skipped.

    Ensure a feed tuple with 6 items (key, title, url, publication_id, issn, processor)
    is processed and that an article from returned Crossref JSON is inserted.
    """
    conn = sqlite3.connect(":memory:")
    from ednews.db import init_db

    init_db(conn)

    # Provide a feeds list where each item has an extra 'processor' field
    feeds = [
        ("k1", "Title", "http://example.invalid/feed", "10.1000", "1234-5678", "crossref"),
    ]

    # Fake Crossref JSON response with a single item
    fake_json = {"message": {"items": [{"DOI": "10.1000/xyz", "title": "X"}]}}

    class Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return fake_json

    # Monkeypatch requests.Session.get so network isn't used
    monkeypatch.setattr("requests.Session.get", lambda self, url, params=None, headers=None, timeout=None: Resp())

    from ednews.db.manage_db import fetch_latest_journal_works

    inserted = fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1)
    assert inserted == 1

    cur = conn.cursor()
    cur.execute("SELECT doi FROM articles WHERE doi = ?", ("10.1000/xyz",))
    row = cur.fetchone()
    assert row and row[0] == "10.1000/xyz"
