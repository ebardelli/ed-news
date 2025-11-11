import sqlite3
import types
import sys
from ednews.db import rematch_publication_dois


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    # publication mapping (publication_id -> feed_id f1)
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "10.26300", "EdWorkingPapers Feed", ""))
    # Insert an item with missing DOI (candidate for rematch)
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (None, "f1", "ai25-1322", "Title", "https://edworkingpapers.com/edworkingpapers/ai25-1322", "h1", "2025-11-01", "2025-11-01T00:00:00Z"))
    conn.commit()


def test_rematch_uses_edworkingpapers_postprocessor(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    called = {}

    # Fake load_feeds to return a feed configured for this publication
    def fake_load_feeds():
        return [("f1", "EdWorkingPapers", "https://edworkingpapers.com/feed", "10.26300", None, None)]

    import ednews.feeds as feeds_mod
    monkeypatch.setattr(feeds_mod, "load_feeds", fake_load_feeds)

    # Monkeypatch crossref lookup to simulate finding a DOI by title
    def fake_query(title, preferred_publication_id=None):
        called['invoked'] = True
        return '10.26300/ai25-1322'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch in only_missing mode (should process the missing-doi item)
    res = rematch_publication_dois(conn, publication_id='10.26300', feed_keys=None, dry_run=False, remove_orphan_articles=False, only_missing=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('ai25-1322',))
    row = cur.fetchone()
    assert row and row[0] == '10.26300/ai25-1322'
