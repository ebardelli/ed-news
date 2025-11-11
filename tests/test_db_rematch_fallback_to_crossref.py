import sqlite3
import types
import sys
from ednews.db import rematch_publication_dois


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    # publication mapping (publication_id -> feed_id f1)
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "edfp", "Econ Dev FP", ""))
    # Insert an item with missing DOI (candidate for rematch)
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (None, "f1", "g1", "Title", "http://example/1", "h1", "2025-11-01", "2025-11-01T00:00:00Z"))
    conn.commit()


def test_rematch_falls_back_to_crossref(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    called = {}

    # Fake load_feeds to return a feed configured WITHOUT a postprocessor
    def fake_load_feeds():
        return [("f1", "Econ Dev FP", "http://example/feed", "edfp", None, None)]

    import ednews.feeds as feeds_mod
    monkeypatch.setattr(feeds_mod, "load_feeds", fake_load_feeds)

    # Mock crossref title lookup to return a DOI
    def fake_query(title, preferred_publication_id=None):
        called['invoked'] = True
        return '10.1162/edfp.00001'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch in only_missing mode (should process the missing-doi item)
    res = rematch_publication_dois(conn, publication_id='edfp', feed_keys=None, dry_run=False, remove_orphan_articles=False, only_missing=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')
