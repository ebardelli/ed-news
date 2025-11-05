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

    # Fake load_feeds to return a feed configured with edworkingpapers postprocessor
    def fake_load_feeds():
        # returns list of tuples: (key, title, url, publication_id, issn, processor)
        return [("f1", "EdWorkingPapers", "https://edworkingpapers.com/feed", "10.26300", None, {"pre": "edworkingpapers", "post": "edworkingpapers"})]

    import ednews.feeds as feeds_mod
    monkeypatch.setattr(feeds_mod, "load_feeds", fake_load_feeds)

    # Monkeypatch processors module to provide edworkingpapers_postprocessor_db
    def fake_edwp_postprocessor_db(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False):
        called['invoked'] = True
        # Simulate attaching a DOI to the item
        cur = conn_arg.cursor()
        cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", ("10.26300/ai25-1322", feed_key, "ai25-1322"))
        # Upsert an article row
        cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("10.26300/ai25-1322", "Title", "A", "abs", None, feed_key, publication_id, issn, "2025-11-01", "2025-11-01T00:00:00Z"))
        conn_arg.commit()
        return 1

    try:
        import ednews.processors as proc_mod
        monkeypatch.setattr(proc_mod, "edworkingpapers_postprocessor_db", fake_edwp_postprocessor_db, raising=False)
    except Exception:
        # inject minimal module
        mod = types.ModuleType('ednews.processors')
        mod.edworkingpapers_postprocessor_db = fake_edwp_postprocessor_db
        sys.modules['ednews.processors'] = mod

    # Run rematch in only_wrong mode (should process the missing-doi item)
    res = rematch_publication_dois(conn, publication_id='10.26300', feed_keys=None, dry_run=False, remove_orphan_articles=False, only_wrong=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('ai25-1322',))
    row = cur.fetchone()
    assert row and row[0] == '10.26300/ai25-1322'
