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
        # returns list of tuples: (key, title, url, publication_id, issn, processor)
        # processor set to a dict with only pre so no post_names will be resolved
        return [("f1", "Econ Dev FP", "http://example/feed", "edfp", None, {"pre": "rss"})]

    import ednews.feeds as feeds_mod
    monkeypatch.setattr(feeds_mod, "load_feeds", fake_load_feeds)

    # Provide a fake crossref_postprocessor_db on ednews.processors
    def fake_crossref_postprocessor_db(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False):
        called['invoked'] = True
        cur = conn_arg.cursor()
        # Simulate attaching a DOI to the item
        for e in entries:
            guid = e.get('guid')
            if guid == 'g1':
                cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", ('10.1162/edfp.00001', feed_key, guid))
                cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ('10.1162/edfp.00001', 'Title', 'X', 'abs', None, feed_key, publication_id, issn, '2025-11-01', '2025-11-01T00:00:00Z'))
        conn_arg.commit()
        return 1

    try:
        import ednews.processors as proc_mod
        monkeypatch.setattr(proc_mod, "crossref_postprocessor_db", fake_crossref_postprocessor_db, raising=False)
    except Exception:
        # inject minimal module
        mod = types.ModuleType('ednews.processors')
        mod.crossref_postprocessor_db = fake_crossref_postprocessor_db
        sys.modules['ednews.processors'] = mod

    # Run rematch in only_wrong mode (should process the missing-doi item)
    res = rematch_publication_dois(conn, publication_id='edfp', feed_keys=None, dry_run=False, remove_orphan_articles=False, only_wrong=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')
