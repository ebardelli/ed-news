import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db_for_rematch(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publication mapping (publication_id -> feed_id f1)
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "edfp", "Econ Dev FP", ""))
    # Insert an item that currently has a DOI from the wrong publisher
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("10.3386/w28669", "f1", "g1", "The Insurance Value of Financial Aid", "http://example/1", "h1", now, now))
    # Insert an article row for that doi but with wrong publication_id
    cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("10.3386/w28669", "The Insurance Value of Financial Aid", "A", "abs", None, "f1", "wrongpub", None, now, now))
    conn.commit()


def test_rematch_passes_entries_with_none_doi_and_updates(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db_for_rematch(conn)

    # We'll wrap the crossref.lookup to capture which titles the rematch attempts
    captured = {'titles': [], 'called': False}

    def fake_query(title, preferred_publication_id=None):
        captured['called'] = True
        captured['titles'].append((title, preferred_publication_id))
        # simulate resolving DOI
        return '10.1162/edfp.00001'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    res = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True)

    # Ensure crossref lookup was invoked and the title passed was the item's title
    assert captured['called'] is True
    assert any('The Insurance Value of Financial Aid' in t[0] for t in captured['titles'])

    # Confirm DB was updated by rematch logic using crossref result
    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')
