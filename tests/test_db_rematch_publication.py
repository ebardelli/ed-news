import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db(conn):
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


def test_rematch_publication_updates_wrong_publication(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Monkeypatch crossref lookup to return the matching DOI for the item/title
    def fake_query(title, preferred_publication_id=None):
        return '10.1162/edfp.00001'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch
    res = rematch_publication_dois(conn, publication_id='edfp', feed_keys=None, dry_run=False, remove_orphan_articles=False)

    # After rematch, item DOI should be updated and article should refer to edfp DOI
    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')

    cur.execute("SELECT publication_id FROM articles WHERE doi = ?", ('10.1162/edfp.00001',))
    row2 = cur.fetchone()
    assert row2 and row2[0] == 'edfp'