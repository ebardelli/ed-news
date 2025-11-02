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

    # Capture entries passed to postprocessor and ensure their doi is None
    captured = {'entries': None, 'called': False}

    def fake_postprocessor(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, **kwargs):
        captured['called'] = True
        captured['entries'] = entries
        # Simulate resolving DOI for guid g1 and updating DB
        cur = conn_arg.cursor()
        for e in entries:
            if e.get('guid') == 'g1' or e.get('link') == 'http://example/1':
                cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", ('10.1162/edfp.00001', feed_key, 'g1'))
                cur.execute("DELETE FROM articles WHERE doi = ?", ('10.3386/w28669',))
                cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ('10.1162/edfp.00001', 'The Insurance Value of Financial Aid', 'X', 'abs', None, feed_key, 'edfp', None, datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()))
        conn_arg.commit()
        return 1

    import ednews.processors as proc_mod
    monkeypatch.setattr(proc_mod, 'crossref_postprocessor_db', fake_postprocessor, raising=False)

    res = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True)

    # Ensure postprocessor was called
    assert captured['called'] is True
    # Ensure entries were passed and that for our target guid the doi is None
    assert isinstance(captured['entries'], list)
    found = False
    for e in captured['entries']:
        if e.get('guid') == 'g1':
            found = True
            assert e.get('doi') is None
    assert found

    # Confirm DB was updated by the fake postprocessor
    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')
