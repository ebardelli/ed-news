import sqlite3
from ednews import feeds, db as eddb, crossref
import pytest
from datetime import datetime, timezone


def setup_in_memory_db():
    conn = eddb.get_connection(":memory:")
    eddb.init_db(conn)
    return conn


def test_feed_entry_with_doi_is_augmented(monkeypatch):
    conn = setup_in_memory_db()
    cur = conn.cursor()

    # Prepare a fake feed entry with a DOI in the link
    entry = {
        'guid': 'g1',
        'title': 'Feed Title',
        'link': 'https://doi.org/10.1000/testdoi',
        'published': '2020-01-01T00:00:00Z',
        'summary': 'Feed summary',
        '_entry': {'link': 'https://doi.org/10.1000/testdoi', 'links': [{'href': 'https://doi.org/10.1000/testdoi'}]},
        '_feed_publication_id': None,
        '_feed_issn': None,
    }

    # Mock Crossref to return richer metadata
    def fake_fetch(doi):
        assert doi == '10.1000/testdoi'
        return {
            'authors': 'Crossref Author',
            'abstract': 'Crossref abstract text',
            'published': '2020-01-01',
            'raw': '<xml>crossref</xml>'
        }

    monkeypatch.setattr(crossref, 'fetch_crossref_metadata', fake_fetch)

    # Call save_entries to insert items/articles
    inserted = feeds.save_entries(conn, 'feed1', 'Feed Title', [entry])
    assert inserted == 1

    # Check items row has DOI attached
    cur.execute('SELECT id, doi, published FROM items')
    rows = cur.fetchall()
    assert len(rows) == 1
    item_id, item_doi, item_published = rows[0]
    assert item_doi == '10.1000/testdoi'

    # Check articles table has Crossref-preferred fields
    cur.execute('SELECT doi, title, authors, abstract, crossref_xml, published FROM articles WHERE doi = ?', ('10.1000/testdoi',))
    arow = cur.fetchone()
    assert arow is not None
    a_doi, a_title, a_authors, a_abstract, a_raw, a_published = arow
    assert a_doi == '10.1000/testdoi'
    # Title may be from feed since crossref fetch in our impl doesn't return title; ensure authors/abstract come from crossref
    assert a_authors == 'Crossref Author'
    assert a_abstract == 'Crossref abstract text'
    assert a_raw == '<xml>crossref</xml>'
    # Published value should be set from crossref published
    assert a_published == '2020-01-01'


def test_feed_entry_without_crossref_falls_back(monkeypatch):
    conn = setup_in_memory_db()
    cur = conn.cursor()

    entry = {
        'guid': 'g2',
        'title': 'Feed Title Two',
        'link': 'https://example.com/article/123',
        'published': '2021-06-01T00:00:00Z',
        'summary': 'Feed summary two',
        '_entry': {'link': 'https://example.com/article/123', 'summary': 'Feed summary two', 'links': []},
        '_feed_publication_id': None,
        '_feed_issn': None,
    }

    # No DOI will be extracted, ensure no exception and no insertion of article
    inserted = feeds.save_entries(conn, 'feed1', 'Feed Title', [entry])
    # An item should be saved even without DOI
    assert inserted == 1
    cur.execute('SELECT id, doi FROM items WHERE guid = ?', ('g2',))
    r = cur.fetchone()
    assert r is not None
    assert r[1] is None
