import sqlite3

from ednews.processors import crossref_postprocessor_db
from ednews.db.schema import init_db


def test_crossref_postprocess_prefers_feed_publication_id(monkeypatch):
    """Integration-style test: two feeds with same title but different
    publication_id prefixes should each get the DOI matching their
    configured publication_id when postprocessed.
    """
    conn = sqlite3.connect(':memory:')
    init_db(conn)
    cur = conn.cursor()

    # Create two items, each belonging to a different feed but sharing the same title
    title = 'A Much Longer Identical Title For Testing Purposes'
    link_a = 'http://example/a'
    guid_a = 'gid-a'
    feed_a = 'feed.a'
    pub_a = '10.1111'

    link_b = 'http://example/b'
    guid_b = 'gid-b'
    feed_b = 'feed.b'
    pub_b = '10.3333'

    cur.execute("INSERT INTO items (feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (feed_a, guid_a, title, link_a, 'uhash-a', '2020-01-01', '2020-01-01'))
    cur.execute("INSERT INTO items (feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)", (feed_b, guid_b, title, link_b, 'uhash-b', '2020-01-01', '2020-01-01'))
    conn.commit()

    # Monkeypatch Crossref title search to return two candidate DOIs; the
    # postprocessor should prefer the DOI with the publication_id prefix.
    fake_resp = {"message": {"items": [{"DOI": f"{pub_a}/one"}, {"DOI": f"{pub_b}/target"}]}}

    def fake_get_json(url, params=None, headers=None, timeout=None, retries=None, backoff=None, status_forcelist=None, requests_module=None):
        return fake_resp

    import ednews.http as http_mod
    monkeypatch.setattr(http_mod, 'get_json', fake_get_json)
    
    # Also patch fetch_crossref_metadata so the postprocessor receives metadata
    import ednews.crossref as crossref_mod

    def fake_fetch_metadata(doi, timeout=10, conn=None):
        return {'authors': 'A Author', 'abstract': 'An abstract', 'raw': '<xml/>'}

    monkeypatch.setattr(crossref_mod, 'fetch_crossref_metadata', fake_fetch_metadata)

    # Sanity-check title lookup selects the preferred DOI for each publication id
    found_for_a = crossref_mod._query_crossref_doi_by_title_uncached(title, preferred_publication_id=pub_a)
    assert found_for_a == f"{pub_a}/one"
    found_for_b = crossref_mod._query_crossref_doi_by_title_uncached(title, preferred_publication_id=pub_b)
    assert found_for_b == f"{pub_b}/target"

    # Ensure cached wrapper doesn't return stale results from other tests
    try:
        crossref_mod.query_crossref_doi_by_title.cache_clear()
    except Exception:
        pass

    # Patch the public cached wrapper to return based on preferred_publication_id
    def fake_query(title_arg, preferred_publication_id=None, timeout=8):
        if preferred_publication_id == pub_a:
            return f"{pub_a}/one"
        if preferred_publication_id == pub_b:
            return f"{pub_b}/target"
        return None

    monkeypatch.setattr(crossref_mod, 'query_crossref_doi_by_title', fake_query)

    # Run postprocessor for feed A and B separately, passing their publication_id
    rows_a = cur.execute("SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ?", (feed_a,)).fetchall()
    entries_a = [{'guid': r[0], 'link': r[1], 'title': r[2], 'published': r[3], '_fetched_at': r[4], 'doi': r[5] if len(r) > 5 else None} for r in rows_a]

    rows_b = cur.execute("SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ?", (feed_b,)).fetchall()
    entries_b = [{'guid': r[0], 'link': r[1], 'title': r[2], 'published': r[3], '_fetched_at': r[4], 'doi': r[5] if len(r) > 5 else None} for r in rows_b]

    updated_a = crossref_postprocessor_db(conn, feed_a, entries_a, session=None, publication_id=pub_a, issn=None)
    updated_b = crossref_postprocessor_db(conn, feed_b, entries_b, session=None, publication_id=pub_b, issn=None)

    assert updated_a >= 1
    assert updated_b >= 1

    # Verify each article in articles table has the expected DOI
    cur.execute("SELECT doi FROM articles WHERE feed_id = ?", (feed_a,))
    row_a = cur.fetchone()
    assert row_a is not None and row_a[0].startswith(pub_a)

    cur.execute("SELECT doi FROM articles WHERE feed_id = ?", (feed_b,))
    row_b = cur.fetchone()
    assert row_b is not None and row_b[0].startswith(pub_b)

    # Also ensure items rows were updated with the DOI
    cur.execute("SELECT doi FROM items WHERE guid = ?", (guid_a,))
    item_a = cur.fetchone()
    assert item_a is not None and item_a[0].startswith(pub_a)

    cur.execute("SELECT doi FROM items WHERE guid = ?", (guid_b,))
    item_b = cur.fetchone()
    assert item_b is not None and item_b[0].startswith(pub_b)

    conn.close()
