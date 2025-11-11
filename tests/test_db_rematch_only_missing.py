import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publications mapping
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "pub-a", "Feed A", ""))
    # items: one missing DOI
    cur.execute(
        "INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (None, "f1", "g1", "Title 1", "http://example/1", "h1", now, now),
    )
    conn.commit()


def test_rematch_only_missing_uses_feed_publication_and_updates(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Ensure feeds.load_feeds provides pub id for f1
    try:
        import ednews.feeds as feeds_mod

        monkeypatch.setattr(feeds_mod, "load_feeds", lambda: [("f1", "Feed A", "http://example/", "pub-feed-config", None, None)])
    except Exception:
        pass

    # Mock crossref lookup to return the DOI and ensure feed publication id is used
    called = {}

    def fake_query(title, preferred_publication_id=None):
        assert preferred_publication_id == 'pub-feed-config'
        called['invoked'] = True
        return '10.0/newdoi'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch with only_missing
    res = rematch_publication_dois(conn, publication_id=None, feed_keys=['f1'], dry_run=False, remove_orphan_articles=False, only_wrong=False, only_missing=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g1',))
    row = cur.fetchone()
    assert row and row[0] == '10.0/newdoi'

    # Check that an article row was ensured and has publication_id from feed config
    cur.execute("SELECT publication_id FROM articles WHERE doi = ?", ('10.0/newdoi',))
    arow = cur.fetchone()
    # publication_id may be set by upsert_article; expect it to equal feed config
    assert arow and arow[0] == 'pub-feed-config'

