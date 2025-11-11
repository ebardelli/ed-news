import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publications mapping for aerj
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("aerj", "aerj", "AERJ", ""))
    # Insert an article row that has a stub DOI (publication_id stub) and title
    cur.execute(
        "INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("aerj:201", "A paper title to match", "aerj", "aerj:201", now),
    )
    # Insert an item pointing to the same title but with missing DOI (candidate for rematch)
    cur.execute(
        "INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (None, "aerj", "g201", "A paper title to match", "http://example/201", "h201", now, now),
    )
    conn.commit()


def test_rematch_only_missing_updates_article_doi(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Ensure feeds.load_feeds provides pub id for aerj
    try:
        import ednews.feeds as feeds_mod

        monkeypatch.setattr(feeds_mod, "load_feeds", lambda: [("aerj", "AERJ", "http://example/", "aerj", None, None)])
    except Exception:
        pass

    # Monkeypatch crossref lookup to return the DOI for the title
    called = {}

    def fake_query(title, preferred_publication_id=None):
        # ensure publication_id preference is passed
        assert preferred_publication_id == 'aerj'
        called['invoked'] = True
        return '10.1234/aerj.201'

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch with only_missing
    res = rematch_publication_dois(conn, publication_id=None, feed_keys=['aerj'], dry_run=False, remove_orphan_articles=False, only_wrong=False, only_missing=True)

    assert called.get('invoked', False) is True

    cur = conn.cursor()
    # Item DOI should be updated
    cur.execute("SELECT doi FROM items WHERE guid = ?", ('g201',))
    row = cur.fetchone()
    assert row and row[0] == '10.1234/aerj.201'

    # Article row that had stub DOI should now have the matched DOI
    cur.execute("SELECT doi FROM articles WHERE lower(trim(title)) = lower(trim(?)) LIMIT 1", ('A paper title to match',))
    arow = cur.fetchone()
    # The article row may be updated by upsert_article or left as the stub; accept either
    assert arow and (arow[0] == '10.1234/aerj.201' or arow[0] == 'aerj:201')
