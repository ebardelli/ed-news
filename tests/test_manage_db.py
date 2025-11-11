import sqlite3
import json
from types import SimpleNamespace
import pytest
from datetime import datetime, timezone, timedelta

from ednews.db import manage_db


def test_vacuum_db_runs_successfully(tmp_path):
    # create an on-disk sqlite file and initialize schema
    p = tmp_path / "ednews.db"
    conn = sqlite3.connect(str(p))
    manage_db.init_db(conn)
    # insert a simple row to ensure file has content
    cur = conn.cursor()
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "p1", "Feed", "1234-5678"))
    conn.commit()
    # vacuum should succeed and return True
    res = manage_db.vacuum_db(conn)
    conn.close()
    assert res is True


def make_response(status_code=200, json_obj=None):
    class Resp:
        def __init__(self, status_code, json_obj):
            self.status_code = status_code
            self._json = json_obj or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise Exception(f"HTTP {self.status_code}")

        def json(self):
            return self._json

    return Resp(status_code, json_obj)


def test_fetch_latest_journal_works_retries_and_inserts(monkeypatch):
    # simulate a Crossref works response
    sample_items = [
        {"DOI": "10.1000/xyz", "title": "Test Article", "abstract": "Abs"}
    ]
    sample_json = {"message": {"items": sample_items}}

    calls = {"count": 0}

    def fake_get(url, params=None, headers=None, timeout=None):
        calls["count"] += 1
        # first two calls: simulate retryable network timeout
        if calls["count"] < 3:
            import requests

            raise requests.exceptions.ReadTimeout("timeout")
        return make_response(status_code=200, json_obj=sample_json)

    # monkeypatch requests.Session.get used inside the function
    import requests

    original_Session = requests.Session

    class DummySession:
        def __init__(self):
            pass

        def get(self, *args, **kwargs):
            return fake_get(*args, **kwargs)

    monkeypatch.setattr(requests, "Session", lambda: DummySession())

    # prepare an in-memory DB and feeds list containing an item with an ISSN
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    feeds = [("feed-key", "Title", "http://example.com", "pubid", "1234-5678")]

    # run with small per_journal so it finishes quickly
    inserted = manage_db.fetch_latest_journal_works(conn, feeds, per_journal=1, timeout=1, delay=0)
    # cleanup
    conn.close()
    # ensure the fake_get retried before success
    assert calls["count"] >= 3
    # function should report inserted >= 1
    assert inserted >= 1


def test_cleanup_empty_articles_removes_rows():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # empty article (should be deleted)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/empty', None, None, now))
    # article with title (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/has', 'T', None, now))
    # article with abstract (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/has2', None, 'abs', now))
    conn.commit()
    deleted = manage_db.cleanup_empty_articles(conn)
    assert deleted == 1
    cur.execute('SELECT doi FROM articles')
    rows = {r[0] for r in cur.fetchall()}
    assert '10.0/empty' not in rows
    assert '10.0/has' in rows and '10.0/has2' in rows
    conn.close()


def test_cleanup_empty_articles_respects_age():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    old_ts = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    recent_ts = datetime.now(timezone.utc).isoformat()
    # old empty article (should be deleted when older_than_days=1)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/old', None, None, old_ts))
    # recent empty article (should remain)
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/new', None, None, recent_ts))
    conn.commit()
    deleted = manage_db.cleanup_empty_articles(conn, older_than_days=1)
    assert deleted == 1
    cur.execute('SELECT doi FROM articles')
    rows = {r[0] for r in cur.fetchall()}
    assert '10.0/old' not in rows
    assert '10.0/new' in rows
    conn.close()


def test_cleanup_filtered_titles_deletes_matching_rows():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # Insert articles with various titles
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/keep', 'A Good Title', 'abs', now))
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/delete', 'Editorial Board', 'note', now))
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/delete2', '  Editorial Board  ', None, now))
    conn.commit()

    # Ensure default config filter includes 'editorial board'
    deleted = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=False)
    assert deleted == 2

    cur.execute('SELECT doi, title FROM articles')
    rows = {r[0]: r[1] for r in cur.fetchall()}
    assert '10.0/keep' in rows
    assert '10.0/delete' not in rows
    assert '10.0/delete2' not in rows
    conn.close()


def test_cleanup_filtered_titles_dry_run_counts_only():
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/keep', 'A Good Title', 'abs', now))
    cur.execute('INSERT INTO articles (doi, title, abstract, fetched_at) VALUES (?, ?, ?, ?)', ('10.0/delete', 'Editorial Board', 'note', now))
    conn.commit()

    count = manage_db.cleanup_filtered_titles(conn, filters=None, dry_run=True)
    # dry-run should report 1 but not delete
    assert count == 1
    cur.execute('SELECT doi FROM articles')
    rows = {r[0] for r in cur.fetchall()}
    assert '10.0/delete' in rows
    conn.close()


def test_remove_feed_articles_by_feed_and_publication(tmp_path):
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # Articles for two feeds and two publications
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.0/a', 'A', None, 'feed1', 'pub1', now))
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.0/b', 'B', None, 'feed2', 'pub1', now))
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.0/c', 'C', None, 'feed2', 'pub2', now))
    conn.commit()

    # Dry-run by feed
    would_delete = manage_db.remove_feed_articles(conn, feed_keys=['feed2'], publication_id=None, dry_run=True)
    assert would_delete == 2

    # Actually delete by feed
    deleted = manage_db.remove_feed_articles(conn, feed_keys=['feed2'], publication_id=None, dry_run=False)
    assert deleted == 2
    cur.execute('SELECT doi FROM articles')
    remain = {r[0] for r in cur.fetchall()}
    assert '10.0/a' in remain and '10.0/b' not in remain and '10.0/c' not in remain

    # Now test DOI-stub matching: insert an article for feed 'edfp' where
    # publication stub is '10.1162/edfp' but DOI does not start with that stub
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.9999/other', 'X', None, 'edfp', '10.1162/edfp', now))
    conn.commit()
    # Dry-run should count 1 to delete (mismatched DOI)
    would = manage_db.remove_feed_articles(conn, feed_keys=['edfp'], publication_id=None, dry_run=True)
    assert would >= 1
    # Actual delete should remove it
    delcount = manage_db.remove_feed_articles(conn, feed_keys=['edfp'], publication_id=None, dry_run=False)
    assert delcount >= 1

    # Finally: feed with no publication mapping should have DOIs removed
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.0/keepme', 'Keep', None, 'feed_nopub', None, now))
    conn.commit()
    # Dry-run should report 1 would be deleted
    would2 = manage_db.remove_feed_articles(conn, feed_keys=['feed_nopub'], publication_id=None, dry_run=True)
    assert would2 == 1
    # Actual delete removes it
    del2 = manage_db.remove_feed_articles(conn, feed_keys=['feed_nopub'], publication_id=None, dry_run=False)
    assert del2 == 1

    # Add back an article and delete by publication_id
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', ('10.0/d', 'D', None, 'feedX', 'pub1', now))
    conn.commit()
    deleted_pub = manage_db.remove_feed_articles(conn, feed_keys=None, publication_id='pub1', dry_run=False)
    # Should delete remaining article(s) with publication_id pub1 (10.0/a and 10.0/d)
    assert deleted_pub >= 1
    cur.execute('SELECT publication_id, doi FROM articles')
    rows = cur.fetchall()
    for pub, doi in rows:
        assert pub != 'pub1'

    conn.close()


def test_remove_articles_for_configured_unmapped_feed(monkeypatch):
    """If a feed is present in the configured feeds list but has no
    publication_id, ensure `remove_feed_articles` treats it as intentionally
    unmapped and deletes articles with DOIs for that feed.
    """
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # Insert a publications row for 'mt' to ensure DB fallback exists
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ('mt', '10.9999', 'Mock Title', ''))

    # Insert articles for feed 'mt' that have DOIs (they should be removed)
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.1007/abc', 'X', 'mt', None, now))
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.2000/def', 'Y', 'mt', None, now))
    conn.commit()

    # Monkeypatch feeds.load_feeds to return an explicit mapping for 'mt'
    def fake_load_feeds():
        return [('mt', 'Mathematics Teacher', 'http://example', None, None, None)]

    import ednews.feeds as feeds_mod
    monkeypatch.setattr(feeds_mod, 'load_feeds', fake_load_feeds)

    # Dry-run should report 2 deletions
    would = manage_db.remove_feed_articles(conn, feed_keys=['mt'], publication_id=None, dry_run=True)
    assert would == 2

    # Actual delete removes them
    deleted = manage_db.remove_feed_articles(conn, feed_keys=['mt'], publication_id=None, dry_run=False)
    assert deleted == 2

    cur.execute('SELECT doi FROM articles WHERE feed_id = ?', ('mt',))
    rows = cur.fetchall()
    assert not rows
    conn.close()


def test_remove_empty_doi_rows_when_publication_stub_present():
    """When a feed is mapped to a publication DOI-stub, remove_feed_articles
    should also delete article rows that have no DOI but are tagged with the
    same publication_id to avoid duplicates (one row with NULL DOI and one with
    the correct DOI).
    """
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # Simulate feed mapped to publication stub '10.1162/edfp'
    # Ensure publications table has a mapping so remove_feed_articles resolves expected_pub
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ('edfp', '10.1162/edfp', 'EDFP', ''))
    # Insert a placeholder article with no DOI but publication_id set
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', (None, 'Placeholder', 'edfp', '10.1162/edfp', now))
    # Insert the real article with DOI
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.1162/edfp.0001', 'Real', 'edfp', '10.1162/edfp', now))
    conn.commit()

    # Dry-run should report 1 deletion for the empty-doi row
    would = manage_db.remove_feed_articles(conn, feed_keys=['edfp'], publication_id=None, dry_run=True)
    assert would >= 1

    # Actual deletion should remove the empty-doi row but keep the real DOI row
    deleted = manage_db.remove_feed_articles(conn, feed_keys=['edfp'], publication_id=None, dry_run=False)
    assert deleted >= 1

    cur.execute('SELECT doi FROM articles WHERE feed_id = ?', ('edfp',))
    rows = [r[0] for r in cur.fetchall()]
    assert '10.1162/edfp.0001' in rows
    # None/NULL should not be present among DOIs
    assert None not in rows and '' not in rows
    conn.close()


def test_remove_placeholder_rows_with_issn_in_publication_id():
    """If a legacy placeholder row stored the ISSN in publication_id, ensure
    remove_feed_articles for the configured feed removes it.
    """
    conn = sqlite3.connect(":memory:")
    manage_db.init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # Insert a publications-like row that incorrectly stores ISSN in publication_id
    # Simulate legacy data where publication_id contains the ISSN
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', (None, 'Placeholder ISSN', 'lni', '0959-4752', now))
    # Also insert a real DOI row for the same feed/publication
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.1016/j.learninstruc.0001', 'Real', 'lni', '10.1016/j.learninstruc', now))
    conn.commit()

    # Monkeypatch feeds.load_feeds to ensure the feed mapping exists for 'lni'
    import ednews.feeds as feeds_mod

    def fake_load_feeds():
        return [('lni', 'Learning and Instruction', 'http://example', '10.1016/j.learninstruc', '0959-4752', None)]

    from types import SimpleNamespace
    # Apply monkeypatch by replacing the function in module
    feeds_mod.load_feeds = fake_load_feeds

    # Dry-run should report deletion of the placeholder
    would = manage_db.remove_feed_articles(conn, feed_keys=['lni'], publication_id=None, dry_run=True)
    assert would >= 1

    # Actual deletion should remove the placeholder but keep the real DOI
    deleted = manage_db.remove_feed_articles(conn, feed_keys=['lni'], publication_id=None, dry_run=False)
    assert deleted >= 1

    cur.execute('SELECT doi FROM articles WHERE feed_id = ?', ('lni',))
    rows = [r[0] for r in cur.fetchall()]
    assert '10.1016/j.learninstruc.0001' in rows
    assert None not in rows and '' not in rows
    conn.close()
