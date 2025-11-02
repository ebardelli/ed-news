import logging
import sqlite3

import pytest

from ednews.db import maintenance
import ednews.processors as ed_processors


def test_rematch_only_wrong_skips_postprocessor(monkeypatch, caplog):
    """When --only-wrong is used and no wrong/missing DOIs are found for a feed,
    the postprocessor should not be invoked and the function should return quickly.
    """
    caplog.set_level(logging.INFO)

    # Create an in-memory sqlite3 DB with minimal schema the function expects.
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    # Minimal publications and items tables used by rematch_publication_dois
    cur.execute("CREATE TABLE publications (feed_id TEXT, publication_id TEXT)")
    cur.execute("CREATE TABLE items (guid TEXT, feed_id TEXT, doi TEXT, link TEXT, title TEXT, published TEXT, fetched_at TEXT)")
    # Insert a publication row so the function resolves one feed key
    cur.execute("INSERT INTO publications (feed_id, publication_id) VALUES (?, ?)", ("aerj", "aerj"))
    conn.commit()

    called = {'postprocessor': False}

    def fake_postprocessor(conn_arg, feed_key, entries, session=None, **kwargs):
        called['postprocessor'] = True
        return 1

    # Monkeypatch the symbol the maintenance module looks up on import
    monkeypatch.setattr(ed_processors, 'crossref_postprocessor_db', fake_postprocessor, raising=False)

    # Call rematch_publication_dois with only_wrong=True and dry_run so no DB changes
    results = maintenance.rematch_publication_dois(conn, publication_id='aerj', dry_run=True, only_wrong=True)

    # Ensure we logged that zero wrong items were identified and that the
    # postprocessor was skipped.
    assert 'feeds' in results
    assert 'aerj' in results['feeds']
    # No postprocessor calls should have happened
    assert called['postprocessor'] is False
    assert any('identified 0 wrong/missing DOIs' in r.getMessage() for r in caplog.records)

