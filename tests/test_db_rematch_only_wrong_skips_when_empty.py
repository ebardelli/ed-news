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

    # Call rematch_publication_dois with only_wrong=True and dry_run so no DB changes
    results = maintenance.rematch_publication_dois(conn, publication_id='aerj', feed_keys=['aerj'], dry_run=True, only_wrong=True)

    # Ensure no processing occurred for the feed: postprocessor_results should be 0
    assert 'postprocessor_results' in results
    assert results['postprocessor_results'].get('aerj', 0) == 0

