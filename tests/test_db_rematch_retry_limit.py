import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db_retry(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "edfp", "Econ Dev FP", ""))
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("wrong:doi", "f1", "g1", "Title one", "http://example/1", "h1", now, now))
    conn.commit()


def test_rematch_skips_after_retry_limit(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db_retry(conn)

    call_count = {'calls': 0}

    def fake_postprocessor(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, **kwargs):
        call_count['calls'] += 1
        # Simulate failure to find DOI (no updates)
        return 0

    import ednews.processors as proc_mod
    monkeypatch.setattr(proc_mod, 'crossref_postprocessor_db', fake_postprocessor, raising=False)

    # First run with retry_limit=1 should attempt and increment attempts
    res1 = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True, retry_limit=1)
    assert call_count['calls'] == 1

    # Second run should skip the guid because attempts >= retry_limit
    res2 = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True, retry_limit=1)
    # Ensure we recorded skipped_due_to_retry_limit for the feed
    assert 'f1' in res2['feeds'] and res2['feeds']['f1'].get('skipped_due_to_retry_limit', 0) >= 1
    # postprocessor should not have been called again
    assert call_count['calls'] == 1
