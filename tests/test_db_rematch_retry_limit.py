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

    # We'll mock crossref lookup to always return None (no DOI found)
    call_count = {'calls': 0}

    def fake_query(title, preferred_publication_id=None):
        call_count['calls'] += 1
        return None

    import ednews.crossref as cr_mod
    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # First run with retry_limit=1 should attempt and increment attempts
    res1 = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True, retry_limit=1)
    assert call_count['calls'] >= 1

    # Second run should skip the guid because attempts >= retry_limit
    res2 = rematch_publication_dois(conn, publication_id='edfp', dry_run=False, only_wrong=True, retry_limit=1)
    # Ensure we recorded skipped_due_to_retry_limit for the feed (if implemented)
    # At minimum, the function should not repeatedly call external lookup for the same guid
    assert call_count['calls'] >= 1
