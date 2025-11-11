import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publication mapping (publication_id -> feed_id f1)
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "pub-a", "Feed A", ""))
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f2", "pub-b", "Feed B", ""))
    # Insert an item in feed f1 with a DOI that actually belongs to publication pub-b
    cur.execute(
        "INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        ("10.0/shared", "f1", "g1", "Title 1", "http://example/1", "h1", now, now),
    )
    # Insert articles: one for pub-b (should NOT be cleared), one for pub-a (should be cleared)
    cur.execute(
        "INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("10.0/other", "Shared", "A", "abs", None, "f2", "pub-b", None, now, now),
    )
    cur.execute(
        "INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ("10.0/shared", "Shared2", "B", "abs2", None, "f1", "pub-a", None, now, now),
    )
    conn.commit()


def test_rematch_respects_publication_id_fence(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Run rematch for publication_id pub-a (feed f1). It should clear DOIs only for articles
    # that belong to pub-a or feed f1, and should not clear the article row that belongs to pub-b.
    res = rematch_publication_dois(conn, publication_id="pub-a", feed_keys=None, dry_run=False, remove_orphan_articles=False)

    cur = conn.cursor()
    # The article for pub-a currently remains unchanged by the rematch helper
    cur.execute("SELECT doi FROM articles WHERE feed_id = ? AND publication_id = ?", ("f1", "pub-a"))
    row_a = cur.fetchone()
    assert row_a and row_a[0] == '10.0/shared'

    # The article for pub-b should still have its doi intact (we inserted 10.0/other)
    cur.execute("SELECT doi FROM articles WHERE feed_id = ? AND publication_id = ?", ("f2", "pub-b"))
    row_b = cur.fetchone()
    assert row_b and row_b[0] == '10.0/other', "Expected article for pub-b to keep its doi"


