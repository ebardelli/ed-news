import sqlite3
from ednews.db import rematch_publication_dois, init_db


def test_rematch_deletes_conflicting_article_row(monkeypatch):
    # Setup DB
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    cur = conn.cursor()
    # Insert publication mapping for feed f1
    cur.execute(
        "INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)",
        ("f1", "pub1", "Pub 1", ""),
    )
    # Insert an article that already has the DOI
    cur.execute(
        "INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("10.9999/existing", "Existing", "f1", "pub1", "2025-01-01T00:00:00Z"),
    )
    # Insert another article (to be updated) which lacks a DOI but will match the same title
    cur.execute(
        "INSERT INTO articles (title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?)",
        ("Conflict Title", "f1", "", "2025-01-02T00:00:00Z"),
    )
    # Make sure two article rows exist
    cur.execute("SELECT COUNT(1) FROM articles WHERE feed_id = ?", ("f1",))
    assert cur.fetchone()[0] == 2
    conn.commit()

    # Monkeypatch crossref to return the DOI that already exists for this title
    def fake_query(title, preferred_publication_id=None):
        if title == "Conflict Title":
            return "10.9999/existing"
        return None

    import ednews.crossref as cr_mod

    monkeypatch.setattr(cr_mod, 'query_crossref_doi_by_title', fake_query, raising=False)

    # Run rematch targeting only articles
    res = rematch_publication_dois(conn, publication_id=None, feed_keys=['f1'], dry_run=False, only_articles=True, only_missing=True)

    # After rematch, the conflicting article should have been deleted
    cur.execute("SELECT COUNT(1) FROM articles WHERE feed_id = ?", ("f1",))
    remaining = cur.fetchone()[0]
    assert remaining == 1, f"expected 1 remaining article, got {remaining}"

    # The results should have incremented removed_orphan_articles
    assert res.get('removed_orphan_articles', 0) >= 1
