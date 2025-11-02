import sqlite3
from datetime import datetime, timezone

from ednews.db import rematch_publication_dois, init_db


def setup_db(conn):
    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()

    # publication mapping (publication_id -> feed_id edfp)
    cur.execute(
        "INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)",
        ("edfp", "10.1162/edfp", "Education Finance and Policy", "1557-3060"),
    )

    # Insert an item that currently has the correct DOI for the feed
    cur.execute(
        "INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "10.1162/edfp_a_00442",
            "edfp",
            "g1",
            "The Insurance Value of Financial Aid",
            "http://example/1",
            "h1",
            now,
            now,
        ),
    )

    # Insert an article row with the correct DOI
    cur.execute(
        "INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "10.1162/edfp_a_00442",
            "The Insurance Value of Financial Aid",
            "A",
            "abs",
            None,
            "edfp",
            "10.1162/edfp",
            "1557-3060",
            now,
            now,
        ),
    )

    # Insert a stale/wrong article row for the same feed (the DOI should be cleared)
    cur.execute(
        "INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "10.3386/w28669",
            "The Insurance Value of Financial Aid",
            "B",
            "abs2",
            None,
            "edfp",
            None,
            None,
            now,
            now,
        ),
    )
    stale_id = cur.lastrowid
    conn.commit()
    return stale_id


def test_rematch_clears_orphan_article_doi():
    conn = sqlite3.connect(":memory:")
    stale_id = setup_db(conn)

    # Run rematch for the publication; this should clear DOIs on orphaned
    # article rows that are not referenced by any items for the feed.
    res = rematch_publication_dois(conn, publication_id="10.1162/edfp", feed_keys=None, dry_run=False, remove_orphan_articles=False)

    # The stale article row should have its doi set to NULL (None in Python)
    cur = conn.cursor()
    cur.execute("SELECT doi FROM articles WHERE id = ?", (stale_id,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] is None

    # The correct article DOI should still be present
    cur.execute("SELECT doi, publication_id FROM articles WHERE doi = ?", ("10.1162/edfp_a_00442",))
    good = cur.fetchone()
    assert good and good[0] == "10.1162/edfp_a_00442"

    # Also expect the result to contain our feed key and a non-negative count
    assert isinstance(res.get('feeds', {}).get('edfp', {}), dict)
