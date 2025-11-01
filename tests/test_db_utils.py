import hashlib
import sqlite3

from ednews.db import utils


def test_compute_url_hash_none_and_deterministic():
    # None/empty returns None
    assert utils.compute_url_hash(None) is None
    assert utils.compute_url_hash("") is None

    # Deterministic SHA-256
    url = "https://example.com/foo"
    expected = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert utils.compute_url_hash(url) == expected


def _create_items_table(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            link TEXT,
            url_hash TEXT,
            published TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()


def test_backfill_and_resolve_collisions():
    conn = sqlite3.connect(":memory:")
    _create_items_table(conn)
    cur = conn.cursor()

    # Two rows with same link and missing url_hash. One has doi, the other has an earlier published timestamp.
    link = "https://example.com/article"
    cur.execute(
        "INSERT INTO items (doi, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?)",
        ("doi:1", link, None, "2020-01-02T00:00:00", "2020-01-02T01:00:00"),
    )
    cur.execute(
        "INSERT INTO items (doi, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?)",
        (None, link, None, "2020-01-01T00:00:00", "2020-01-01T01:00:00"),
    )
    conn.commit()

    # Backfill should set url_hash for both rows and return updated count 2
    updated, collisions = utils.backfill_missing_url_hash(conn)
    assert updated == 2
    assert isinstance(collisions, list)

    # Now duplicates exist for the computed hash
    h = utils.compute_url_hash(link)
    assert h is not None

    # Resolve collisions: should delete one duplicate and merge doi into the kept row
    resolved, unresolved = utils.resolve_url_hash_collisions(conn, [h])
    assert resolved == 1
    assert unresolved == []

    # Only one row should remain, with doi merged (kept row was the earlier published row, which lacked doi)
    cur.execute("SELECT COUNT(*) FROM items")
    assert cur.fetchone()[0] == 1

    cur.execute("SELECT doi, published FROM items")
    doi, published = cur.fetchone()
    assert doi == "doi:1"
    assert published == "2020-01-01T00:00:00"


def test_missing_timestamps_fallback_to_id_order():
    conn = sqlite3.connect(":memory:")
    _create_items_table(conn)
    cur = conn.cursor()

    # Two rows with same link, no published or fetched_at values. Order should fall back to id
    link = "https://example.com/no-times"
    cur.execute("INSERT INTO items (doi, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?)",
                ("doi:a", link, None, None, None))
    cur.execute("INSERT INTO items (doi, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?)",
                ("doi:b", link, None, None, None))
    conn.commit()

    updated, collisions = utils.backfill_missing_url_hash(conn)
    assert updated == 2

    h = utils.compute_url_hash(link)
    resolved, unresolved = utils.resolve_url_hash_collisions(conn, [h])
    # Keep the row with the lower id (first inserted), so doi:b should be removed and doi:a kept
    assert resolved == 1
    assert unresolved == []
    cur.execute("SELECT doi FROM items")
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "doi:a"


def test_many_duplicates_merge_and_delete():
    conn = sqlite3.connect(":memory:")
    _create_items_table(conn)
    cur = conn.cursor()

    link = "https://example.com/many"
    # Create five rows with same link. Only one has doi, another has published.
    cur.executemany(
        "INSERT INTO items (doi, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?)",
        [
            (None, link, None, None, "2020-01-05T00:00:00"),
            (None, link, None, None, "2020-01-04T00:00:00"),
            ("doi:many", link, None, None, None),
            (None, link, None, "2020-01-03T00:00:00", None),
            (None, link, None, None, None),
        ],
    )
    conn.commit()

    updated, collisions = utils.backfill_missing_url_hash(conn)
    assert updated == 5

    h = utils.compute_url_hash(link)
    resolved, unresolved = utils.resolve_url_hash_collisions(conn, [h])
    # Should resolve 4 duplicates (keep 1) and merge doi/published into kept row
    assert resolved == 4
    assert unresolved == []

    cur.execute("SELECT COUNT(*), doi, published FROM items")
    cnt, doi, pub = cur.fetchone()
    assert cnt == 1
    assert doi == "doi:many"
    assert pub == "2020-01-03T00:00:00"
