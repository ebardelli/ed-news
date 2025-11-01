"""Utility helpers for DB migrations: url hashing and duplicate resolution.

These functions are small, pure where possible, and easy to unit-test.
"""
import hashlib
import logging

logger = logging.getLogger("ednews.db.utils")


def compute_url_hash(link: str) -> str | None:
    """Compute a deterministic hex SHA-256 hash for a URL string.

    Returns None for falsy links.
    """
    if not link:
        return None
    try:
        return hashlib.sha256(str(link).encode("utf-8")).hexdigest()
    except Exception:
        logger.exception("compute_url_hash failed for link=%r", link)
        return None


def backfill_missing_url_hash(conn) -> tuple[int, list[str]]:
    """Backfill url_hash for rows in `items` that have a link but missing url_hash.

    Returns (updated_count, collisions)
    where collisions is a list of url_hash values that could not be written due to
    unique-constraint collisions.
    """
    cur = conn.cursor()
    cur.execute(
        "SELECT id, link FROM items WHERE (url_hash IS NULL OR url_hash = '') AND link IS NOT NULL AND link != ''"
    )
    rows = cur.fetchall()
    updated = 0
    collisions: list[str] = []
    for rid, link in rows:
        try:
            h = compute_url_hash(link)
            if not h:
                continue
            try:
                cur.execute("UPDATE items SET url_hash = ? WHERE id = ?", (h, rid))
                updated += 1
            except Exception:
                # Could be unique constraint - record the hash for resolution
                logger.debug("backfill: could not set url_hash for id=%s link=%s", rid, link)
                collisions.append(h)
        except Exception:
            logger.exception("backfill_missing_url_hash failed for id=%s link=%s", rid, link)
    conn.commit()
    return updated, collisions


def resolve_url_hash_collisions(conn, url_hashes: list[str] | None = None) -> tuple[int, list[str]]:
    """Resolve duplicate rows that share the same url_hash.

    If `url_hashes` is None, detect all url_hash values that occur more than once.
    For each group, keep the row with the earliest COALESCE(published, fetched_at)
    (ties broken by id), merge doi/published into the kept row when missing, and
    delete the other rows.

    Returns (resolved_count, unresolved_hashes).
    """
    cur = conn.cursor()
    # Detect duplicates if not provided
    if url_hashes is None:
        try:
            cur.execute("SELECT url_hash FROM items WHERE url_hash IS NOT NULL GROUP BY url_hash HAVING COUNT(*) > 1")
            url_hashes = [r[0] for r in cur.fetchall() if r and r[0]]
        except Exception:
            logger.exception("Failed to enumerate duplicate url_hash values")
            url_hashes = []

    resolved = 0
    unresolved: list[str] = []
    for h in url_hashes:
        try:
            cur.execute(
                """
                SELECT id, doi, link, published, fetched_at FROM items
                WHERE url_hash = ?
                ORDER BY COALESCE(NULLIF(published, ''), NULLIF(fetched_at, ''), '9999-12-31T23:59:59') ASC, id ASC
                """,
                (h,),
            )
            rows_h = cur.fetchall()
            if not rows_h or len(rows_h) < 2:
                continue
            keep = rows_h[0]
            keep_id, keep_doi, keep_link, keep_pub, keep_fetched = keep
            for other in rows_h[1:]:
                oid, od_doi, od_link, od_pub, od_fetched = other
                try:
                    if (not keep_doi) and od_doi:
                        cur.execute("UPDATE items SET doi = ? WHERE id = ?", (od_doi, keep_id))
                        keep_doi = od_doi
                    if (not keep_pub or keep_pub == '') and (od_pub and od_pub != ''):
                        cur.execute("UPDATE items SET published = ? WHERE id = ?", (od_pub, keep_id))
                        keep_pub = od_pub
                    cur.execute("DELETE FROM items WHERE id = ?", (oid,))
                    resolved += 1
                except Exception:
                    logger.exception("Failed to resolve duplicate row id=%s for url_hash=%s", oid, h)
            conn.commit()
        except Exception:
            logger.exception("Failed to process duplicates for url_hash=%s", h)
            unresolved.append(h)

    return resolved, unresolved
