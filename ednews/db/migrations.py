"""Schema migrations for ednews DB (extracted from manage_db).
"""
import logging

logger = logging.getLogger("ednews.manage_db.migrations")


def create_combined_view(conn):
    # For compatibility, provide the view creation function here
    logger.info("Creating combined_articles view (migrations)")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE VIEW IF NOT EXISTS combined_articles AS
        SELECT
            articles.doi AS doi,
            COALESCE(articles.title, '') AS title,
            ('https://doi.org/' || articles.doi) AS link,
            COALESCE(publications.feed_title, feeds.feed_title, '') AS feed_title,
            COALESCE(articles.abstract, '') AS content,
            COALESCE(articles.published, articles.fetched_at) AS published,
            COALESCE(articles.authors, '') AS authors
        FROM articles
            LEFT JOIN publications on publications.feed_id = articles.feed_id
            LEFT JOIN publications as feeds on feeds.feed_id = articles.feed_id
        WHERE articles.doi IS NOT NULL
        """,
    )
    conn.commit()
    logger.debug("combined_articles view created")


def migrate_add_items_url_hash(conn):
    import hashlib
    result = {"added_column": False, "updated_rows": 0, "index_created": False, "collisions": []}
    cur = conn.cursor()
    try:
        # Check if url_hash column exists
        cur.execute("PRAGMA table_info(items)")
        cols = cur.fetchall()
        col_names = [c[1] for c in cols]
        if 'url_hash' not in col_names:
            try:
                cur.execute("ALTER TABLE items ADD COLUMN url_hash TEXT")
                conn.commit()
                result['added_column'] = True
                logger.info("Added url_hash column to items table")
            except Exception:
                logger.exception("Failed to add url_hash column to items")
        # Backfill: compute hash for rows missing url_hash but with a link
        cur.execute("SELECT id, link, url_hash FROM items WHERE (url_hash IS NULL OR url_hash = '') AND link IS NOT NULL AND link != ''")
        rows = cur.fetchall()
        updated = 0
        for r in rows:
            rid, link, uh = r
            try:
                h = hashlib.sha256(str(link).encode('utf-8')).hexdigest()
                try:
                    cur.execute("UPDATE items SET url_hash = ? WHERE id = ?", (h, rid))
                    updated += 1
                except Exception:
                    # Likely a unique constraint collision; record it for reporting
                    logger.debug("Failed to update url_hash for id=%s link=%s: possible duplicate", rid, link)
                    result['collisions'].append(h)
            except Exception:
                logger.exception("Failed to compute url_hash for id=%s link=%s", rid, link)
        conn.commit()
        result['updated_rows'] = updated

        # Attempt to create a unique index on url_hash if it doesn't exist
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_hash ON items(url_hash)")
            conn.commit()
            result['index_created'] = True
            logger.info("Created unique index idx_items_url_hash on items.url_hash")
        except Exception:
            logger.exception("Failed to create unique index on items.url_hash; attempting to detect and resolve duplicates")
            # Detect collisions: url_hash values that appear more than once
            try:
                cur.execute("SELECT url_hash FROM items WHERE url_hash IS NOT NULL GROUP BY url_hash HAVING COUNT(*) > 1")
                dupes = [d[0] for d in cur.fetchall() if d and d[0]]
            except Exception:
                logger.exception("Failed to enumerate duplicate url_hash values")
                dupes = []

            resolved = 0
            unresolved = []
            for h in dupes:
                try:
                    # Fetch candidate rows for this hash and pick the one with the earliest published/fetched_at
                    # Use COALESCE(NULLIF(published,''), NULLIF(fetched_at,''), '9999-12-31T23:59:59') for ordering
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
                    # Merge DOI/published from other rows if missing on kept row
                    for other in rows_h[1:]:
                        oid, od_doi, od_link, od_pub, od_fetched = other
                        try:
                            if (not keep_doi) and od_doi:
                                cur.execute("UPDATE items SET doi = ? WHERE id = ?", (od_doi, keep_id))
                                keep_doi = od_doi
                            # Update published if missing
                            if (not keep_pub or keep_pub == '') and (od_pub and od_pub != ''):
                                cur.execute("UPDATE items SET published = ? WHERE id = ?", (od_pub, keep_id))
                                keep_pub = od_pub
                            # Delete the duplicate row
                            cur.execute("DELETE FROM items WHERE id = ?", (oid,))
                            resolved += 1
                        except Exception:
                            logger.exception("Failed to resolve duplicate row id=%s for url_hash=%s", oid, h)
                    conn.commit()
                except Exception:
                    logger.exception("Failed to process duplicates for url_hash=%s", h)
                    unresolved.append(h)

            result['collisions'] = unresolved
            result['resolved_duplicates'] = resolved
            # After attempting resolution, try to create the unique index again
            try:
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_hash ON items(url_hash)")
                conn.commit()
                result['index_created'] = True
                logger.info("Created unique index idx_items_url_hash on items.url_hash after resolving duplicates")
            except Exception:
                logger.exception("Failed to create unique index on items.url_hash after attempted resolution")
    except Exception:
        logger.exception("Migration migrate_add_items_url_hash failed")
    return result


def migrate_db(conn):
    # Run all migrations. Keep behavior as previous migrate_db wrapper.
    logger.info("migrate_db: running migrations (migrations module)")
    try:
        res = migrate_add_items_url_hash(conn)
        logger.info("migrate_add_items_url_hash: %s", res)
        if res and isinstance(res, dict) and res.get('collisions'):
            return False
        return True
    except Exception:
        logger.exception("migrate_db failed")
        return False
