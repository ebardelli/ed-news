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
    from .utils import compute_url_hash, backfill_missing_url_hash, resolve_url_hash_collisions

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
        # Backfill missing url_hash values
        updated, collisions = backfill_missing_url_hash(conn)
        result['updated_rows'] = updated
        # Record any collisions that occurred during the backfill
        if collisions:
            result['collisions'].extend(collisions)

        # Attempt to create a unique index on url_hash if it doesn't exist
        try:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_hash ON items(url_hash)")
            conn.commit()
            result['index_created'] = True
            logger.info("Created unique index idx_items_url_hash on items.url_hash")
        except Exception:
            logger.exception("Failed to create unique index on items.url_hash; attempting to detect and resolve duplicates")
            # Resolve duplicates using helpers
            resolved, unresolved = resolve_url_hash_collisions(conn)
            result['resolved_duplicates'] = resolved
            result['collisions'] = unresolved
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
