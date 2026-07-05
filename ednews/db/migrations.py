"""Schema migrations for ednews DB (extracted from manage_db)."""

import logging

logger = logging.getLogger("ednews.manage_db.migrations")


_COMBINED_VIEW_SQL = """
        CREATE VIEW IF NOT EXISTS combined_articles AS
        SELECT
            articles.doi AS doi,
            COALESCE(articles.title, '') AS title,
            CASE WHEN articles.doi LIKE 'https://%' THEN articles.doi ELSE ('https://doi.org/' || articles.doi) END AS link,
            COALESCE(publications.feed_title, feeds.feed_title, '') AS feed_title,
            COALESCE(articles.abstract, '') AS content,
            COALESCE(articles.published, articles.fetched_at) AS published,
            COALESCE(articles.authors, '') AS authors
        FROM articles
            LEFT JOIN publications on publications.feed_id = articles.feed_id
            LEFT JOIN publications as feeds on feeds.feed_id = articles.feed_id
        WHERE articles.doi IS NOT NULL
"""


def create_combined_view(conn):
    # For compatibility, provide the view creation function here
    logger.info("Creating combined_articles view (migrations)")
    cur = conn.cursor()
    cur.execute(_COMBINED_VIEW_SQL)
    conn.commit()
    logger.debug("combined_articles view created")


def migrate_update_combined_view(conn):
    """Drop and recreate combined_articles to pick up the URL-DOI link fix.

    Also patches any existing edpolicyinca/* synthetic DOI records to use
    the full article URL so their links resolve correctly.
    """
    cur = conn.cursor()
    try:
        cur.execute("DROP VIEW IF EXISTS combined_articles")
        cur.execute(_COMBINED_VIEW_SQL.replace("IF NOT EXISTS ", ""))
        conn.commit()
        logger.info("Recreated combined_articles view with URL-DOI link fix")
    except Exception:
        logger.exception("Failed to recreate combined_articles view")

    # Patch existing edpolicyinca/* synthetic DOI records.
    # The new format stores the full URL as DOI, so find the matching items
    # row to get the real link and update both tables.
    try:
        cur.execute(
            """
            UPDATE articles
            SET doi = (
                SELECT items.link FROM items
                WHERE items.doi = articles.doi
                LIMIT 1
            )
            WHERE articles.doi LIKE 'edpolicyinca/%'
              AND EXISTS (
                SELECT 1 FROM items WHERE items.doi = articles.doi
              )
            """
        )
        patched_articles = cur.rowcount or 0
        cur.execute(
            "UPDATE items SET doi = link WHERE doi LIKE 'edpolicyinca/%'"
        )
        patched_items = cur.rowcount or 0
        conn.commit()
        if patched_articles or patched_items:
            logger.info(
                "Patched %d article rows and %d item rows from edpolicyinca/ to full-URL DOIs",
                patched_articles,
                patched_items,
            )
    except Exception:
        logger.exception("Failed to patch edpolicyinca synthetic DOI records")


def migrate_add_items_url_hash(conn):
    from .utils import (
        compute_url_hash,
        backfill_missing_url_hash,
        resolve_url_hash_collisions,
    )

    result = {
        "added_column": False,
        "updated_rows": 0,
        "index_created": False,
        "collisions": [],
    }
    cur = conn.cursor()
    try:
        # Check if url_hash column exists
        cur.execute("PRAGMA table_info(items)")
        cols = cur.fetchall()
        col_names = [c[1] for c in cols]
        if "url_hash" not in col_names:
            try:
                cur.execute("ALTER TABLE items ADD COLUMN url_hash TEXT")
                conn.commit()
                result["added_column"] = True
                logger.info("Added url_hash column to items table")
            except Exception:
                logger.exception("Failed to add url_hash column to items")
        # Backfill missing url_hash values
        updated, collisions = backfill_missing_url_hash(conn)
        result["updated_rows"] = updated
        # Record any collisions that occurred during the backfill
        if collisions:
            result["collisions"].extend(collisions)

        # Attempt to create a unique index on url_hash if it doesn't exist
        try:
            cur.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_hash ON items(url_hash)"
            )
            conn.commit()
            result["index_created"] = True
            logger.info("Created unique index idx_items_url_hash on items.url_hash")
        except Exception:
            logger.exception(
                "Failed to create unique index on items.url_hash; attempting to detect and resolve duplicates"
            )
            # Resolve duplicates using helpers
            resolved, unresolved = resolve_url_hash_collisions(conn)
            result["resolved_duplicates"] = resolved
            result["collisions"] = unresolved
            # After attempting resolution, try to create the unique index again
            try:
                cur.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_hash ON items(url_hash)"
                )
                conn.commit()
                result["index_created"] = True
                logger.info(
                    "Created unique index idx_items_url_hash on items.url_hash after resolving duplicates"
                )
            except Exception:
                logger.exception(
                    "Failed to create unique index on items.url_hash after attempted resolution"
                )
    except Exception:
        logger.exception("Migration migrate_add_items_url_hash failed")
    return result


def migrate_add_items_authors(conn):
    """Add authors column to items table if not present."""
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(items)")
        cols = [c[1] for c in cur.fetchall()]
        if "authors" not in cols:
            cur.execute("ALTER TABLE items ADD COLUMN authors TEXT")
            conn.commit()
            logger.info("Added authors column to items table")
    except Exception:
        logger.exception("migrate_add_items_authors failed")


def migrate_normalize_published_dates(conn):
    """Normalize all published values to YYYY-MM-DD across items, articles, headlines."""
    from .utils import normalize_date_ymd
    import re
    _ymd = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    cur = conn.cursor()
    totals = {}
    for table in ("items", "articles", "headlines"):
        try:
            cur.execute(
                f"SELECT id, published FROM {table}"
                f" WHERE published IS NOT NULL AND published != ''"
                f" AND published NOT GLOB '????-??-??'"
            )
            rows = cur.fetchall()
            count = 0
            for row_id, raw in rows:
                normalized = normalize_date_ymd(raw)
                if normalized and normalized != raw:
                    cur.execute(
                        f"UPDATE {table} SET published = ? WHERE id = ?",
                        (normalized, row_id),
                    )
                    count += 1
            conn.commit()
            totals[table] = count
            if count:
                logger.info("normalize_published_dates: fixed %d rows in %s", count, table)
        except Exception:
            logger.exception("normalize_published_dates failed for table %s", table)
    return totals


def migrate_db(conn):
    # Run all migrations. Keep behavior as previous migrate_db wrapper.
    logger.info("migrate_db: running migrations (migrations module)")
    try:
        res = migrate_add_items_url_hash(conn)
        logger.info("migrate_add_items_url_hash: %s", res)
        if res and isinstance(res, dict) and res.get("collisions"):
            return False
        migrate_update_combined_view(conn)
        migrate_add_items_authors(conn)
        totals = migrate_normalize_published_dates(conn)
        logger.info("migrate_normalize_published_dates: %s", totals)
        return True
    except Exception:
        logger.exception("migrate_db failed")
        return False
