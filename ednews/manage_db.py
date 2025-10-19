"""Compatibility shim to re-export maintenance helpers from the new location.

This file keeps the old import path `ednews.manage_db` working by importing
everything from `ednews.db.manage_db`.
"""

from .db.manage_db import *  # re-export public names



def vacuum_db(conn: sqlite3.Connection):
    """Run VACUUM to defragment the SQLite database file."""
    try:
        cur = conn.cursor()
        cur.execute("VACUUM")
        conn.commit()
        logger.info("Database vacuumed")
        return True
    except Exception:
        logger.exception("VACUUM failed")
        return False


def cleanup_empty_articles(conn: sqlite3.Connection, older_than_days: int | None = None) -> int:
    """Delete articles that have no title and no abstract.

    If `older_than_days` is provided, only delete articles whose
    `fetched_at` or `published` timestamp is older than the cutoff.

    Returns the number of rows deleted.
    """
    try:
        cur = conn.cursor()
        params = []
        where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
        if older_than_days is not None:
            # compute cutoff ISO timestamp
            cutoff = (datetime.now(timezone.utc) - timedelta(days=int(older_than_days))).isoformat()
            where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
            params.extend([cutoff, cutoff])
        where_sql = " AND ".join(where_clauses)
        # Use DELETE ... WHERE ... and return rowcount
        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(params))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        conn.commit()
        logger.info("cleanup_empty_articles deleted %s rows (older_than_days=%s)", deleted, older_than_days)
        return deleted or 0
    except Exception:
        logger.exception("cleanup_empty_articles failed")
        return 0


def migrate_db(conn: sqlite3.Connection):
    """Placeholder for schema migrations.

    Currently a no-op. Add migration steps here as needed. Returns True
    if migrations applied or no-op succeeded.
    """
    logger.info("migrate_db: no migrations to apply")
    return True
