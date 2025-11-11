"""Cleanup helpers extracted from maintenance.py."""

import logging, sqlite3
from datetime import datetime, timezone, timedelta

logger = logging.getLogger("ednews.manage_db.maintenance.cleanup")


def cleanup_empty_articles(
    conn: sqlite3.Connection, older_than_days: int | None = None
) -> int:
    try:
        cur = conn.cursor()
        params = []
        where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
        if older_than_days is not None:
            cutoff = (
                datetime.now(timezone.utc) - timedelta(days=int(older_than_days))
            ).isoformat()
            where_clauses.append(
                "(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)"
            )
            params.extend([cutoff, cutoff])
        where_sql = " AND ".join(where_clauses)
        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(params))
        deleted = cur.rowcount if hasattr(cur, "rowcount") else None
        conn.commit()
        logger.info(
            "cleanup_empty_articles deleted %s rows (older_than_days=%s)",
            deleted,
            older_than_days,
        )
        return deleted or 0
    except Exception:
        logger.exception("cleanup_empty_articles failed")
        return 0


def cleanup_filtered_titles(
    conn: sqlite3.Connection, filters: list | None = None, dry_run: bool = False
) -> int:
    try:
        from ednews import config

        try:
            if filters is None:
                filters = getattr(config, "TITLE_FILTERS", [])
        except Exception:
            filters = filters or []
        if not filters:
            logger.debug(
                "cleanup_filtered_titles: no filters configured; nothing to do"
            )
            return 0
        norm_filters = [str(f).strip().lower() for f in filters if f]
        if not norm_filters:
            return 0
        cur = conn.cursor()
        clauses = []
        params = []
        for _ in norm_filters:
            clauses.append("LOWER(TRIM(COALESCE(title, ''))) = ?")
        where_sql = " OR ".join(clauses)
        if dry_run:
            cur.execute(
                f"SELECT COUNT(1) FROM articles WHERE {where_sql}", tuple(norm_filters)
            )
            row = cur.fetchone()
            count = row[0] if row and row[0] else 0
            logger.info("cleanup_filtered_titles dry-run would delete %s rows", count)
            return count
        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(norm_filters))
        deleted = cur.rowcount if hasattr(cur, "rowcount") else None
        conn.commit()
        logger.info("cleanup_filtered_titles deleted %s rows", deleted)
        return deleted or 0
    except Exception:
        logger.exception("cleanup_filtered_titles failed")
        return 0


__all__ = ["cleanup_empty_articles", "cleanup_filtered_titles"]
