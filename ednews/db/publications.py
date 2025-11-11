"""Publication helpers split from ednews.db.__init__."""
import logging, sqlite3
logger = logging.getLogger("ednews.db.publications")

def upsert_publication(
    conn,
    feed_id: str | None,
    publication_id: str | None,
    feed_title: str | None,
    issn: str | None,
):
    if not publication_id and not feed_id:
        logger.debug("upsert_publication called without publication_id or feed_id; skipping")
        return False
    try:
        cur = conn.cursor()
        if publication_id and issn:
            cur.execute(
                """
                INSERT INTO publications (feed_id, publication_id, feed_title, issn)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(publication_id, issn) DO UPDATE SET
                    feed_id = COALESCE(excluded.feed_id, publications.feed_id),
                    feed_title = COALESCE(excluded.feed_title, publications.feed_title)
                """,
                (feed_id, publication_id, feed_title, issn),
            )
        else:
            cur.execute(
                "INSERT OR REPLACE INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)",
                (feed_id, publication_id or feed_id, feed_title, issn or ""),
            )
        conn.commit()
        logger.debug(
            "upsert_publication succeeded for feed_id=%s publication_id=%s issn=%s",
            feed_id,
            publication_id,
            issn,
        )
        return True
    except Exception:
        logger.exception(
            "upsert_publication failed for feed_id=%s publication_id=%s issn=%s",
            feed_id,
            publication_id,
            issn,
        )
        return False

def sync_publications_from_feeds(conn, feeds_list) -> int:
    try:
        from .maintenance_sync import sync_publications_from_feeds as _sync
        return _sync(conn, feeds_list)
    except Exception:
        logger.exception("Falling back to local sync_publications_from_feeds due to import error")
        if not feeds_list:
            return 0
        count = 0
        for item in feeds_list:
            try:
                key = item[0] if len(item) > 0 else None
                title = item[1] if len(item) > 1 else None
                pub_id = item[3] if len(item) > 3 else None
                issn = item[4] if len(item) > 4 else None
                ok = upsert_publication(conn, key, pub_id, title, issn)
                if ok:
                    count += 1
            except Exception:
                logger.exception("Failed to sync publication for feed item: %s", item)
                continue
        logger.info("Synchronized %d publications from feeds", count)
        return count

__all__ = ["upsert_publication", "sync_publications_from_feeds"]
