"""Sync helpers (publications and articles) extracted from maintenance.py."""

import logging, sqlite3

logger = logging.getLogger("ednews.manage_db.maintenance.sync")


def sync_publications_from_feeds(conn, feeds_list) -> int:
    if not feeds_list:
        return 0
    count = 0
    from . import upsert_publication

    for item in feeds_list:
        try:
            key = item[0] if len(item) > 0 else None
            title = item[1] if len(item) > 1 else None
            pub_id = item[3] if len(item) > 3 else None
            issn = item[4] if len(item) > 4 else None
            cur = conn.cursor()
            try:
                ok = upsert_publication(conn, key, pub_id, title, issn)
                if ok:
                    try:
                        cur.execute(
                            "DELETE FROM publications WHERE feed_id = ? AND (publication_id != ? OR issn != ?)",
                            (key, pub_id or "", issn or ""),
                        )
                        conn.commit()
                    except Exception:
                        logger.exception(
                            "Failed to cleanup old publication rows for feed_id=%s", key
                        )
                    count += 1
            except Exception:
                logger.exception("Failed to sync publication for feed item: %s", item)
        except Exception:
            logger.exception("Failed to sync publication for feed item: %s", item)
            continue
    logger.info("Synchronized %d publications from feeds", count)
    return count


def sync_articles_from_items(
    conn: sqlite3.Connection,
    feed_keys: list | None = None,
    publication_id: str | None = None,
    dry_run: bool = False,
) -> dict:
    results = {"feeds": {}, "total_created": 0, "total_existing": 0}
    cur = conn.cursor()
    keys: list[str] = []
    if feed_keys:
        keys = [k for k in feed_keys if k]
    if publication_id and not keys:
        try:
            cur.execute(
                "SELECT feed_id FROM publications WHERE publication_id = ?",
                (publication_id,),
            )
            rows = cur.fetchall()
            keys = [r[0] for r in rows if r and r[0]]
        except Exception:
            logger.exception(
                "Failed to lookup feeds for publication_id=%s", publication_id
            )
    if not keys:
        try:
            cur.execute(
                "SELECT DISTINCT feed_id FROM publications WHERE COALESCE(feed_id, '') != ''"
            )
            rows = cur.fetchall()
            keys = [r[0] for r in rows if r and r[0]]
        except Exception:
            keys = []
        if not keys:
            try:
                cur.execute(
                    "SELECT DISTINCT feed_id FROM items WHERE COALESCE(feed_id, '') != ''"
                )
                rows = cur.fetchall()
                keys = [r[0] for r in rows if r and r[0]]
            except Exception:
                keys = []
    if not keys:
        return results
    from . import ensure_article_row

    for fk in keys:
        created = 0
        existing = 0
        try:
            cur.execute(
                "SELECT DISTINCT doi, title FROM items WHERE feed_id = ? AND COALESCE(doi,'') != ''",
                (fk,),
            )
            rows = cur.fetchall()
            for doi, title in rows:
                if not doi:
                    continue
                try:
                    cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
                    if cur.fetchone():
                        existing += 1
                        continue
                    if dry_run:
                        created += 1
                        continue
                    aid = ensure_article_row(
                        conn,
                        doi,
                        title=title,
                        feed_id=fk,
                        publication_id=publication_id,
                    )
                    if aid:
                        created += 1
                except Exception:
                    logger.exception(
                        "Failed to ensure article for doi=%s feed=%s", doi, fk
                    )
            results["feeds"][fk] = {"created": created, "existing": existing}
            results["total_created"] += created
            results["total_existing"] += existing
        except Exception:
            logger.exception("Failed to sync articles for feed=%s", fk)
            continue
    return results


__all__ = ["sync_publications_from_feeds", "sync_articles_from_items"]
