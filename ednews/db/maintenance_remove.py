"""Remove helper extracted from maintenance.py."""

import logging, sqlite3

logger = logging.getLogger("ednews.manage_db.maintenance.remove")


def remove_feed_articles(
    conn: sqlite3.Connection,
    feed_keys: list | None = None,
    publication_id: str | None = None,
    dry_run: bool = False,
) -> int:
    try:
        cur = conn.cursor()
        try:
            from ednews import feeds as feeds_mod

            _feeds_list = {
                f[0]: ((f[3] if len(f) > 3 else None), (f[4] if len(f) > 4 else None))
                for f in feeds_mod.load_feeds()
            }
        except Exception:
            _feeds_list = {}
        if publication_id and not feed_keys:
            if dry_run:
                try:
                    cur.execute(
                        "SELECT COUNT(1) FROM articles WHERE publication_id = ?",
                        (publication_id,),
                    )
                    row = cur.fetchone()
                    cnt = row[0] if row and row[0] else 0
                    logger.info(
                        "remove_feed_articles dry-run would delete %d rows with publication_id=%s",
                        cnt,
                        publication_id,
                    )
                    return cnt
                except Exception:
                    logger.exception(
                        "Failed to count articles for publication_id=%s", publication_id
                    )
                    return 0
            try:
                cur.execute(
                    "DELETE FROM articles WHERE publication_id = ?", (publication_id,)
                )
                n = cur.rowcount if hasattr(cur, "rowcount") else None
                if n:
                    conn.commit()
                logger.info(
                    "remove_feed_articles deleted %s rows with publication_id=%s",
                    n,
                    publication_id,
                )
                return n or 0
            except Exception:
                logger.exception(
                    "Failed to delete articles for publication_id=%s", publication_id
                )
                return 0
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
            return 0
        total_deleted = 0
        for fk in keys:
            try:
                expected_pub = publication_id
                feed_issn = None
                if not expected_pub:
                    if fk in _feeds_list:
                        expected_pub = _feeds_list[fk][0]
                        feed_issn = _feeds_list[fk][1]
                        config_present = True
                    else:
                        expected_pub = None
                        config_present = False
                if expected_pub:
                    pub_param = expected_pub or ""
                    try:
                        if dry_run:
                            cur.execute(
                                "SELECT COUNT(1) FROM articles WHERE feed_id = ? AND COALESCE(doi,'') != ''",
                                (fk,),
                            )
                            row = cur.fetchone()
                            total_with_doi = row[0] if row and row[0] else 0
                            cur.execute(
                                "SELECT COUNT(1) FROM articles WHERE feed_id = ? AND COALESCE(doi,'') != '' AND NOT (lower(doi) LIKE lower(?) || '%' OR lower(doi) LIKE '%/' || lower(?) || '%')",
                                (fk, pub_param, pub_param),
                            )
                            row = cur.fetchone()
                            to_delete = row[0] if row and row[0] else 0
                            total_deleted += to_delete
                        else:
                            cur.execute(
                                "DELETE FROM articles WHERE feed_id = ? AND COALESCE(doi,'') != '' AND NOT (lower(doi) LIKE lower(?) || '%' OR lower(doi) LIKE '%/' || lower(?) || '%')",
                                (fk, pub_param, pub_param),
                            )
                            n = cur.rowcount if hasattr(cur, "rowcount") else None
                            if n:
                                conn.commit()
                            total_deleted += n or 0
                    except Exception:
                        logger.exception(
                            "Failed to delete non-matching DOIs for feed=%s publication=%s",
                            fk,
                            expected_pub,
                        )
                    empty_deleted = 0
                    try:
                        try:
                            if dry_run:
                                if feed_issn:
                                    cur.execute(
                                        "SELECT COUNT(1) FROM articles WHERE COALESCE(doi, '') = '' AND (publication_id = ? OR publication_id = ?)",
                                        (expected_pub, feed_issn),
                                    )
                                else:
                                    cur.execute(
                                        "SELECT COUNT(1) FROM articles WHERE COALESCE(doi, '') = '' AND publication_id = ?",
                                        (expected_pub,),
                                    )
                                row = cur.fetchone()
                                empty_deleted = row[0] if row and row[0] else 0
                                total_deleted += empty_deleted
                            else:
                                if feed_issn:
                                    cur.execute(
                                        "DELETE FROM articles WHERE COALESCE(doi, '') = '' AND (publication_id = ? OR publication_id = ?)",
                                        (expected_pub, feed_issn),
                                    )
                                else:
                                    cur.execute(
                                        "DELETE FROM articles WHERE COALESCE(doi, '') = '' AND publication_id = ?",
                                        (expected_pub,),
                                    )
                                n = cur.rowcount if hasattr(cur, "rowcount") else None
                                empty_deleted = n or 0
                                if empty_deleted:
                                    conn.commit()
                                total_deleted += empty_deleted
                        except Exception:
                            logger.exception(
                                "Failed to handle empty-doi articles for publication=%s (feed=%s)",
                                expected_pub,
                                fk,
                            )
                    except Exception:
                        logger.exception(
                            "Failed to handle empty-doi articles for publication=%s (feed=%s)",
                            expected_pub,
                            fk,
                        )
                else:
                    if dry_run:
                        cur.execute(
                            "SELECT COUNT(1) FROM articles WHERE feed_id = ? AND COALESCE(doi,'') != ''",
                            (fk,),
                        )
                        row = cur.fetchone()
                        cnt = row[0] if row and row[0] else 0
                        total_deleted += cnt
                    else:
                        try:
                            cur.execute(
                                "DELETE FROM articles WHERE feed_id = ? AND COALESCE(doi,'') != ''",
                                (fk,),
                            )
                            n = cur.rowcount if hasattr(cur, "rowcount") else None
                            if n:
                                conn.commit()
                            total_deleted += n or 0
                        except Exception:
                            logger.exception(
                                "Failed to delete articles for feed with no publication_id: %s",
                                fk,
                            )
            except Exception:
                logger.exception(
                    "Failed to process feed %s in remove_feed_articles", fk
                )
                continue
        return total_deleted
    except Exception:
        logger.exception(
            "remove_feed_articles failed for feed_keys=%s publication_id=%s",
            feed_keys,
            publication_id,
        )
        return 0


__all__ = ["remove_feed_articles"]
