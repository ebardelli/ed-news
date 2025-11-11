"""DOI rematch helper: in-memory rematch using CrossRef title lookup.

Simple, well-scoped implementation:
- select article or item rows to rematch
- for each entry call ednews.crossref.query_crossref_doi_by_title
- if a DOI is returned, upsert the article and attach DOI to article/item rows

This avoids auxiliary tables and performs the work in memory with direct DB writes.
"""

import logging
import sqlite3
from typing import List, Dict, Any, Optional

logger = logging.getLogger("ednews.manage_db.maintenance.rematch")


def rematch_publication_dois(
    conn: sqlite3.Connection,
    publication_id: Optional[str] = None,
    feed_keys: Optional[list] = None,
    dry_run: bool = False,
    remove_orphan_articles: bool = False,
    only_wrong: bool = False,
    only_missing: bool = False,
    only_articles: bool = False,
    retry_limit: Optional[int] = 3,
) -> Dict[str, Any]:
    cur = conn.cursor()
    results: Dict[str, Any] = {
        "feeds": {},
        "total_cleared": 0,
        "postprocessor_results": {},
        "postprocessor_matches": {},
        "removed_orphan_articles": 0,
        "articles_created_total": 0,
        "articles_updated_total": 0,
    }

    # Resolve feed keys
    keys: List[str] = []
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

    # Lazy imports
    try:
        from ednews import crossref as cr
        from ednews import db as eddb
        from ednews import feeds as feeds_mod
    except Exception:
        cr = None
        eddb = None
        feeds_mod = None

    for fk in keys:
        try:
            if only_articles and (not only_missing) and (not only_wrong):
                only_missing = True

            # Determine expected publication id for this feed
            expected_pub = publication_id
            if not expected_pub:
                try:
                    from ednews import feeds as feeds_pkg

                    fl = feeds_pkg.load_feeds() or []
                    for item in fl:
                        try:
                            if item and item[0] == fk:
                                cfg_pub = item[3] if len(item) > 3 else None
                                if cfg_pub:
                                    expected_pub = cfg_pub
                                    break
                        except Exception:
                            continue
                except Exception:
                    expected_pub = None
            if not expected_pub and not only_missing:
                try:
                    cur.execute(
                        "SELECT publication_id FROM publications WHERE feed_id = ?",
                        (fk,),
                    )
                    prow = cur.fetchone()
                    expected_pub = prow[0] if prow and prow[0] else None
                except Exception:
                    expected_pub = None

            # Build list of entries to rematch
            entries: List[Dict[str, Any]] = []
            if only_articles:
                # articles table does not contain an explicit `link` column.
                # Select id, title, published, fetched_at and leave `link` None
                if only_missing:
                    cur.execute(
                        "SELECT id, title, published, fetched_at FROM articles WHERE COALESCE(feed_id, '') = ? AND COALESCE(doi, '') = '' ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        (fk,),
                    )
                else:
                    cur.execute(
                        "SELECT id, title, published, fetched_at FROM articles WHERE COALESCE(feed_id, '') = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        (fk,),
                    )
                rows = cur.fetchall()
                for r in rows:
                    entries.append(
                        {
                            "guid": f"article:{r[0]}",
                            "link": None,
                            "title": r[1],
                            "published": r[2],
                            "_fetched_at": r[3],
                        }
                    )
            else:
                if only_missing:
                    cur.execute(
                        "SELECT guid, link, title, published, fetched_at FROM items WHERE feed_id = ? AND COALESCE(doi, '') = '' ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        (fk,),
                    )
                elif only_wrong:
                    cur.execute(
                        "SELECT guid, link, title, published, fetched_at FROM items WHERE feed_id = ? AND COALESCE(doi, '') != '' ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        (fk,),
                    )
                else:
                    cur.execute(
                        "SELECT guid, link, title, published, fetched_at FROM items WHERE feed_id = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        (fk,),
                    )
                rows = cur.fetchall()
                for r in rows:
                    entries.append(
                        {
                            "guid": r[0],
                            "link": r[1],
                            "title": r[2],
                            "published": r[3],
                            "_fetched_at": r[4],
                        }
                    )

            if dry_run:
                results["postprocessor_results"][fk] = 0
                continue

            post_map: Dict[str, Optional[str]] = {}
            updated = 0
            for entry in entries:
                guid = entry.get("guid")
                title = (entry.get("title") or "").strip()
                logger.info(
                    "rematch: processing guid=%s title=%s",
                    guid,
                    (title[:200] if title else "<no title>"),
                )
                if not title or cr is None:
                    if isinstance(guid, str):
                        post_map[guid] = None
                    continue

                try:
                    doi_found = cr.query_crossref_doi_by_title(
                        title, preferred_publication_id=expected_pub
                    )
                except Exception:
                    doi_found = None
                if not doi_found:
                    if isinstance(guid, str):
                        post_map[guid] = None
                    continue

                doi = doi_found
                try:
                    if feeds_mod and hasattr(feeds_mod, "normalize_doi"):
                        doi = feeds_mod.normalize_doi(doi) or doi
                except Exception:
                    pass

                try:
                    # If this entry corresponds to an existing article id, update that
                    # article row first to avoid creating a duplicate DOI via upsert.
                    updated_existing = False
                    if guid and isinstance(guid, str) and guid.startswith("article:"):
                        try:
                            aid_local = int(guid.split(":", 1)[1])
                            try:
                                cur.execute(
                                    "UPDATE articles SET doi = ?, feed_id = COALESCE(?, feed_id), publication_id = CASE WHEN COALESCE(publication_id, '') = '' THEN ? ELSE publication_id END WHERE id = ?",
                                    (doi, fk, expected_pub, aid_local),
                                )
                                if cur.rowcount and cur.rowcount > 0:
                                    conn.commit()
                                    updated_existing = True
                            except sqlite3.IntegrityError:
                                # If another article already has this DOI (unique constraint),
                                # remove the article row we attempted to update to avoid duplicates.
                                logger.warning(
                                    "IntegrityError updating article id=%s with doi=%s; deleting row",
                                    aid_local,
                                    doi,
                                )
                                try:
                                    cur.execute("DELETE FROM articles WHERE id = ?", (aid_local,))
                                    conn.commit()
                                    results["removed_orphan_articles"] = (
                                        results.get("removed_orphan_articles", 0) + 1
                                    )
                                except Exception:
                                    logger.exception(
                                        "Failed to delete article id=%s after IntegrityError",
                                        aid_local,
                                    )
                        except Exception:
                            logger.exception(
                                "Failed to update original article id from guid=%s",
                                guid,
                            )

                    # Ensure an article exists for the DOI if we didn't update an existing article
                    if not updated_existing and eddb and hasattr(eddb, "upsert_article"):
                        try:
                            eddb.upsert_article(
                                conn,
                                doi,
                                title=title,
                                authors=None,
                                abstract=None,
                                feed_id=fk,
                                publication_id=expected_pub,
                                fetched_at=entry.get("_fetched_at") or None,
                                published=entry.get("published") or None,
                            )
                        except Exception:
                            logger.exception("upsert_article failed for doi=%s", doi)

                    # If this was an item, attach DOI to the item row
                    if (
                        guid
                        and isinstance(guid, str)
                        and not guid.startswith("article:")
                    ):
                        try:
                            cur.execute(
                                "UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?",
                                (doi, fk, guid),
                            )
                            conn.commit()
                        except Exception:
                            logger.exception(
                                "Failed to update item doi for guid=%s", guid
                            )

                    if isinstance(guid, str):
                        post_map[guid] = doi
                    updated += 1
                except Exception:
                    logger.exception("Failed to upsert doi=%s for guid=%s", doi, guid)
                    if isinstance(guid, str):
                        post_map[guid] = None

            results["postprocessor_matches"][fk] = post_map
            results["postprocessor_results"][fk] = updated
            results["feeds"][fk] = {"would_clear": len(entries), "cleared": 0}

        except Exception:
            logger.exception("Failed to rematch DOIs for feed %s", fk)
            continue

    return results
