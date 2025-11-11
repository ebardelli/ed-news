"""DOI rematch helper extracted from maintenance.py."""

import logging, sqlite3, requests

logger = logging.getLogger("ednews.manage_db.maintenance.rematch")


def rematch_publication_dois(
    conn: sqlite3.Connection,
    publication_id: str | None = None,
    feed_keys: list | None = None,
    dry_run: bool = False,
    remove_orphan_articles: bool = False,
    only_wrong: bool = False,
    retry_limit: int | None = 3,
) -> dict:
    cur = conn.cursor()
    results = {
        "feeds": {},
        "total_cleared": 0,
        "postprocessor_results": {},
        "removed_orphan_articles": 0,
        "articles_created_total": 0,
        "articles_updated_total": 0,
    }
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
    try:
        session = requests.Session()
    except Exception:
        session = None
    try:
        import ednews.processors as proc_mod
    except Exception:
        proc_mod = None
    try:
        from ednews import feeds as feeds_mod

        _feeds_list = feeds_mod.load_feeds() or []
    except Exception:
        _feeds_list = []
    for fk in keys:
        try:
            expected_pub = publication_id
            if not expected_pub:
                try:
                    cur.execute(
                        "SELECT publication_id FROM publications WHERE feed_id = ?",
                        (fk,),
                    )
                    prow = cur.fetchone()
                    expected_pub = prow[0] if prow and prow[0] else None
                except Exception:
                    expected_pub = None
            if only_wrong:
                cur.execute("SELECT guid, doi FROM items WHERE feed_id = ?", (fk,))
            else:
                cur.execute(
                    "SELECT guid, doi FROM items WHERE feed_id = ? AND COALESCE(doi, '') != ''",
                    (fk,),
                )
            rows_with_doi = cur.fetchall()
            wrong_items = []
            wrong_dois = set()
            if expected_pub:
                pref = expected_pub.strip().lower()
                for r in rows_with_doi:
                    guid = r[0]
                    doi = r[1] or ""
                    doi_str = doi.strip() if doi is not None else ""
                    if not doi_str:
                        if only_wrong:
                            wrong_items.append((guid, ""))
                        continue
                    ld = doi.lower()
                    matches = False
                    if ld.startswith(pref):
                        matches = True
                    else:
                        if "/" in ld:
                            try:
                                suffix = ld.split("/", 1)[1]
                                if suffix.startswith(pref):
                                    matches = True
                            except Exception:
                                pass
                        try:
                            import re as _re

                            if _re.search(
                                r"/" + _re.escape(pref) + r"(?:[^a-z0-9]|$)", ld
                            ):
                                matches = True
                        except Exception:
                            pass
                    if not matches:
                        wrong_items.append((guid, doi))
                        wrong_dois.add(doi)
            else:
                for r in rows_with_doi:
                    guid = r[0]
                    doi = (r[1] or "").strip()
                    if doi:
                        wrong_items.append((guid, doi))
                        wrong_dois.add(doi)
                    else:
                        if only_wrong:
                            wrong_items.append((guid, doi))
            results["feeds"][fk] = {"would_clear": len(wrong_items)}
            if only_wrong and not wrong_items:
                results["postprocessor_results"][fk] = 0
                if dry_run:
                    continue
                else:
                    continue
            to_process = list(wrong_items)
            skipped_guids = []
            if only_wrong and retry_limit and retry_limit > 0:
                try:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS rematch_attempts (
                            guid TEXT PRIMARY KEY,
                            attempts INTEGER DEFAULT 0,
                            last_attempt TEXT
                        )
                        """
                    )
                    conn.commit()
                    guids = [w[0] for w in wrong_items if w[0]]
                    if guids:
                        placeholders = ",".join(["?"] * len(guids))
                        cur.execute(
                            f"SELECT guid, attempts FROM rematch_attempts WHERE guid IN ({placeholders})",
                            tuple(guids),
                        )
                        rows = cur.fetchall()
                        attempts_map = {r[0]: (r[1] or 0) for r in rows if r and r[0]}
                    else:
                        attempts_map = {}
                    filtered = []
                    for w in wrong_items:
                        g = w[0]
                        a = attempts_map.get(g, 0)
                        if a >= retry_limit:
                            skipped_guids.append(g)
                        else:
                            filtered.append(w)
                    to_process = filtered
                    if skipped_guids:
                        results["feeds"][fk]["skipped_due_to_retry_limit"] = len(
                            skipped_guids
                        )
                except Exception:
                    logger.exception(
                        "Failed to consult rematch_attempts for feed=%s", fk
                    )
            if dry_run:
                continue
            if only_wrong and not to_process:
                results["postprocessor_results"][fk] = 0
                continue
            cleared = 0
            if to_process:
                try:
                    guids = [w[0] for w in to_process]
                    placeholders = ",".join(["?"] * len(guids))
                    cur.execute(
                        f"UPDATE items SET doi = NULL WHERE feed_id = ? AND guid IN ({placeholders})",
                        tuple([fk] + guids),
                    )
                    cleared = cur.rowcount if hasattr(cur, "rowcount") else None
                    cleared = cleared or 0
                    conn.commit()
                except Exception:
                    logger.exception("Failed to clear item DOIs for feed %s", fk)
            results["feeds"][fk]["cleared"] = cleared
            results["total_cleared"] += cleared
            articles_pub_cleared = 0
            if wrong_dois and expected_pub:
                try:
                    for od in list(wrong_dois):
                        try:
                            cur.execute(
                                "UPDATE articles SET publication_id = NULL WHERE doi = ? AND COALESCE(publication_id, '') != ?",
                                (od, expected_pub),
                            )
                            n = cur.rowcount if hasattr(cur, "rowcount") else None
                            articles_pub_cleared += n or 0
                        except Exception:
                            logger.exception(
                                "Failed to clear publication_id for article doi=%s", od
                            )
                    if articles_pub_cleared:
                        conn.commit()
                except Exception:
                    logger.exception(
                        "Failed to clear articles publication_id for feed %s", fk
                    )
            results["feeds"][fk]["articles_publication_cleared"] = articles_pub_cleared
            articles_doi_cleared = 0
            if wrong_dois:
                try:
                    for od in list(wrong_dois):
                        if not od:
                            continue
                        try:
                            cur.execute(
                                "UPDATE articles SET doi = NULL WHERE doi = ?", (od,)
                            )
                            n = cur.rowcount if hasattr(cur, "rowcount") else None
                            articles_doi_cleared += n or 0
                        except Exception:
                            logger.exception(
                                "Failed to clear doi for article doi=%s", od
                            )
                    if articles_doi_cleared:
                        conn.commit()
                except Exception:
                    logger.exception("Failed to clear articles doi for feed %s", fk)
            results["feeds"][fk]["articles_doi_cleared"] = articles_doi_cleared
            feed_orphan_cleared = 0
            try:
                cur.execute(
                    "SELECT doi FROM articles WHERE feed_id = ? AND COALESCE(doi, '') != '' AND doi NOT IN (SELECT doi FROM items WHERE feed_id = ? AND COALESCE(doi, '') != '')",
                    (fk, fk),
                )
                orphan_rows = cur.fetchall()
                orphan_dois = [r[0] for r in orphan_rows if r and r[0]]
                if orphan_dois:
                    for od in orphan_dois:
                        try:
                            cur.execute(
                                "UPDATE articles SET doi = NULL WHERE doi = ?", (od,)
                            )
                            n = cur.rowcount if hasattr(cur, "rowcount") else None
                            feed_orphan_cleared += n or 0
                        except Exception:
                            logger.exception(
                                "Failed to clear orphan article doi=%s for feed=%s",
                                od,
                                fk,
                            )
                    conn.commit()
            except Exception:
                logger.exception("Failed to clear orphan article DOIs for feed %s", fk)
            results["feeds"][fk]["feed_orphan_articles_cleared"] = feed_orphan_cleared
            updated = 0
            post_fn_for_feed = None
            try:
                proc_config = None
                for item in _feeds_list:
                    try:
                        if item and item[0] == fk:
                            proc_config = item[5] if len(item) > 5 else None
                            break
                    except Exception:
                        continue
                if proc_mod and hasattr(proc_mod, "resolve_postprocessor"):
                    post_fn_for_feed = proc_mod.resolve_postprocessor(
                        proc_config, preferred_proc_name=None
                    )
                else:
                    post_fn_for_feed = (
                        getattr(proc_mod, "crossref_postprocessor_db", None)
                        if proc_mod
                        else None
                    )
            except Exception:
                post_fn_for_feed = (
                    getattr(proc_mod, "crossref_postprocessor_db", None)
                    if proc_mod
                    else None
                )
            if post_fn_for_feed:
                entries = []
                if only_wrong and to_process:
                    guids = [w[0] for w in to_process]
                    placeholders = ",".join(["?"] * len(guids))
                    cur.execute(
                        f"SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? AND guid IN ({placeholders}) ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                        tuple([fk] + guids),
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
                                "doi": None,
                            }
                        )
                else:
                    cur.execute(
                        "SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
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
                                "doi": r[5] if len(r) > 5 else None,
                            }
                        )
                post_map = {}
                try:
                    try:
                        updated = post_fn_for_feed(
                            conn,
                            fk,
                            entries,
                            session=session,
                            publication_id=expected_pub,
                            issn=None,
                            force=True,
                        )
                    except TypeError:
                        try:
                            updated = post_fn_for_feed(
                                conn,
                                fk,
                                entries,
                                session=session,
                                publication_id=expected_pub,
                                issn=None,
                            )
                        except TypeError:
                            updated = post_fn_for_feed(conn, fk, entries)
                    guids_to_check = [e.get("guid") for e in entries if e.get("guid")]
                    if guids_to_check:
                        placeholders_chk = ",".join(["?"] * len(guids_to_check))
                        cur.execute(
                            f"SELECT guid, doi FROM items WHERE feed_id = ? AND guid IN ({placeholders_chk})",
                            tuple([fk] + guids_to_check),
                        )
                        post_rows = cur.fetchall()
                        post_map = {
                            r[0]: (r[1] if len(r) > 1 else None) for r in post_rows
                        }
                        if (not updated) and any((not v) for v in post_map.values()):
                            logger.warning(
                                "rematch_publication_dois: postprocessor returned 0 but some items still have no DOI for feed=%s; guids=%s",
                                fk,
                                [g for g, d in post_map.items() if not d],
                            )
                        if retry_limit and retry_limit > 0:
                            import datetime as _dt

                            now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
                            for g, d in post_map.items():
                                if not d:
                                    try:
                                        cur.execute(
                                            "INSERT INTO rematch_attempts (guid, attempts, last_attempt) VALUES (?, ?, ?) ON CONFLICT(guid) DO UPDATE SET attempts = rematch_attempts.attempts + 1, last_attempt = ?",
                                            (g, 1, now_iso, now_iso),
                                        )
                                    except Exception:
                                        try:
                                            cur.execute(
                                                "SELECT attempts FROM rematch_attempts WHERE guid = ?",
                                                (g,),
                                            )
                                            r = cur.fetchone()
                                            if r and r[0] is not None:
                                                cur.execute(
                                                    "UPDATE rematch_attempts SET attempts = ? , last_attempt = ? WHERE guid = ?",
                                                    (r[0] + 1, now_iso, g),
                                                )
                                            else:
                                                cur.execute(
                                                    "INSERT OR REPLACE INTO rematch_attempts (guid, attempts, last_attempt) VALUES (?, ?, ?)",
                                                    (g, 1, now_iso),
                                                )
                                        except Exception:
                                            logger.exception(
                                                "Failed to increment rematch_attempts for guid=%s",
                                                g,
                                            )
                            conn.commit()
                    try:
                        from ednews import db as eddb

                        if post_map:
                            created = 0
                            updated_a = 0
                            for g, d in list(post_map.items()):
                                if not d:
                                    continue
                                try:
                                    cur.execute(
                                        "SELECT title, link, published FROM items WHERE feed_id = ? AND guid = ? LIMIT 1",
                                        (fk, g),
                                    )
                                    itrow = cur.fetchone()
                                    title_val = (
                                        itrow[0] if itrow and len(itrow) > 0 else None
                                    )
                                    cur.execute(
                                        "SELECT id FROM articles WHERE doi = ? LIMIT 1",
                                        (d,),
                                    )
                                    existing = cur.fetchone()
                                    try:
                                        eddb.ensure_article_row(
                                            conn,
                                            d,
                                            title=title_val,
                                            feed_id=fk,
                                            publication_id=expected_pub,
                                        )
                                    except Exception:
                                        logger.exception(
                                            "Failed to ensure article row for doi=%s", d
                                        )
                                    try:
                                        cur.execute(
                                            "SELECT id FROM articles WHERE doi = ? LIMIT 1",
                                            (d,),
                                        )
                                        aid_row = cur.fetchone()
                                        if aid_row and not existing:
                                            created += 1
                                    except Exception:
                                        logger.exception(
                                            "Failed to check created article id for doi=%s",
                                            d,
                                        )
                                    try:
                                        cur.execute(
                                            "UPDATE articles SET feed_id = COALESCE(?, feed_id), publication_id = COALESCE(?, publication_id) WHERE doi = ?",
                                            (fk, expected_pub, d),
                                        )
                                        n = (
                                            cur.rowcount
                                            if hasattr(cur, "rowcount")
                                            else None
                                        )
                                        if n and n > 0:
                                            updated_a += n
                                            conn.commit()
                                    except Exception:
                                        logger.exception(
                                            "Failed to update articles feed/publication for doi=%s",
                                            d,
                                        )
                                except Exception:
                                    logger.exception(
                                        "Failed to sync article for guid=%s feed=%s",
                                        g,
                                        fk,
                                    )
                            results["feeds"][fk]["articles_created"] = created
                            results["feeds"][fk]["articles_updated"] = updated_a
                            results["articles_created_total"] += created
                            results["articles_updated_total"] += updated_a
                    except Exception:
                        logger.exception(
                            "Failed to synchronize articles from items after postprocessor for feed=%s",
                            fk,
                        )
                except Exception:
                    logger.exception("crossref_postprocessor_db failed for feed %s", fk)
            results["postprocessor_results"][fk] = updated or 0
            if remove_orphan_articles and publication_id:
                try:
                    cur.execute(
                        "SELECT doi FROM articles WHERE publication_id = ? AND COALESCE(doi, '') != '' AND doi NOT IN (SELECT doi FROM items WHERE COALESCE(doi, '') != '')",
                        (publication_id,),
                    )
                    orphan_rows = cur.fetchall()
                    orphans = [r[0] for r in orphan_rows if r and r[0]]
                    removed = 0
                    for od in orphans:
                        try:
                            cur.execute("DELETE FROM articles WHERE doi = ?", (od,))
                            removed += 1
                        except Exception:
                            logger.exception(
                                "Failed to delete orphan article doi=%s", od
                            )
                    conn.commit()
                    results["removed_orphan_articles"] += removed
                    results["feeds"][fk]["removed_orphan_articles"] = removed
                except Exception:
                    logger.exception(
                        "Failed to remove orphan articles for publication_id=%s",
                        publication_id,
                    )
        except Exception:
            logger.exception("Failed to rematch DOIs for feed %s", fk)
            continue
    return results


__all__ = ["rematch_publication_dois"]
