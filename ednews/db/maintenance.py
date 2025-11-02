"""Maintenance helpers: cleanup, vacuum, and publication sync.
"""
from datetime import datetime, timezone, timedelta
import logging
import sqlite3
import time
import os

logger = logging.getLogger("ednews.manage_db.maintenance")


def log_maintenance_run(conn: sqlite3.Connection, command: str, status: str, started: str | None = None, finished: str | None = None, duration: float | None = None, details: dict | None = None) -> int:
    try:
        cur = conn.cursor()
        import json
        details_json = json.dumps(details, default=str) if details is not None else None
        cur.execute(
            "INSERT INTO maintenance_runs (command, status, started, finished, duration, details) VALUES (?, ?, ?, ?, ?, ?)",
            (command, status, started, finished, duration, details_json),
        )
        conn.commit()
        return cur.lastrowid if hasattr(cur, 'lastrowid') else 0
    except Exception:
        logger.exception("Failed to log maintenance run for command=%s", command)
        return 0


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
            # Use the canonical upsert_publication helper to insert/update the
            # publications table keyed by (publication_id, issn). This avoids
            # UNIQUE constraint errors. After upserting by publication_id/issn,
            # remove any stale rows that still reference this feed_id but with
            # a different publication_id/issn.
            cur = conn.cursor()
            try:
                ok = upsert_publication(conn, key, pub_id, title, issn)
                if ok:
                    # Remove any legacy rows that mapped this feed_id to a
                    # different publication identifier to avoid duplicates.
                    try:
                        cur.execute("DELETE FROM publications WHERE feed_id = ? AND (publication_id != ? OR issn != ?)", (key, pub_id or '', issn or ''))
                        conn.commit()
                    except Exception:
                        logger.exception("Failed to cleanup old publication rows for feed_id=%s", key)
                    count += 1
            except Exception:
                logger.exception("Failed to sync publication for feed item: %s", item)
        except Exception:
            logger.exception("Failed to sync publication for feed item: %s", item)
            continue
    logger.info("Synchronized %d publications from feeds", count)
    return count


def fetch_latest_journal_works(
    conn: sqlite3.Connection,
    feeds,
    per_journal: int = 30,
    timeout: int = 10,
    delay: float = 0.05,
    sort_by: str = "created",
    date_filter_type: str | None = None,
    from_date: str | None = None,
    until_date: str | None = None,
):
    import requests

    cur = conn.cursor()
    session = requests.Session()
    try:
        from ednews import config as _config
        connect_timeout = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
        read_timeout = getattr(_config, 'CROSSREF_TIMEOUT', 30)
        default_retries = getattr(_config, 'CROSSREF_RETRIES', 3)
        backoff = getattr(_config, 'CROSSREF_BACKOFF', 0.3)
        status_forcelist = getattr(_config, 'CROSSREF_STATUS_FORCELIST', [429, 500, 502, 503, 504])
    except Exception:
        connect_timeout = 5
        read_timeout = 30
        default_retries = 3
        backoff = 0.3
        status_forcelist = [429, 500, 502, 503, 504]

    attempts = max(1, int(default_retries) + 1)
    inserted = 0
    skipped = 0
    logger.info("Fetching latest journal works for %s feeds", len(feeds) if hasattr(feeds, '__len__') else 'unknown')

    for item in feeds:
        key = item[0] if len(item) > 0 else None
        title = item[1] if len(item) > 1 else None
        url = item[2] if len(item) > 2 else None
        publication_id = item[3] if len(item) > 3 else None
        issn = item[4] if len(item) > 4 else None
        if not issn:
            continue

        try:
            ua = None
            try:
                from ednews import config as _cfg
                ua = getattr(_cfg, 'USER_AGENT', None)
            except Exception:
                ua = None
            headers = {"User-Agent": ua or "ed-news-fetcher/1.0", "Accept": "application/json"}
            mailto = os.environ.get("CROSSREF_MAILTO", "your_email@example.com")
            base_url = f"https://api.crossref.org/journals/{issn}/works"

            filter_parts = ["type:journal-article"]
            if date_filter_type and from_date:
                filter_parts.append(f"from-{date_filter_type}-date:{from_date}")
            if date_filter_type and until_date:
                filter_parts.append(f"until-{date_filter_type}-date:{until_date}")
            base_filter = ",".join(filter_parts)

            remaining = int(per_journal)
            cursor = "*"
            collected_items: list[dict] = []
            used_timeout = (connect_timeout, timeout if timeout and timeout > 0 else read_timeout)

            while remaining > 0:
                params = {
                    "sort": sort_by,
                    "order": "desc",
                    "filter": base_filter,
                    "rows": min(1000, remaining),
                    "mailto": mailto,
                    "cursor": cursor,
                }

                resp = None
                last_exc = None
                for attempt in range(1, attempts + 1):
                    try:
                        resp = session.get(base_url, params=params, headers=headers, timeout=used_timeout)
                        if resp.status_code in status_forcelist:
                            last_exc = requests.HTTPError(f"status={resp.status_code}")
                            raise last_exc
                        resp.raise_for_status()
                        break
                    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
                        last_exc = e
                        logger.warning("Request attempt %d/%d failed for ISSN=%s: %s", attempt, attempts, issn, e)
                    except requests.HTTPError as e:
                        last_exc = e
                        code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                        if code in status_forcelist:
                            logger.warning("HTTP %s on attempt %d/%d for ISSN=%s: will retry", code, attempt, attempts, issn)
                        else:
                            raise
                    if attempt < attempts:
                        sleep_for = backoff * (2 ** (attempt - 1))
                        sleep_for = sleep_for + (0.1 * backoff)
                        time.sleep(sleep_for)

                if resp is None:
                    raise last_exc if last_exc is not None else Exception("Failed to retrieve Crossref data")

                data = resp.json()
                page_items = data.get("message", {}).get("items", []) or []
                collected_items.extend(page_items)
                remaining = per_journal - len(collected_items)
                next_cursor = data.get("message", {}).get("next-cursor")
                if not next_cursor or not page_items:
                    break
                cursor = next_cursor

            items = collected_items

            for it in items[:per_journal]:
                doi = (it.get("DOI") or "").strip()
                if not doi:
                    continue
                norm = doi
                if not norm:
                    continue
                try:
                    from ednews.db import article_exists, upsert_article, update_article_crossref
                    if article_exists(conn, norm):
                        skipped += 1
                        continue

                    try:
                        from ednews.crossref import fetch_crossref_metadata
                        cr = fetch_crossref_metadata(norm, conn=conn)
                    except Exception:
                        cr = None

                    authors_val = cr.get('authors') if cr and cr.get('authors') else None
                    abstract_val = cr.get('abstract') if cr and cr.get('abstract') else it.get('abstract')
                    published_val = cr.get('published') if cr and cr.get('published') else None

                    aid = upsert_article(
                        conn,
                        norm,
                        title=it.get('title'),
                        authors=authors_val,
                        abstract=abstract_val,
                        feed_id=key,
                        publication_id=issn,
                        issn=issn,
                        fetched_at=None,
                        published=published_val,
                    )
                    if aid:
                        inserted += 1

                    if cr and cr.get('raw'):
                        try:
                            update_article_crossref(conn, norm, authors=authors_val, abstract=abstract_val, raw=cr.get('raw'), published=published_val)
                        except Exception:
                            logger.debug("Failed to update crossref data for doi=%s after upsert", norm)
                except Exception:
                    logger.exception("Failed to upsert article doi=%s from journal %s", doi, issn)
            conn.commit()
        except Exception:
            logger.exception("Failed to fetch works for ISSN=%s (feed=%s)", issn, key)
    logger.info("ISSN lookup summary: inserted=%d skipped=%d", inserted, skipped)
    return inserted


def vacuum_db(conn: sqlite3.Connection):
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
    try:
        cur = conn.cursor()
        params = []
        where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
        if older_than_days is not None:
            cutoff = (datetime.now(timezone.utc) - timedelta(days=int(older_than_days))).isoformat()
            where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
            params.extend([cutoff, cutoff])
        where_sql = " AND ".join(where_clauses)
        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(params))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        conn.commit()
        logger.info("cleanup_empty_articles deleted %s rows (older_than_days=%s)", deleted, older_than_days)
        return deleted or 0
    except Exception:
        logger.exception("cleanup_empty_articles failed")
        return 0


def cleanup_filtered_titles(conn: sqlite3.Connection, filters: list | None = None, dry_run: bool = False) -> int:
    try:
        from ednews import config
        try:
            if filters is None:
                filters = getattr(config, 'TITLE_FILTERS', [])
        except Exception:
            filters = filters or []

        if not filters:
            logger.debug("cleanup_filtered_titles: no filters configured; nothing to do")
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
            cur.execute(f"SELECT COUNT(1) FROM articles WHERE {where_sql}", tuple(norm_filters))
            row = cur.fetchone()
            count = row[0] if row and row[0] else 0
            logger.info("cleanup_filtered_titles dry-run would delete %s rows", count)
            return count

        cur.execute(f"DELETE FROM articles WHERE {where_sql}", tuple(norm_filters))
        deleted = cur.rowcount if hasattr(cur, 'rowcount') else None
        conn.commit()
        logger.info("cleanup_filtered_titles deleted %s rows", deleted)
        return deleted or 0
    except Exception:
        logger.exception("cleanup_filtered_titles failed")
        return 0


def rematch_publication_dois(conn: sqlite3.Connection, publication_id: str | None = None, feed_keys: list | None = None, dry_run: bool = False, remove_orphan_articles: bool = False, only_wrong: bool = False, retry_limit: int | None = 3) -> dict:
    """Clear DOI assignments for items belonging to a publication or feed(s) and re-run Crossref postprocessor.

    This helper will:
    - Determine feed keys to operate on from `publication_id` (lookup in publications) or `feed_keys`.
    - Count item rows that currently have a non-empty DOI for those feeds.
    - If not dry_run, set those items' doi to NULL/empty.
    - Invoke the `crossref_postprocessor_db` for each affected feed to attempt rematching.
    - Optionally remove orphaned articles that are not referenced by any items and are associated with the publication_id.

    Returns a dict with summary counts and per-feed results.
    """
    cur = conn.cursor()
    results = {"feeds": {}, "total_cleared": 0, "postprocessor_results": {}, "removed_orphan_articles": 0}
    # Resolve target feed keys
    keys: list[str] = []
    if feed_keys:
        keys = [k for k in feed_keys if k]

    if publication_id and not keys:
        try:
            cur.execute("SELECT feed_id FROM publications WHERE publication_id = ?", (publication_id,))
            rows = cur.fetchall()
            keys = [r[0] for r in rows if r and r[0]]
        except Exception:
            logger.exception("Failed to lookup feeds for publication_id=%s", publication_id)

    if not keys:
        # If no specific publication_id or feed_keys were provided, run
        # across all known feeds. Prefer the `publications` table; if it's
        # empty, fall back to distinct feed_id values from items.
        try:
            cur.execute("SELECT DISTINCT feed_id FROM publications WHERE COALESCE(feed_id, '') != ''")
            rows = cur.fetchall()
            keys = [r[0] for r in rows if r and r[0]]
        except Exception:
            keys = []

        if not keys:
            try:
                cur.execute("SELECT DISTINCT feed_id FROM items WHERE COALESCE(feed_id, '') != ''")
                rows = cur.fetchall()
                keys = [r[0] for r in rows if r and r[0]]
            except Exception:
                keys = []

        if not keys:
            logger.debug("rematch_publication_dois: no feed keys resolved for publication_id=%s feed_keys=%s", publication_id, feed_keys)
            return results

    # Build a session for postprocessor calls
    try:
        import requests

        session = requests.Session()
    except Exception:
        session = None

    # Import crossref postprocessor
    try:
        import ednews.processors as proc_mod
        post_fn = getattr(proc_mod, 'crossref_postprocessor_db', None)
    except Exception:
        post_fn = None

    for fk in keys:
        try:
            # Determine the expected publication_id for this feed (prefer explicit arg)
            expected_pub = publication_id
            if not expected_pub:
                try:
                    cur.execute("SELECT publication_id FROM publications WHERE feed_id = ?", (fk,))
                    prow = cur.fetchone()
                    expected_pub = prow[0] if prow and prow[0] else None
                except Exception:
                    expected_pub = None

            # Fetch items for this feed. If only_wrong is True, include items
            # with missing DOIs as candidates; otherwise only fetch items with DOIs.
            if only_wrong:
                cur.execute("SELECT guid, doi FROM items WHERE feed_id = ?", (fk,))
            else:
                cur.execute("SELECT guid, doi FROM items WHERE feed_id = ? AND COALESCE(doi, '') != ''", (fk,))
            rows_with_doi = cur.fetchall()
            wrong_items = []
            wrong_dois = set()
            if expected_pub:
                pref = expected_pub.strip().lower()
                for r in rows_with_doi:
                    guid = r[0]
                    doi = (r[1] or '')
                    doi_str = doi.strip() if doi is not None else ''
                    if not doi_str:
                        # missing DOI: candidate for rematch when only_wrong is set
                        if only_wrong:
                            wrong_items.append((guid, ''))
                        continue
                    ld = doi.lower()
                    matches = False
                    if ld.startswith(pref):
                        matches = True
                    else:
                        # Check suffix after '/' for short ids like 'edfp'
                        if '/' in ld:
                            try:
                                suffix = ld.split('/', 1)[1]
                                if suffix.startswith(pref):
                                    matches = True
                            except Exception:
                                pass
                        # regex fallback '/pref' in suffix
                        try:
                            import re as _re

                            if _re.search(r'/' + _re.escape(pref) + r'(?:[^a-z0-9]|$)', ld):
                                matches = True
                        except Exception:
                            pass
                    if not matches:
                        wrong_items.append((guid, doi))
                        wrong_dois.add(doi)
            else:
                # No expected publication_id configured; treat all fetched items
                # as candidates (if only_wrong is True this includes missing DOIs)
                for r in rows_with_doi:
                    guid = r[0]
                    doi = (r[1] or '').strip()
                    if doi:
                        wrong_items.append((guid, doi))
                        wrong_dois.add(doi)
                    else:
                        # missing DOI -- candidate for rematch when only_wrong
                        if only_wrong:
                            wrong_items.append((guid, doi))

            results['feeds'][fk] = {"would_clear": len(wrong_items)}

            # Log how many wrong/missing DOIs were identified when only_wrong is set
            if only_wrong:
                try:
                    logger.info(
                        "rematch_publication_dois: feed=%s identified %d wrong/missing DOIs (only-wrong)",
                        fk,
                        len(wrong_items),
                    )
                except Exception:
                    pass

            # If --only-wrong was requested but we found no wrong items, there's
            # nothing to do: skip running the (potentially expensive) postprocessor.
            if only_wrong and not wrong_items:
                try:
                    logger.info("rematch_publication_dois: feed=%s no wrong items; skipping postprocessor (only-wrong)", fk)
                except Exception:
                    pass
                results['postprocessor_results'][fk] = 0
                # continue to next feed
                if dry_run:
                    continue
                else:
                    continue

            # If we have a retry_limit configured and are running in only_wrong
            # mode, consult the rematch_attempts table to avoid repeatedly
            # reprocessing items that have failed rematch too many times.
            to_process = list(wrong_items)
            skipped_guids = []
            if only_wrong and retry_limit and retry_limit > 0:
                try:
                    # Ensure rematch_attempts table exists
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
                    # Fetch current attempt counts
                    guids = [w[0] for w in wrong_items if w[0]]
                    if guids:
                        placeholders = ','.join(['?'] * len(guids))
                        cur.execute(f"SELECT guid, attempts FROM rematch_attempts WHERE guid IN ({placeholders})", tuple(guids))
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
                        logger.info("rematch_publication_dois: feed=%s skipping %d guids that exceeded retry_limit=%s", fk, len(skipped_guids), retry_limit)
                        results['feeds'][fk]['skipped_due_to_retry_limit'] = len(skipped_guids)
                except Exception:
                    logger.exception("Failed to consult rematch_attempts for feed=%s", fk)

            if dry_run:
                continue

            # If we are in only_wrong mode and retry filtering removed all items,
            # there's nothing to process for this feed: skip the postprocessor.
            if only_wrong and not to_process:
                try:
                    logger.info("rematch_publication_dois: feed=%s no items to process after retry filtering; skipping postprocessor", fk)
                except Exception:
                    pass
                results['postprocessor_results'][fk] = 0
                continue

            # Clear DOIs on the identified wrong items (only those we're processing)
            cleared = 0
            if to_process:
                try:
                    guids = [w[0] for w in to_process]
                    # Use parameterized IN clause
                    placeholders = ','.join(['?'] * len(guids))
                    cur.execute(f"UPDATE items SET doi = NULL WHERE feed_id = ? AND guid IN ({placeholders})", tuple([fk] + guids))
                    cleared = cur.rowcount if hasattr(cur, 'rowcount') else None
                    cleared = cleared or 0
                    conn.commit()
                except Exception:
                    logger.exception("Failed to clear item DOIs for feed %s", fk)
            results['feeds'][fk]['cleared'] = cleared
            results['total_cleared'] += cleared

            # For any wrong DOIs, clear their publication_id in articles so
            # that an upsert during postprocessing will be able to set the
            # correct publication_id passed to the postprocessor.
            articles_pub_cleared = 0
            if wrong_dois and expected_pub:
                try:
                    for od in list(wrong_dois):
                        try:
                            cur.execute("UPDATE articles SET publication_id = NULL WHERE doi = ? AND COALESCE(publication_id, '') != ?", (od, expected_pub))
                            n = cur.rowcount if hasattr(cur, 'rowcount') else None
                            articles_pub_cleared += n or 0
                        except Exception:
                            logger.exception("Failed to clear publication_id for article doi=%s", od)
                    if articles_pub_cleared:
                        conn.commit()
                except Exception:
                    logger.exception("Failed to clear articles publication_id for feed %s", fk)
            results['feeds'][fk]['articles_publication_cleared'] = articles_pub_cleared

            # Re-run crossref postprocessor for this feed if available
            updated = 0
            if post_fn:
                # load recent items. If only_wrong is True, limit to only the
                # guids we identified as wrong so the postprocessor will run
                # title lookups for them rather than processing the entire feed.
                entries = []
                if only_wrong and to_process:
                    guids = [w[0] for w in to_process]
                    placeholders = ','.join(['?'] * len(guids))
                    cur.execute(f"SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? AND guid IN ({placeholders}) ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000", tuple([fk] + guids))
                    rows = cur.fetchall()
                    for r in rows:
                        # Ensure doi is explicitly None so postprocessor will
                        # perform a title lookup rather than reuse an existing DOI
                        entries.append({'guid': r[0], 'link': r[1], 'title': r[2], 'published': r[3], '_fetched_at': r[4], 'doi': None})
                else:
                    cur.execute("SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000", (fk,))
                    rows = cur.fetchall()
                    for r in rows:
                        entries.append({'guid': r[0], 'link': r[1], 'title': r[2], 'published': r[3], '_fetched_at': r[4], 'doi': r[5] if len(r) > 5 else None})

                try:
                    # Log diagnostic: how many entries and sample guids
                    try:
                        sample_guids = [e.get('guid') for e in entries[:5]]
                    except Exception:
                        sample_guids = None
                    logger.debug("rematch_publication_dois: calling postprocessor for feed=%s entries=%s sample_guids=%s", fk, len(entries), sample_guids)

                    # attempt newer signature first and request force=True so existing DOIs are re-fetched
                    try:
                        updated = post_fn(conn, fk, entries, session=session, publication_id=expected_pub, issn=None, force=True)
                    except TypeError:
                        # older signatures may not accept force kwarg
                        try:
                            updated = post_fn(conn, fk, entries, session=session, publication_id=expected_pub, issn=None)
                        except TypeError:
                            # older legacy signature without session/publication_id
                            updated = post_fn(conn, fk, entries)

                    logger.info("rematch_publication_dois: postprocessor returned %s updates for feed=%s", updated or 0, fk)

                    # Re-query items we attempted to update and log their current DOI values
                    try:
                        guids_to_check = [e.get('guid') for e in entries if e.get('guid')]
                        if guids_to_check:
                            placeholders_chk = ','.join(['?'] * len(guids_to_check))
                            cur.execute(f"SELECT guid, doi FROM items WHERE feed_id = ? AND guid IN ({placeholders_chk})", tuple([fk] + guids_to_check))
                            post_rows = cur.fetchall()
                            # Build a small mapping for logging
                            post_map = {r[0]: (r[1] if len(r) > 1 else None) for r in post_rows}
                            logger.debug("rematch_publication_dois: postprocessor result for feed=%s item_dois=%s", fk, post_map)
                            # If postprocessor reported zero updates but DOIs remain empty, warn
                            if (not updated) and any((not v) for v in post_map.values()):
                                logger.warning("rematch_publication_dois: postprocessor returned 0 but some items still have no DOI for feed=%s; guids=%s", fk, [g for g, d in post_map.items() if not d])
                            # If we have retry tracking enabled, increment attempts for
                            # processed items that still lack DOIs so they will be
                            # skipped after exceeding retry_limit.
                            try:
                                if retry_limit and retry_limit > 0:
                                    import datetime as _dt
                                    now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()
                                    for g, d in post_map.items():
                                        if not d:
                                            # increment or insert
                                            try:
                                                cur.execute("INSERT INTO rematch_attempts (guid, attempts, last_attempt) VALUES (?, ?, ?) ON CONFLICT(guid) DO UPDATE SET attempts = rematch_attempts.attempts + 1, last_attempt = ?", (g, 1, now_iso, now_iso))
                                            except Exception:
                                                # Fallback for SQLite versions without DO UPDATE syntax
                                                try:
                                                    cur.execute("SELECT attempts FROM rematch_attempts WHERE guid = ?", (g,))
                                                    r = cur.fetchone()
                                                    if r and r[0] is not None:
                                                        cur.execute("UPDATE rematch_attempts SET attempts = ? , last_attempt = ? WHERE guid = ?", (r[0] + 1, now_iso, g))
                                                    else:
                                                        cur.execute("INSERT OR REPLACE INTO rematch_attempts (guid, attempts, last_attempt) VALUES (?, ?, ?)", (g, 1, now_iso))
                                                except Exception:
                                                    logger.exception("Failed to increment rematch_attempts for guid=%s", g)
                                    conn.commit()
                            except Exception:
                                logger.exception("Failed to update rematch_attempts after postprocessor for feed=%s", fk)
                    except Exception:
                        logger.exception("Failed to re-query items after postprocessor for feed=%s", fk)
                except Exception:
                    logger.exception("crossref_postprocessor_db failed for feed %s", fk)
            results['postprocessor_results'][fk] = updated or 0

            # Optionally remove orphan articles tied to this publication (no items reference their DOI)
            if remove_orphan_articles and publication_id:
                try:
                    # Find article DOIs for this publication that are not present in items
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
                            logger.exception("Failed to delete orphan article doi=%s", od)
                    conn.commit()
                    results['removed_orphan_articles'] += removed
                    results['feeds'][fk]['removed_orphan_articles'] = removed
                except Exception:
                    logger.exception("Failed to remove orphan articles for publication_id=%s", publication_id)
        except Exception:
            logger.exception("Failed to rematch DOIs for feed %s", fk)
            continue

    return results
