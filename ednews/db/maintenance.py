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
            ok = upsert_publication(conn, key, pub_id, title, issn)
            if ok:
                count += 1
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
