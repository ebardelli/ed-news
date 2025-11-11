"""Journal works fetcher (Crossref) extracted from maintenance.py."""

import logging, sqlite3, time, os, requests

logger = logging.getLogger("ednews.manage_db.maintenance.journal")


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
    cur = conn.cursor()
    session = requests.Session()
    try:
        from ednews import config as _config

        connect_timeout = getattr(_config, "CROSSREF_CONNECT_TIMEOUT", 5)
        read_timeout = getattr(_config, "CROSSREF_TIMEOUT", 30)
        default_retries = getattr(_config, "CROSSREF_RETRIES", 3)
        backoff = getattr(_config, "CROSSREF_BACKOFF", 0.3)
        status_forcelist = getattr(
            _config, "CROSSREF_STATUS_FORCELIST", [429, 500, 502, 503, 504]
        )
    except Exception:
        connect_timeout = 5
        read_timeout = 30
        default_retries = 3
        backoff = 0.3
        status_forcelist = [429, 500, 502, 503, 504]
    attempts = max(1, int(default_retries) + 1)
    inserted = 0
    skipped = 0
    logger.info(
        "Fetching latest journal works for %s feeds",
        len(feeds) if hasattr(feeds, "__len__") else "unknown",
    )
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

                ua = getattr(_cfg, "USER_AGENT", None)
            except Exception:
                ua = None
            headers = {
                "User-Agent": ua or "ed-news-fetcher/1.0",
                "Accept": "application/json",
            }
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
            used_timeout = (
                connect_timeout,
                timeout if timeout and timeout > 0 else read_timeout,
            )
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
                        resp = session.get(
                            base_url,
                            params=params,
                            headers=headers,
                            timeout=used_timeout,
                        )
                        if resp.status_code in status_forcelist:
                            last_exc = requests.HTTPError(f"status={resp.status_code}")
                            raise last_exc
                        resp.raise_for_status()
                        break
                    except (
                        requests.exceptions.ReadTimeout,
                        requests.exceptions.ConnectionError,
                    ) as e:
                        last_exc = e
                        logger.warning(
                            "Request attempt %d/%d failed for ISSN=%s: %s",
                            attempt,
                            attempts,
                            issn,
                            e,
                        )
                    except requests.HTTPError as e:
                        last_exc = e
                        code = (
                            getattr(e.response, "status_code", None)
                            if hasattr(e, "response")
                            else None
                        )
                        if code in status_forcelist:
                            logger.warning(
                                "HTTP %s on attempt %d/%d for ISSN=%s: will retry",
                                code,
                                attempt,
                                attempts,
                                issn,
                            )
                        else:
                            raise
                    if attempt < attempts:
                        sleep_for = backoff * (2 ** (attempt - 1))
                        sleep_for = sleep_for + (0.1 * backoff)
                        time.sleep(sleep_for)
                if resp is None:
                    raise (
                        last_exc
                        if last_exc is not None
                        else Exception("Failed to retrieve Crossref data")
                    )
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
                    from ednews.db import (
                        article_exists,
                        upsert_article,
                        update_article_crossref,
                    )

                    if article_exists(conn, norm):
                        skipped += 1
                        continue
                    try:
                        from ednews.crossref import fetch_crossref_metadata

                        # Some fetchers accept a conn kwarg; try with conn first, fall back to no-conn
                        try:
                            cr = fetch_crossref_metadata(norm, conn=conn)
                        except TypeError:
                            try:
                                cr = fetch_crossref_metadata(norm)
                            except Exception:
                                cr = None
                        except Exception:
                            cr = None
                        # Ensure we have a dict or None
                        if cr is None or not isinstance(cr, dict):
                            cr = None
                    except Exception:
                        cr = None
                    authors_val = (
                        cr.get("authors") if cr and cr.get("authors") else None
                    )
                    abstract_val = (
                        cr.get("abstract")
                        if cr and cr.get("abstract")
                        else it.get("abstract")
                    )
                    published_val = (
                        cr.get("published") if cr and cr.get("published") else None
                    )
                    aid = upsert_article(
                        conn,
                        norm,
                        title=it.get("title"),
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
                    if cr and cr.get("raw"):
                        try:
                            update_article_crossref(
                                conn,
                                norm,
                                authors=authors_val,
                                abstract=abstract_val,
                                raw=cr.get("raw"),
                                published=published_val,
                            )
                        except Exception:
                            logger.debug(
                                "Failed to update crossref data for doi=%s after upsert",
                                norm,
                            )
                except Exception:
                    logger.exception(
                        "Failed to upsert article doi=%s from journal %s", doi, issn
                    )
            conn.commit()
        except Exception:
            logger.exception("Failed to fetch works for ISSN=%s (feed=%s)", issn, key)
    logger.info("ISSN lookup summary: inserted=%d skipped=%d", inserted, skipped)
    return inserted


__all__ = ["fetch_latest_journal_works"]
