import logging
from pathlib import Path
from typing import Any
from .. import feeds
from concurrent.futures import ThreadPoolExecutor, as_completed
from .common import get_conn, get_session
from typing import Any

logger = logging.getLogger("ednews.cli.fetch")


def cmd_fetch(args: Any) -> None:
    """Fetch configured article feeds and/or news headlines and persist them.

    Args:
        args: argparse namespace with .articles and .headlines boolean flags.
    """
    want_articles = getattr(args, "articles", False)
    want_headlines = getattr(args, "headlines", False)

    if not want_articles and not want_headlines:
        want_articles = True
        want_headlines = True

    feeds_list = feeds.load_feeds() if want_articles else []
    if not feeds_list:
        if want_articles:
            logger.error("No feeds found; aborting fetch")
            return
    conn = get_conn()
    try:
        from ..db import init_db

        try:
            init_db(conn)
        except Exception:
            logger.debug("failed to initialize DB schema before fetch")
    except Exception:
        logger.debug("ednews.db.init_db not importable; skipping init_db call")

    try:
        from ednews.db.manage_db import sync_publications_from_feeds  # type: ignore[import]

        try:
            synced = sync_publications_from_feeds(conn, feeds_list)
            logger.info("synced %d publications from feeds", synced)
        except Exception:
            logger.debug("failed to sync publications from feeds list")
    except Exception:
        logger.debug("failed to import sync_publications_from_feeds")

    session = get_session()
    if want_articles:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {}
            for item in feeds_list:
                if len(item) >= 4:
                    key = item[0]
                    title = item[1] if len(item) > 1 else None
                    url = item[2] if len(item) > 2 else None
                    publication_doi = item[3] if len(item) > 3 else None
                    issn = item[4] if len(item) > 4 else None
                    processor = item[5] if len(item) > 5 else None
                else:
                    continue

                if processor:
                    def run_processors_for_feed(session, conn, key, title, url, publication_doi, issn, pre_names=None, post_names=None):
                        try:
                            import importlib
                            import ednews.processors as proc_mod
                            from .. import feeds as feeds_mod

                            merged = []
                            seen = set()
                            pre_called = False
                            for name in (pre_names or []):
                                if not name:
                                    continue
                                pre_fn = getattr(proc_mod, f"{name}_feed_processor", None) or getattr(proc_mod, f"{name}_preprocessor", None)
                                if not pre_fn:
                                    try:
                                        mod = importlib.import_module(name)
                                        pre_fn = getattr(mod, f"{name}_preprocessor", None) or getattr(mod, f"{name}_feed_processor", None)
                                    except Exception:
                                        pre_fn = None
                                if not pre_fn:
                                    logger.warning("preprocessor %s not found for feed %s", name, key)
                                    continue
                                pre_called = True
                                try:
                                    entries = pre_fn(session, url, publication_id=publication_doi, issn=issn)
                                except TypeError:
                                    try:
                                        entries = pre_fn(session, url)
                                    except Exception:
                                        entries = []
                                except Exception as e:
                                    logger.exception("preprocessor %s failed for feed %s: %s", name, key, e)
                                    entries = []

                                for e in entries or []:
                                    link = (e.get('link') or '').strip()
                                    guid = (e.get('guid') or '').strip()
                                    key_id = link or guid or (e.get('title') or '')
                                    if key_id in seen:
                                        continue
                                    seen.add(key_id)
                                    merged.append(e)

                            if not pre_called:
                                try:
                                    import ednews.processors as proc_mod
                                    rss_fn = getattr(proc_mod, 'rss_preprocessor', None)
                                    if rss_fn:
                                        entries = rss_fn(session, url, publication_id=publication_doi, issn=issn)
                                    else:
                                        feed_res = feeds_mod.fetch_feed(session, key, title, url, publication_doi, issn)
                                        entries = feed_res.get('entries') or []
                                except Exception:
                                    feed_res = feeds_mod.fetch_feed(session, key, title, url, publication_doi, issn)
                                    entries = feed_res.get('entries') or []
                                merged = entries

                            return {"key": key, "title": title, "url": url, "publication_id": publication_doi, "error": None, "entries": merged, "post_processors": (post_names or [])}
                        except Exception as e:
                            return {"key": key, "title": title, "url": url, "publication_id": publication_doi, "error": str(e), "entries": []}

                    pre_names = None
                    post_names = None
                    if processor:
                        if isinstance(processor, (list, tuple)):
                            pre_names = list(processor)
                            post_names = list(processor)
                        elif isinstance(processor, dict):
                            p = processor.get('pre')
                            post = processor.get('post')
                            if isinstance(p, (list, tuple)):
                                pre_names = list(p)
                            elif isinstance(p, str):
                                pre_names = [p]
                            else:
                                pre_names = []

                            if isinstance(post, (list, tuple)):
                                post_names = list(post)
                            elif isinstance(post, str):
                                post_names = [post]
                            else:
                                post_names = []
                        else:
                            pre_names = [processor]
                            post_names = [processor]
                    else:
                        pre_names = None
                        post_names = []

                    fut = ex.submit(run_processors_for_feed, session, conn, key, title, url, publication_doi, issn, pre_names, post_names)
                    futures[fut] = (key, title, url, publication_doi)
                else:
                    fut = ex.submit(feeds.fetch_feed, session, key, title, url, publication_doi, issn)
                    futures[fut] = (key, title, url, publication_doi)

            for fut in as_completed(futures):
                meta = futures[fut]
                try:
                    res = fut.result()
                except Exception as exc:
                    logger.error("Error fetching %s: %s", meta[2], exc)
                    continue
                if res.get("error"):
                    logger.warning("Failed: %s -> %s", meta[2], res["error"])
                    continue
                try:
                    from ..feeds import save_entries

                    cnt = save_entries(conn, res["key"], res["title"], res["entries"])
                    logger.info("%s: fetched %d entries, inserted %d", res["key"], len(res["entries"]), cnt)

                    try:
                        import ednews.processors as proc_mod
                        proc_names_post = res.get('post_processors') or []
                        for name in proc_names_post:
                            if not name:
                                continue
                            post_db = getattr(proc_mod, f"{name}_postprocessor_db", None)
                            if post_db:
                                try:
                                    # Instead of passing the raw preprocessor entries, query the
                                    # `items` table and pass rows in the same shape as
                                    # `cmd_postprocess` does. This ensures DB-level
                                    # postprocessors operate on the canonical `items`
                                    # representation and behave the same when invoked
                                    # from fetch or via the postprocess CLI.
                                    cur = conn.cursor()
                                    cur.execute(
                                        "SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000",
                                        (res.get("key"),),
                                    )
                                    rows = cur.fetchall()
                                    entries_items = []
                                    for r in rows:
                                        entries_items.append({
                                            'guid': r[0],
                                            'link': r[1],
                                            'title': r[2],
                                            'published': r[3],
                                            '_fetched_at': r[4],
                                            'doi': r[5] if len(r) > 5 else None,
                                        })
                                    post_db(conn, res.get("key"), entries_items, session=session, publication_id=res.get("publication_id"), issn=res.get("_feed_issn"))
                                except Exception:
                                    logger.exception("postprocessor_db %s failed for %s", name, res.get("key"))
                            else:
                                post_mem = getattr(proc_mod, f"{name}_postprocessor", None)
                                if post_mem:
                                    try:
                                        import inspect

                                        sig = inspect.signature(post_mem)
                                        params = list(sig.parameters.keys())
                                        if params and params[0] in ("entries", "items", "rows"):
                                            post_mem(res.get("entries"), session=session, publication_id=res.get("publication_id"), issn=res.get("_feed_issn"))
                                        else:
                                            try:
                                                post_mem(conn, res.get("entries"), session=session)
                                            except Exception:
                                                post_mem(res.get("entries"))
                                    except Exception:
                                        logger.exception("postprocessor %s failed for %s", name, res.get("key"))
                    except Exception:
                        pass
                except Exception as e:
                    logger.exception("Failed to save entries for %s: %s", res.get("key"), e)
    else:
        pass

    if want_headlines:
        try:
            from ..news import fetch_all

            results = fetch_all(session=session, conn=conn)
            logger.info("Fetched headlines for %d sites", len(results))
        except Exception:
            logger.exception("Failed to fetch headlines")

    conn.close()
