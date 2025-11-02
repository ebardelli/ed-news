import logging
from typing import Any
from .common import get_conn, get_session

logger = logging.getLogger("ednews.cli.postprocess")


def cmd_postprocess(args: Any) -> None:
    """Run a DB-level postprocessor for configured feeds or a specific feed list.

    Args:
        args: argparse namespace with .processor (required), optional .feed, .only_missing, .missing_field, .force, .check_fields
    """
    proc_name = getattr(args, 'processor', None)
    if not proc_name:
        logger.error("No processor name provided")
        return

    feeds_list = []
    try:
        from ednews import feeds as feeds_mod

        feeds_list = feeds_mod.load_feeds()
    except Exception:
        logger.debug("Could not load feeds list; proceeding with provided --feed keys only")

    feed_map = {}
    for item in feeds_list:
        if len(item) >= 3:
            key = item[0]
            title = item[1]
            publication_id = item[3] if len(item) > 3 else None
            issn = item[4] if len(item) > 4 else None
            feed_map[key] = {'title': title, 'publication_id': publication_id, 'issn': issn}

    selected_feeds = getattr(args, 'feed', None) or list(feed_map.keys())
    if not selected_feeds:
        logger.error("No feeds available to postprocess")
        return

    conn = get_conn()
    session = get_session()
    try:
        import importlib
        import ednews.processors as proc_mod

        post_fn = getattr(proc_mod, f"{proc_name}_postprocessor_db", None)
        if not post_fn:
            logger.error("Processor '%s' does not expose a %s_postprocessor_db callable", proc_name, proc_name)
            return

        cur = conn.cursor()
        force = getattr(args, 'force', False)
        check_fields_arg = getattr(args, 'check_fields', None)
        check_fields = None
        if check_fields_arg:
            check_fields = [c.strip() for c in str(check_fields_arg).split(',') if c.strip()]
        only_missing = getattr(args, 'only_missing', False)
        missing_field = getattr(args, 'missing_field', 'doi')
        allowed_missing_fields = {'doi', 'title', 'link', 'guid', 'published'}
        if only_missing and missing_field not in allowed_missing_fields:
            logger.error("missing-field '%s' not allowed; choose from %s", missing_field, sorted(list(allowed_missing_fields)))
            return
        total_updated = 0
        for fk in selected_feeds:
            try:
                pub_id = feed_map.get(fk, {}).get('publication_id')
                issn = feed_map.get(fk, {}).get('issn')
                if only_missing:
                    sql = f"SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? AND (COALESCE({missing_field}, '') = '') ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000"
                    cur.execute(sql, (fk,))
                else:
                    cur.execute("SELECT guid, link, title, published, fetched_at, doi FROM items WHERE feed_id = ? ORDER BY COALESCE(published, fetched_at) DESC LIMIT 2000", (fk,))
                rows = cur.fetchall()
                entries = []
                for r in rows:
                    entries.append({'guid': r[0], 'link': r[1], 'title': r[2], 'published': r[3], '_fetched_at': r[4], 'doi': r[5] if len(r) > 5 else None})

                if not entries:
                    logger.info("No items found for feed %s; skipping", fk)
                    continue

                logger.info("Running postprocessor %s for feed %s (items=%d)", proc_name, fk, len(entries))
                try:
                    try:
                        updated = post_fn(conn, fk, entries, session=session, publication_id=pub_id, issn=issn, force=force, check_fields=check_fields)
                    except TypeError:
                        updated = post_fn(conn, fk, entries, session=session, publication_id=pub_id, issn=issn)
                    if isinstance(updated, int):
                        total_updated += updated
                except Exception:
                    logger.exception("Postprocessor %s failed for feed %s", proc_name, fk)
            except Exception:
                logger.exception("Failed to postprocess feed %s", fk)

        logger.info("Postprocessor %s completed; total updated rows: %s", proc_name, total_updated)
    finally:
        try:
            conn.close()
        except Exception:
            pass
