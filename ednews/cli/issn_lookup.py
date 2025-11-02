from ednews import feeds
import logging
from typing import Any
from .common import normalize_cli_date, get_conn

logger = logging.getLogger("ednews.cli.issn")


def cmd_issn_lookup(args: Any) -> None:
    """Fetch latest works for journals by ISSN and insert into the DB.

    Args:
        args: argparse namespace with parameters: per_journal, timeout, delay, sort_by, date_filter_type, from_date, until_date
    """
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        logger.error("No feeds found; aborting ISSN lookup")
        return
    conn = get_conn()
    from ednews.db.manage_db import fetch_latest_journal_works  # type: ignore[import]
    try:
        inserted = fetch_latest_journal_works(
            conn,
            feeds_list,
            per_journal=args.per_journal,
            timeout=args.timeout,
            delay=args.delay,
            sort_by=args.sort_by if hasattr(args, 'sort_by') else 'created',
            date_filter_type=getattr(args, 'date_filter_type', None),
            from_date=normalize_cli_date(getattr(args, 'from_date', None)),
            until_date=normalize_cli_date(getattr(args, 'until_date', None)),
        )
        logger.info("Inserted %d articles from ISSN lookups", inserted)
    except Exception:
        logger.exception("ISSN lookup failed")
    finally:
        conn.close()
