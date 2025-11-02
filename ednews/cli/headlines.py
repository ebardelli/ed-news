import logging
from typing import Any
from .common import get_conn, get_session

logger = logging.getLogger("ednews.cli.headlines")


def cmd_headlines(args: Any) -> None:
    """Fetch configured news sites and optionally persist or write to JSON.

    Args:
        args: argparse namespace with .out and .no_persist
    """
    from ..news import fetch_all
    import json

    session = get_session()
    conn = None
    try:
        if not getattr(args, "no_persist", False):
            conn = get_conn()
        results = fetch_all(session=session, conn=conn)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                json.dump(results, fh, ensure_ascii=False, indent=2)
            logger.info("Wrote news JSON to %s", args.out)
        else:
            logger.info("Fetched news%s", (" and persisted to DB" if conn else ""))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
