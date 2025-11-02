from .common import get_conn
import logging
from typing import Any

logger = logging.getLogger("ednews.cli.db_init")


def cmd_db_init(args: Any) -> None:
    """Initialize the database schema and views.

    Args:
        args: argparse namespace (unused)
    """
    conn = get_conn()
    try:
        from ..db import init_db

        init_db(conn)
        print("Database initialized (tables and views created)")
    except Exception:
        logger.exception("db-init failed")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass
