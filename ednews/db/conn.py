"""Connection helpers for ednews.db"""

import logging, sqlite3

logger = logging.getLogger("ednews.db.conn")


def get_connection(path: str | None = None):
    """Return a SQLite connection.
    If `path` is provided a connection to that file path is opened; otherwise
    an in-memory connection is returned. Exceptions are propagated after being
    logged.
    """
    try:
        if path:
            logger.debug("Opening SQLite connection to path: %s", path)
            return sqlite3.connect(path)
        logger.debug("Opening in-memory SQLite connection")
        return sqlite3.connect(":memory:")
    except Exception:
        logger.exception("Failed to open SQLite connection (path=%s)", path)
        raise


__all__ = ["get_connection"]
