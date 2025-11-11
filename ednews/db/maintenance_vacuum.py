"""Vacuum helper extracted from maintenance.py."""

import logging, sqlite3

logger = logging.getLogger("ednews.manage_db.maintenance.vacuum")


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


__all__ = ["vacuum_db"]
