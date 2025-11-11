"""Logging helper for maintenance runs (extracted from maintenance.py)."""

import logging, sqlite3, json

logger = logging.getLogger("ednews.manage_db.maintenance.log")


def log_maintenance_run(
    conn: sqlite3.Connection,
    command: str,
    status: str,
    started: str | None = None,
    finished: str | None = None,
    duration: float | None = None,
    details: dict | None = None,
) -> int:
    try:
        cur = conn.cursor()
        details_json = json.dumps(details, default=str) if details is not None else None
        cur.execute(
            "INSERT INTO maintenance_runs (command, status, started, finished, duration, details) VALUES (?, ?, ?, ?, ?, ?)",
            (command, status, started, finished, duration, details_json),
        )
        conn.commit()
        lr = getattr(cur, "lastrowid", None)
        return int(lr) if isinstance(lr, int) and lr is not None else 0
    except Exception:
        logger.exception("Failed to log maintenance run for command=%s", command)
        return 0


__all__ = ["log_maintenance_run"]
