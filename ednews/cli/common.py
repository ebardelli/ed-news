"""Shared helpers for ednews CLI subcommand modules.

Provide small utilities to centralize common operations like creating a
DB connection, creating a requests session, and normalizing CLI dates.
"""
from __future__ import annotations

import sqlite3
import requests
import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from ednews import config


def get_conn() -> sqlite3.Connection:
    """Return a new sqlite3 connection to the configured DB path.

    Returns:
        sqlite3.Connection: new DB connection to `config.DB_PATH`.
    """
    return sqlite3.connect(str(config.DB_PATH))


def get_session() -> requests.Session:
    """Create and return a new requests.Session for HTTP requests."""
    return requests.Session()


def normalize_cli_date(s: Optional[str]) -> Optional[str]:
    """Normalize CLI date inputs for issn-lookup and similar commands.

    - Preserve fragments like 'YYYY', 'YYYY-MM', 'YYYY-MM-DD'.
    - If given a datetime-like string with 'T' and no timezone, treat it as
      UTC and return an ISO-formatted timezone-aware string.
    - Otherwise return the trimmed input.
    """
    if not s:
        return None
    try:
        s2 = str(s).strip()
        if re.match(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?$", s2):
            return s2
        if "T" in s2 and not re.search(r"Z|[+-]\d{2}:?\d{2}$", s2):
            try:
                dt = datetime.fromisoformat(s2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                return s2
        return s2
    except Exception:
        return s


def start_maintenance_run(conn: sqlite3.Connection, name: str, meta: Optional[Dict[str, Any]] = None) -> Tuple[Optional[str], Optional[int]]:
    """Log a maintenance run as started and return (started_iso, run_id).

    This is a thin helper that calls `ednews.db.manage_db.log_maintenance_run`
    if available. It never raises; failures are swallowed and (started, None)
    are returned so callers can proceed.
    """
    try:
        from datetime import datetime, timezone

        started = datetime.now(timezone.utc).isoformat()
        try:
            from ..db import manage_db

            run_id = manage_db.log_maintenance_run(conn, name, "started", started, None, None, meta or {})
        except Exception:
            run_id = None
        return started, run_id
    except Exception:
        return None, None


def finalize_maintenance_run(conn: sqlite3.Connection, name: str, run_id: Optional[int], started: Optional[str], status: str, details: Optional[Dict[str, Any]] = None) -> None:
    """Finalize a maintenance run log entry.

    Safe to call even if `start_maintenance_run` returned a None run_id.
    """
    try:
        from datetime import datetime

        finished = datetime.now().astimezone().isoformat()
        duration = None
        if started:
            try:
                from datetime import datetime as _dt

                duration = (_dt.fromisoformat(finished) - _dt.fromisoformat(started)).total_seconds()
            except Exception:
                duration = None
        if run_id and conn:
            try:
                from ..db import manage_db

                manage_db.log_maintenance_run(conn, name, status, started, finished, duration, details or {})
            except Exception:
                pass
    except Exception:
        pass
