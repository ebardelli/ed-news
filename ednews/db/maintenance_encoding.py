"""Encoding repair helpers for stored database text."""

import logging
import sqlite3

from ..text import recover_mojibake

logger = logging.getLogger("ednews.manage_db.maintenance.encoding")


REPAIR_TARGETS: tuple[tuple[str, str, str], ...] = (
    ("articles", "id", "title"),
    ("articles", "id", "authors"),
    ("articles", "id", "abstract"),
    ("items", "id", "title"),
    ("items", "id", "summary"),
    ("headlines", "id", "source"),
    ("headlines", "id", "title"),
    ("headlines", "id", "text"),
    ("publications", "rowid", "feed_title"),
)


def repair_text_encoding(conn: sqlite3.Connection, dry_run: bool = False) -> dict:
    """Repair mojibake in selected text columns.

    Returns a summary dict containing total updated rows and per-column counts.
    The same row may be counted more than once when multiple columns change.
    """
    cur = conn.cursor()
    summary: dict[str, object] = {
        "dry_run": bool(dry_run),
        "total_updates": 0,
        "by_column": {},
        "samples": {},
    }

    for table, key_col, text_col in REPAIR_TARGETS:
        cur.execute(
            f"SELECT {key_col}, {text_col} FROM {table} WHERE COALESCE({text_col}, '') != ''"
        )
        rows = cur.fetchall()
        changed: list[tuple[object, str, str]] = []
        for row_id, value in rows:
            if not isinstance(value, str):
                continue
            repaired = recover_mojibake(value)
            if repaired != value:
                changed.append((row_id, value, repaired))

        key = f"{table}.{text_col}"
        summary["by_column"][key] = len(changed)
        if changed:
            summary["samples"][key] = [
                {
                    "row_id": row_id,
                    "before": before[:160],
                    "after": after[:160],
                }
                for row_id, before, after in changed[:3]
            ]
        summary["total_updates"] = int(summary["total_updates"]) + len(changed)

        if dry_run or not changed:
            continue

        cur.executemany(
            f"UPDATE {table} SET {text_col} = ? WHERE {key_col} = ?",
            [(after, row_id) for row_id, _before, after in changed],
        )

    if not dry_run:
        conn.commit()

    logger.info(
        "repair_text_encoding dry_run=%s total_updates=%s",
        dry_run,
        summary["total_updates"],
    )
    return summary


__all__ = ["repair_text_encoding", "REPAIR_TARGETS"]