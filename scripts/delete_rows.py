#!/usr/bin/env python3
"""Delete rows from a SQLite table by id list.

Usage examples:
  python3 scripts/delete_rows.py --db ednews.db --table articles --ids 1,2,3
  python3 scripts/delete_rows.py --db ednews.db --table articles --ids "1 2 3"
"""
import argparse
import sqlite3
import re
import sys
from typing import List


def parse_ids(raw: str) -> List[int]:
    parts = re.split(r"[,\s]+", raw.strip())
    ids = []
    for p in parts:
        if not p:
            continue
        try:
            ids.append(int(p))
        except ValueError:
            raise ValueError(f"Invalid id value: {p!r}")
    return ids


def valid_table_name(name: str) -> bool:
    return re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", name) is not None


def main():
    p = argparse.ArgumentParser(description="Delete rows from a SQLite table by id list")
    p.add_argument("--db", default="ednews.db", help="Path to SQLite database file")
    p.add_argument("--table", required=True, help="Table name to delete rows from")
    p.add_argument("--ids", required=True, help="Comma- or space-separated list of ids to delete")
    p.add_argument("--id-column", default="id", help="ID column name (default: id)")
    p.add_argument("--dry-run", action="store_true", help="Don't commit changes; show what would be deleted")

    args = p.parse_args()

    if not valid_table_name(args.table):
        print("Invalid table name. Allowed: letters, digits and underscore, must start with a letter or underscore.")
        sys.exit(2)

    if not valid_table_name(args.id_column):
        print("Invalid id column name. Allowed: letters, digits and underscore, must start with a letter or underscore.")
        sys.exit(2)

    try:
        ids = parse_ids(args.ids)
    except ValueError as e:
        print(e)
        sys.exit(2)

    if not ids:
        print("No ids provided after parsing.")
        sys.exit(2)

    placeholders = ",".join(["?" for _ in ids])
    sql = f"DELETE FROM \"{args.table}\" WHERE \"{args.id_column}\" IN ({placeholders})"

    conn = sqlite3.connect(args.db)
    try:
        before = conn.total_changes
        cur = conn.execute(sql, ids)
        if args.dry_run:
            conn.rollback()
            print(f"Dry-run: would delete rows with ids={ids} from table {args.table}")
        else:
            conn.commit()
            after = conn.total_changes
            deleted = after - before
            print(f"Deleted {deleted} rows from {args.table}")
    except sqlite3.Error as e:
        print(f"SQLite error: {e}")
        sys.exit(3)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
