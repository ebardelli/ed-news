import sqlite3
import sys

import pytest


def test_db_init_creates_schema_and_view(tmp_path, monkeypatch):
    # Prepare a temporary DB path
    db_path = tmp_path / "test_ednews.db"
    # Ensure DB doesn't exist yet
    assert not db_path.exists()

    # Patch config.DB_PATH to point to our temporary db file
    import ednews.config as config

    monkeypatch.setattr(config, "DB_PATH", str(db_path))

    # Run the CLI command by invoking the main entrypoint
    monkeypatch.setattr(sys, "argv", ["ednews", "db-init"])
    import ednews.main as ed_main

    ed_main.main()

    # DB file should now exist
    assert db_path.exists()

    # Connect and assert tables and view exist
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()

    # Check tables
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {r[0] for r in cur.fetchall()}
    expected_tables = {"items", "articles", "publications", "headlines", "maintenance_runs"}
    assert expected_tables.issubset(tables)

    # Check that combined_articles view exists
    cur.execute("SELECT name FROM sqlite_master WHERE type='view' AND name='combined_articles'")
    row = cur.fetchone()
    assert row is not None and row[0] == "combined_articles"

    conn.close()


def test_db_init_idempotent(tmp_path, monkeypatch):
    # Prepare temporary DB path
    db_path = tmp_path / "test_ednews.db"
    import ednews.config as config

    monkeypatch.setattr(config, "DB_PATH", str(db_path))

    # Run db-init twice
    import ednews.main as ed_main
    monkeypatch.setattr(sys, "argv", ["ednews", "db-init"])
    ed_main.main()
    # Capture schema state after first init
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
    first_schema = cur.fetchall()
    conn.close()

    # Run db-init again
    monkeypatch.setattr(sys, "argv", ["ednews", "db-init"])
    ed_main.main()

    # Capture schema state after second init and compare
    conn = sqlite3.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT type, name, sql FROM sqlite_master ORDER BY type, name")
    second_schema = cur.fetchall()
    conn.close()

    assert first_schema == second_schema
