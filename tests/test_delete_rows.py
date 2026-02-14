import runpy
import sqlite3
import sys
from pathlib import Path


def _create_db(path: str):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    cur.execute("CREATE TABLE testtable (id INTEGER PRIMARY KEY, name TEXT)")
    cur.executemany("INSERT INTO testtable (id, name) VALUES (?, ?)", [(1, 'one'), (2, 'two'), (3, 'three'), (4, 'four')])
    conn.commit()
    conn.close()


def test_delete_rows_removes_specified_ids(tmp_path, monkeypatch):
    db = tmp_path / "ednews.db"
    _create_db(str(db))

    monkeypatch.setattr(sys, "argv", ["delete_rows.py", "--db", str(db), "--table", "testtable", "--ids", "1,3"])
    runpy.run_path("scripts/delete_rows.py", run_name="__main__")

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    cur.execute("SELECT id FROM testtable ORDER BY id")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()

    assert rows == [2, 4]


def test_dry_run_does_not_commit(tmp_path, monkeypatch, capsys):
    db = tmp_path / "ednews2.db"
    _create_db(str(db))

    monkeypatch.setattr(sys, "argv", ["delete_rows.py", "--db", str(db), "--table", "testtable", "--ids", "2,4", "--dry-run"])
    runpy.run_path("scripts/delete_rows.py", run_name="__main__")

    captured = capsys.readouterr()
    assert "Dry-run" in captured.out

    conn = sqlite3.connect(str(db))
    cur = conn.cursor()
    cur.execute("SELECT id FROM testtable ORDER BY id")
    rows = [r[0] for r in cur.fetchall()]
    conn.close()

    assert rows == [1, 2, 3, 4]
