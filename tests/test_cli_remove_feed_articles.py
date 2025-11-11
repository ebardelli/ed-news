import sqlite3
import sys
from datetime import datetime, timezone

import pytest

from ednews import main as ed_main


def setup_articles(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # three articles across two feeds and two publications
    cur.execute("INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)", ('10.0/aa', 'A', 'feedA', 'pubX', now))
    cur.execute("INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)", ('10.0/bb', 'B', 'feedB', 'pubX', now))
    cur.execute("INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)", ('10.0/cc', 'C', 'feedB', 'pubY', now))
    conn.commit()


def make_conn_proxy(conn):
    class ConnProxy:
        def __init__(self, inner):
            self._inner = inner

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._inner, name)

    return ConnProxy(conn)


def test_cli_remove_feed_articles_dry_run(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_articles(conn)

    # Patch sqlite connect to return proxy so CLI close() doesn't close our DB
    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: make_conn_proxy(conn))

    # Run CLI dry-run for feedB
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "remove-feed-articles", "--feed", "feedB", "--dry-run"])
    ed_main.main()

    # Ensure original rows still present
    cur = conn.cursor()
    cur.execute("SELECT doi FROM articles ORDER BY doi ASC")
    rows = [r[0] for r in cur.fetchall()]
    assert set(rows) == {'10.0/aa', '10.0/bb', '10.0/cc'}


def test_cli_remove_feed_articles_actual(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_articles(conn)

    # Patch sqlite connect to return proxy so CLI close() doesn't close our DB
    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: make_conn_proxy(conn))

    # Run CLI actual delete for publication pubX
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "remove-feed-articles", "--publication-id", "pubX"])
    ed_main.main()

    # Ensure articles with publication_id pubX are gone
    cur = conn.cursor()
    cur.execute("SELECT doi, publication_id FROM articles ORDER BY doi ASC")
    rows = cur.fetchall()
    assert all(r[1] != 'pubX' for r in rows)
