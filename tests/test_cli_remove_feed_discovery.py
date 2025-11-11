import sqlite3
import sys
from datetime import datetime, timezone

from ednews import main as ed_main


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publication mapping for feed 'f_ok'
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ('f_ok', '10.1111', 'OK', ''))
    # Articles: one for unmapped feed f_unmapped, two for f_ok where one DOI mismatches
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.1111/keep', 'Keep', 'f_ok', '10.1111', now))
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.2222/wrong', 'Wrong', 'f_ok', '10.1111', now))
    cur.execute('INSERT INTO articles (doi, title, feed_id, publication_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.3333/a', 'A', 'f_unmapped', None, now))
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


def test_cli_discovery_and_cleanup(monkeypatch):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # patch sqlite connect used by CLI
    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: make_conn_proxy(conn))

    # Monkeypatch feeds.load_feeds to return f_unmapped and f_ok mapping
    import ednews.feeds as feeds_mod

    def fake_load_feeds():
        # f_unmapped present with no publication_id, f_ok present with publication_id
        return [('f_unmapped', 'Unmapped', 'http://x', None, None, None), ('f_ok', 'OK', 'http://x', '10.1111', None, None)]

    monkeypatch.setattr(feeds_mod, 'load_feeds', fake_load_feeds)

    # Run CLI manage-db remove-feed-articles with no args to trigger discovery
    monkeypatch.setattr(sys, 'argv', ['ednews', 'manage-db', 'remove-feed-articles'])
    ed_main.main()

    # Ensure remaining articles don't include the ones that should be deleted
    cur = conn.cursor()
    cur.execute("SELECT doi, feed_id FROM articles ORDER BY doi ASC")
    rows = cur.fetchall()
    dois = {r[0] for r in rows}
    assert '10.3333/a' not in dois  # from f_unmapped should be deleted
    assert '10.2222/wrong' not in dois  # mismatched DOI for f_ok should be deleted
    assert '10.1111/keep' in dois
