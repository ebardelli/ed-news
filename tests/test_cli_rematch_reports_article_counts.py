import sqlite3
import sys
from datetime import datetime, timezone

from ednews import main as ed_main


def setup_db(conn):
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    # publication mapping
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f2", "pub2", "Feed 2", ""))
    # item with DOI present but no article row exists for it
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("10.0/new", "f2", "gnew", "New Title", "http://example/new", "hnew", now, now))
    conn.commit()


def test_rematch_reports_article_counters(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Patch sqlite connect to return a proxy so CLI close() doesn't close our test DB
    class ConnProxy:
        def __init__(self, inner):
            self._inner = inner

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: ConnProxy(conn))

    # Monkeypatch postprocessor to set DOI on the existing item (simulates rematch)
    try:
        import ednews.processors as proc_mod

        def fake_postprocessor(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            cur = conn_arg.cursor()
            updated = 0
            for e in entries:
                if e.get('guid') == 'gnew':
                    cur.execute("UPDATE items SET doi = ? WHERE guid = ?", ("10.0/new", 'gnew'))
                    updated += 1
            conn_arg.commit()
            return updated

        monkeypatch.setattr(proc_mod, 'crossref_postprocessor_db', fake_postprocessor, raising=False)
    except Exception:
        # proceed anyway; test will still check printed message format
        pass

    # Run rematch CLI for feed f2
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "rematch-dois", "--feed", "f2", "--publication-id", "pub2"])
    ed_main.main()

    captured = capsys.readouterr()
    out = captured.out
    assert 'articles_created=' in out
    assert 'articles_updated=' in out
