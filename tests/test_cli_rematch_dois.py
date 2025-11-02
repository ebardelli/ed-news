import sqlite3
import sys
from datetime import datetime, timezone

import pytest

from ednews import main as ed_main


def setup_db(conn):
    # Initialize schema and insert a publication + items + article
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()
    # publication maps publication_id -> feed_id
    cur.execute("INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)", ("f1", "pubid", "Feed Title", ""))
    # insert items with DOIs
    now = datetime.now(timezone.utc).isoformat()
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("10.0/wrong", "f1", "g1", "T1", "http://example/1", "h1", now, now))
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", ("10.0/right", "f1", "g2", "T2", "http://example/2", "h2", now, now))
    # insert articles for those dois (simulate an existing wrong article)
    cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("10.0/wrong", "Wrong", "A", "abs", None, "f1", "pubid", None, now, now))
    cur.execute("INSERT INTO articles (doi, title, authors, abstract, crossref_xml, feed_id, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ("10.0/right", "Right", "B", "abs2", None, "f1", "pubid", None, now, now))
    conn.commit()


def run_cli_with_args(argv):
    monkeypatch = __import__("pytest").monkeypatch
    monkeypatch.setattr(sys, "argv", argv)
    # call the main entrypoint
    ed_main.main()


def test_rematch_dois_dry_run(monkeypatch, tmp_path):
    # Use a real in-memory DB, but patch sqlite3.connect used by CLI to return it
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # patch ednews.main.sqlite3.connect to return a proxy whose close() is a no-op
    class ConnProxy:
        def __init__(self, inner):
            self._inner = inner

        def close(self):
            # noop so tests can inspect the underlying connection after CLI closes
            return None

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: ConnProxy(conn))

    # Run dry-run
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "rematch-dois", "--publication-id", "pubid", "--dry-run"])
    ed_main.main()

    # Ensure nothing was cleared
    cur = conn.cursor()
    cur.execute("SELECT COUNT(1) FROM items WHERE feed_id = ? AND COALESCE(doi, '') != ''", ("f1",))
    row = cur.fetchone()
    assert row and row[0] == 2


def test_rematch_dois_actual_run(monkeypatch, tmp_path):
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

    # Monkeypatch the crossref_postprocessor_db to simulate matching the right DOI for guid g1
    try:
        import ednews.processors as proc_mod

        def fake_crossref_postprocessor_db(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            # For each entry without a doi, set a doi based on guid to simulate re-matching
            cur = conn_arg.cursor()
            updated = 0
            for e in entries:
                guid = e.get('guid')
                if guid == 'g1':
                    doi = '10.0/right'
                elif guid == 'g2':
                    doi = '10.0/right'
                else:
                    doi = None
                if doi:
                    # update items rows
                    link = (e.get('link') or '').strip()
                    if link:
                        cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND link = ?", (doi, feed_key, link))
                    if e.get('guid'):
                        cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", (doi, feed_key, e.get('guid')))
                    updated += 1
            conn_arg.commit()
            return updated

        monkeypatch.setattr(proc_mod, "crossref_postprocessor_db", fake_crossref_postprocessor_db, raising=False)
    except Exception:
        # If processors module can't be imported, ensure CLI still runs
        pass

    # Run actual rematch
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "rematch-dois", "--publication-id", "pubid"])
    ed_main.main()

    # Now items should have DOIs reassigned to 10.0/right for both rows
    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE feed_id = ? ORDER BY guid ASC", ("f1",))
    rows = [r[0] for r in cur.fetchall()]
    assert all(r == '10.0/right' for r in rows)


def test_rematch_forces_lookup_when_doi_exists(monkeypatch, tmp_path):
    """Ensure rematch forces postprocessor lookup when a DOI already exists and is wrong."""
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Patch sqlite connect to return proxy
    class ConnProxy:
        def __init__(self, inner):
            self._inner = inner

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: ConnProxy(conn))

    # Install a fake postprocessor that records whether force=True is passed
    called = {"force_values": []}
    try:
        import ednews.processors as proc_mod

        def fake_crossref_postprocessor_db(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            called["force_values"].append(bool(force))
            # pretend to correct the DOI for g1 even if it existed
            cur = conn_arg.cursor()
            for e in entries:
                if e.get('guid') == 'g1':
                    cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", ('10.1162/edfp.12345', feed_key, 'g1'))
            conn_arg.commit()
            return 1

        monkeypatch.setattr(proc_mod, "crossref_postprocessor_db", fake_crossref_postprocessor_db, raising=False)
    except Exception:
        pytest.skip("processors module not importable")

    # Run rematch (actual run, not dry-run)
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "rematch-dois", "--publication-id", "pubid"])
    ed_main.main()

    # Verify the fake postprocessor was invoked and was asked to force a lookup
    assert called["force_values"], "postprocessor was not called"
    assert any(called["force_values"]) is True

    # Verify the item DOI changed to the edfp-prefixed DOI
    cur = conn.cursor()
    cur.execute("SELECT doi FROM items WHERE guid = ?", ("g1",))
    row = cur.fetchone()
    assert row and row[0].startswith('10.1162/edfp')


def test_rematch_only_wrong_cli_passes_only_wrong_items(monkeypatch, tmp_path):
    """Ensure that running the CLI with --only-wrong passes only the wrong/missing DOI items
    to the postprocessor (so title lookups will be attempted for them).
    """
    conn = sqlite3.connect(":memory:")
    setup_db(conn)

    # Add an item with missing DOI to be rematched
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.cursor()
    cur.execute("INSERT INTO items (doi, feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", (None, "f1", "g3", "T3", "http://example/3", "h3", now, now))
    conn.commit()

    # Patch sqlite connect to return proxy
    class ConnProxy:
        def __init__(self, inner):
            self._inner = inner

        def close(self):
            return None

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(ed_main.sqlite3, "connect", lambda path: ConnProxy(conn))

    # Capture the entries passed to the postprocessor
    captured = {"entries": None}
    try:
        import ednews.processors as proc_mod

        def fake_crossref_postprocessor_db(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            captured["entries"] = list(entries)
            return 0

        monkeypatch.setattr(proc_mod, "crossref_postprocessor_db", fake_crossref_postprocessor_db, raising=False)
    except Exception:
        pytest.skip("processors module not importable")

    # Run CLI with only-wrong flag
    import sys as _sys
    # Target the feed directly and provide a publication_id that matches g2
    monkeypatch.setattr(_sys, "argv", ["ednews", "manage-db", "rematch-dois", "--feed", "f1", "--publication-id", "10.0/right", "--only-wrong"]) 
    ed_main.main()

    # Check that captured entries contains only the guids for wrong or missing DOIs (g1 and g3)
    assert captured["entries"] is not None
    guids = sorted([e.get('guid') for e in captured["entries"] if e.get('guid')])
    assert guids == ['g1', 'g3']
