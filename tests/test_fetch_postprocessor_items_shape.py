import sqlite3
import sys
import types
import importlib

# Avoid importing heavy build dependencies (duckdb) during test collection by
# stubbing out `ednews.cli.build` before importing the package's CLI module.
fake_build = types.ModuleType("ednews.cli.build")
setattr(fake_build, "cmd_build", lambda args: None)
sys.modules["ednews.cli.build"] = fake_build

# Import ednews.main after stubbing
from ednews import main as ed_main


def test_fetch_invokes_postprocessor_with_items_rows(monkeypatch):
    called = {}

    # Fake load_feeds to include edwp feed with edworkingpapers processor
    def fake_load_feeds():
        # tuple: (key, title, url, publication_id, issn, processor)
        return [("edwp", "EdWorkingPapers", "https://edworkingpapers.com/edworkingpapers", None, None, {"pre": "edworkingpapers", "post": "edworkingpapers"})]

    monkeypatch.setattr(ed_main.feeds, "load_feeds", fake_load_feeds)

    # Force sqlite connect to in-memory DB for tests
    real_connect = ed_main.sqlite3.connect

    def fake_connect(path):
        return real_connect(':memory:')

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Monkeypatch the edworkingpapers preprocessor to return one entry
    try:
        import ednews.processors as proc_mod

        def fake_pre(session, url, publication_id=None, issn=None):
            # return entry with guid, link, title, published, summary
            return [{"guid": "g1", "link": "https://edworkingpapers.com/ai25-1234", "title": "Test Paper", "published": "2025-01-01", "summary": "s"}]

        def fake_post_db(conn, feed_key, entries, session=None, publication_id=None, issn=None, **kwargs):
            # capture what was passed in
            called['feed_key'] = feed_key
            called['entries'] = entries
            # ensure postprocessor reports one updated
            return 1

        # fetch.run looks for a <name>_feed_processor first; patch that symbol
        monkeypatch.setattr(proc_mod, "edworkingpapers_feed_processor", fake_pre, raising=False)
        monkeypatch.setattr(proc_mod, "edworkingpapers_postprocessor_db", fake_post_db, raising=False)
    except Exception:
        pass

    # Prevent external network calls by ensuring requests.Session exists
    monkeypatch.setattr(ed_main, "requests", ed_main.requests)

    # Run the fetch CLI
    import sys

    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--articles"]) 
    ed_main.main()

    # Assertions
    assert called.get('feed_key') == 'edwp'
    entries = called.get('entries') or []
    assert isinstance(entries, list)
    assert len(entries) == 1
    e = entries[0]
    # The entries passed by fetch should be rows from `items` with these keys
    for k in ('guid', 'link', 'title', 'published', '_fetched_at', 'doi'):
        assert k in e, f"expected key {k} in postprocessor entry, got: {list(e.keys())}"
