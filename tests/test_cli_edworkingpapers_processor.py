import sys
from types import SimpleNamespace

from ednews import main as ed_main


class DummyConn:
    def close(self):
        pass


def test_cli_invokes_edworkingpapers_processor(monkeypatch):
    called = []

    # Prevent real DB connections
    monkeypatch.setattr(ed_main, "sqlite3", ed_main.sqlite3)

    def fake_connect(path):
        called.append(f"connect:{path}")
        return DummyConn()

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Ensure load_feeds returns our edworkingpapers entry
    def fake_load_feeds():
        # tuple: (key, title, url, publication_doi, issn, processor)
        return [("edworkingpapers", "EdWorkingPapers", "https://edworkingpapers.com/edworkingpapers", "10.26300", None, "edworkingpapers")]

    monkeypatch.setattr(ed_main.feeds, "load_feeds", fake_load_feeds)

    # Patch processor to record invocation and return sample entries
    try:
        import ednews.processors as proc_mod

        def fake_edwp_processor(session, feed_url, publication_id=None, issn=None):
            called.append(f"edworkingpapers_proc:{feed_url}")
            # Return list of entry dicts as the real feed-style processors do
            return [{"title": "A", "link": "http://example/a", "summary": "s", "published": ""}]

        monkeypatch.setattr(proc_mod, "edworkingpapers_feed_processor", fake_edwp_processor, raising=False)
    except Exception:
        pass

    # Intercept save_entries to assert it's called
    def fake_save_entries(conn, key, title, entries):
        called.append(f"save_entries:{key}")
        return len(entries)

    monkeypatch.setattr(ed_main.feeds, "save_entries", fake_save_entries)

    # Prevent actually fetching headlines; patch fetch_all used for headlines
    def fake_fetch_all(session=None, cfg_path=None, conn=None):
        called.append("fetch_headlines")
        return {}

    monkeypatch.setattr(ed_main, "fetch_all", fake_fetch_all, raising=False)

    # Run the CLI fetch articles only
    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--articles"])
    ed_main.main()

    assert any(x.startswith("edworkingpapers_proc:") for x in called), f"processor not called: {called}"
    assert any(x.startswith("save_entries:") for x in called), f"save_entries not called: {called}"
