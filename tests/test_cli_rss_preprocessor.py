import sqlite3
import sys
from ednews import main as ed_main


def test_cli_uses_rss_preprocessor(monkeypatch, tmp_path):
    called = []

    # Prepare a fake feeds list with stub processor 'rss'
    def fake_load_feeds():
        return [("f1", "Feed 1", "http://example/feed", None, None, "rss")]

    monkeypatch.setattr(ed_main.feeds, "load_feeds", fake_load_feeds)

    # stub sqlite connect to use in-memory DB and init schema
    real_connect = ed_main.sqlite3.connect

    def fake_connect(path):
        return real_connect(':memory:')

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Patch the rss_preprocessor to return one fake entry
    try:
        import ednews.processors as proc_mod

        def fake_rss_pre(session, url, publication_id=None, issn=None):
            called.append('rss')
            return [{"title": "T", "link": "http://example/t", "summary": "s", "published": "2020-01-01"}]

        monkeypatch.setattr(proc_mod, "rss_preprocessor", fake_rss_pre, raising=False)
    except Exception:
        pass

    # prevent actual network by ensuring requests.Session exists
    monkeypatch.setattr(ed_main, "requests", ed_main.requests)

    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--articles"]) 
    ed_main.main()

    assert 'rss' in called
