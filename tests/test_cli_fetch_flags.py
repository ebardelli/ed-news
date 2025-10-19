import sys

from types import SimpleNamespace

from ednews import main as ed_main


class DummyConn:
    def close(self):
        pass


def _setup_monkeypatches(monkeypatch, called):
    # Prevent real DB connections
    monkeypatch.setattr(ed_main, "sqlite3", ed_main.sqlite3)

    def fake_connect(path):
        called.append(f"connect:{path}")
        return DummyConn()

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Mock feed fetcher and saver for articles
    def fake_fetch_feed(session, key, title, url, publication_doi):
        called.append(f"fetch_feed:{key}")
        return {"key": key, "title": title, "entries": [{"id": 1}], "error": None}

    # Accept flexible args to match new fetch/processor signatures
    def fake_fetch_feed_flex(*args, **kwargs):
        # args[1] is key when called as (session, key, title, url, ...)
        key = args[1] if len(args) > 1 else kwargs.get('key', 'unknown')
        called.append(f"fetch_feed:{key}")
        return {"key": key, "title": kwargs.get('title') or (args[2] if len(args) > 2 else None), "entries": [{"id": 1}], "error": None}

    def fake_save_entries(conn, key, title, entries):
        called.append(f"save_entries:{key}")
        return len(entries)

    monkeypatch.setattr(ed_main.feeds, "fetch_feed", fake_fetch_feed_flex)
    monkeypatch.setattr(ed_main.feeds, "save_entries", fake_save_entries)

    # Ensure load_feeds returns a simple feeds list without processors for these tests
    monkeypatch.setattr(ed_main.feeds, "load_feeds", lambda: [("f1", "Feed Title", "http://example.com/feed", "10.0")])

    # Patch the sciencedirect processor if invoked by CLI (new behaviour)
    try:
        import ednews.processors as proc_mod

        def fake_sciencedirect_processor(session, feed_url, publication_id=None, issn=None):
            # return a list of entries like fetch_feed would
            called.append(f"sciencedirect_proc:{feed_url}")
            return [{"guid": "g-p", "title": "P", "link": "http://example/", "published": "", "summary": ""}]

        monkeypatch.setattr(proc_mod, "sciencedirect_feed_processor", fake_sciencedirect_processor, raising=False)
    except Exception:
        pass

    # Mock headlines fetcher
    def fake_fetch_all(session=None, cfg_path=None, conn=None):
        called.append("fetch_headlines")
        return {"site1": [{"title": "hi"}]}

    monkeypatch.setattr(ed_main, "requests", ed_main.requests)
    monkeypatch.setattr(ed_main, "ThreadPoolExecutor", ed_main.ThreadPoolExecutor)
    monkeypatch.setattr(ed_main, "as_completed", ed_main.as_completed)
    monkeypatch.setattr(ed_main, "feeds", ed_main.feeds)
    monkeypatch.setattr(ed_main, "conn", None)

    # Patch the news.fetch_all used by cmd_fetch
    monkeypatch.setattr(ed_main, "fetch_all", fake_fetch_all, raising=False)
    # Also provide access via ednews.news.fetch_all if imported inside function
    try:
        import ednews.news as news_mod

        monkeypatch.setattr(news_mod, "fetch_all", fake_fetch_all)
    except Exception:
        pass


def run_main_with_args(argv):
    monkeypatch = __import__("pytest").monkeypatch
    # The tests use fixture monkeypatch; here we simulate running via sys.argv
    sys.argv[:] = argv
    ed_main.main()


def test_fetch_no_flags_runs_both(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    # Run with no flags: default should fetch both articles and headlines
    monkeypatch.setattr(sys, "argv", ["ednews", "fetch"])
    ed_main.main()
    assert any(x.startswith("fetch_feed:") for x in called)
    assert "fetch_headlines" in called


def test_fetch_articles_only(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--articles"])
    ed_main.main()
    assert any(x.startswith("fetch_feed:") for x in called)
    assert "fetch_headlines" not in called


def test_fetch_headlines_only(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--headlines"])
    ed_main.main()
    assert not any(x.startswith("fetch_feed:") for x in called)
    assert "fetch_headlines" in called
