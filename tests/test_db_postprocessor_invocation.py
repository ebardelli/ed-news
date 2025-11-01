import sqlite3
from types import SimpleNamespace

from ednews import main as ed_main


def test_db_postprocessor_runs(monkeypatch, tmp_path):
    called = []

    # prepare a fake feeds list with a fake feed that uses 'sciencedirect' processor
    def fake_load_feeds():
        return [("lni", "Learning and Instruction", "http://example/feed", "10.1016/j.learninstruc", None, "sciencedirect")]

    monkeypatch.setattr(ed_main.feeds, "load_feeds", fake_load_feeds)

    # stub sqlite connect to use in-memory DB and init schema
    # save the real connect so our fake can call it without recursion
    real_connect = ed_main.sqlite3.connect

    def fake_connect(path):
        # always return in-memory DB by calling the real sqlite3 connect
        return real_connect(':memory:')

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Patch the sciencedirect_preprocessor to return one fake entry
    try:
        import ednews.processors as proc_mod

        def fake_sciencedirect_pre(session, url, publication_id=None, issn=None):
            called.append('pre')
            return [{"title": "T", "link": "http://example/t", "summary": "s", "published": ""}]

        def fake_sciencedirect_post_db(conn, feed_key, entries, session=None, publication_id=None, issn=None):
            called.append('post_db')
            # pretend to attach DOI by writing to a dummy table; just return 1
            return 1

        # ensure legacy feed_processor isn't used (which would attempt network)
        try:
            monkeypatch.setattr(proc_mod, "sciencedirect_feed_processor", None, raising=False)
        except Exception:
            pass
        monkeypatch.setattr(proc_mod, "sciencedirect_preprocessor", fake_sciencedirect_pre, raising=False)
        monkeypatch.setattr(proc_mod, "sciencedirect_postprocessor_db", fake_sciencedirect_post_db, raising=False)
    except Exception:
        pass

    # prevent actual network by ensuring requests.Session exists
    monkeypatch.setattr(ed_main, "requests", ed_main.requests)

    # run CLI fetch --articles
    import sys
    monkeypatch.setattr(sys, "argv", ["ednews", "fetch", "--articles"]) 
    ed_main.main()

    assert 'pre' in called
    assert 'post_db' in called
