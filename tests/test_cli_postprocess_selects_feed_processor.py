import sqlite3
from types import SimpleNamespace

from ednews.cli.postprocess import cmd_postprocess


def test_cmd_postprocess_prefers_feed_processor(monkeypatch, tmp_path):
    called = []

    # Fake feeds list: one feed 'edwp' configured to use 'edworkingpapers'
    def fake_load_feeds():
        return [("edwp", "Ed Working Papers", "http://example/feed", "10.1234/edwp", None, "edworkingpapers")]

    monkeypatch.setattr('ednews.feeds.load_feeds', fake_load_feeds)

    # Create an in-memory DB and minimal items table with one item for feed 'edwp'
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE items (
            guid TEXT,
            link TEXT,
            title TEXT,
            published TEXT,
            fetched_at TEXT,
            doi TEXT,
            feed_id TEXT
        )
        """
    )
    cur.execute("INSERT INTO items (guid, link, title, published, fetched_at, doi, feed_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ('g1', 'http://example/1', 'T', '', '', None, 'edwp'))
    conn.commit()

    # Ensure cmd_postprocess uses our in-memory connection
    monkeypatch.setattr('ednews.cli.postprocess.get_conn', lambda: conn)
    monkeypatch.setattr('ednews.cli.postprocess.get_session', lambda: None)

    # Patch processors: feed-specific should be called; crossref should NOT
    try:
        import ednews.processors as proc_mod

        def fake_edwp_post(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            called.append('edwp')
            # pretend one update
            return 1

        def fake_crossref_post(conn_arg, feed_key, entries, session=None, publication_id=None, issn=None, force=False, check_fields=None):
            called.append('crossref')
            return 0

        monkeypatch.setattr(proc_mod, 'edworkingpapers_postprocessor_db', fake_edwp_post, raising=False)
        monkeypatch.setattr(proc_mod, 'crossref_postprocessor_db', fake_crossref_post, raising=False)
    except Exception:
        # If processors module can't be imported for some reason, still proceed
        pass

    # Build args: user passed --processor crossref, but feed config should override
    args = SimpleNamespace(processor='crossref', feed=None, only_missing=False, missing_field='doi', force=False, check_fields=None)

    # Run the CLI handler
    cmd_postprocess(args)

    # Assert feed-specific postprocessor was called and crossref wasn't
    assert 'edwp' in called, f"expected edworkingpapers postprocessor to be called; got {called}"
    assert 'crossref' not in called, f"crossref should not have been called when feed-specific processor is configured; got {called}"
