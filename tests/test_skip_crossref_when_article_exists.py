import sqlite3


def test_skip_crossref_fetch_when_article_exists(monkeypatch):
    """Ensure we do not call Crossref fetch when the DOI already exists in DB.

    This exercises the `save_entries` flow which previously would call
    `crossref.fetch_crossref_metadata` for any DOI. After the change, when an
    article row exists for the DOI we should load stored metadata instead and
    not call the network fetch.
    """
    from ednews.db import init_db, upsert_article
    from ednews.feeds import save_entries
    import ednews.crossref as crossref_mod

    conn = sqlite3.connect(":memory:")
    # initialize schema
    init_db(conn)

    doi = "10.1234/existdoi"
    # insert an existing article row so article_exists() will be true
    upsert_article(conn, doi, title="Existing", authors="A", abstract="B", published="2025-01-01")

    called = {"count": 0}

    def fake_fetch(doi_arg, timeout=10):
        called["count"] += 1
        raise AssertionError("fetch_crossref_metadata should NOT be called when DOI exists in DB")

    # Patch the Crossref fetch function; it must not be called.
    monkeypatch.setattr(crossref_mod, "fetch_crossref_metadata", fake_fetch)

    entry = {
        "guid": "g1",
        "title": "Test Title",
        "link": "https://example.com/article",
        "published": "2025-01-01",
        "summary": "summary",
        "doi": doi,
    }

    inserted = save_entries(conn, "feed1", "Feed 1", [entry])

    assert called["count"] == 0, "Crossref fetch was unexpectedly called"
    assert inserted == 1
