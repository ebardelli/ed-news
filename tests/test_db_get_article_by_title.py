import sqlite3


def test_get_article_by_title_matches_case_insensitive_and_trimmed():
    from ednews.db import init_db, upsert_article, get_article_by_title

    conn = sqlite3.connect(":memory:")
    init_db(conn)

    doi = "10.9999/example.title"
    stored_title = "  Adjusting strategies when reading reliable and unreliable texts "

    # Insert an article with leading/trailing whitespace in title
    upsert_article(conn, doi, title=stored_title, authors="Author A", abstract="Abs", published="2025-01-02")

    # Exact trimmed match
    res = get_article_by_title(conn, "Adjusting strategies when reading reliable and unreliable texts")
    assert res is not None
    assert res.get("doi") == doi

    # Case-insensitive match
    res2 = get_article_by_title(conn, "adjusting STRATEGIES when reading reliable and unreliable TEXTS")
    assert res2 is not None
    assert res2.get("doi") == doi

    # Non-matching title returns None
    res3 = get_article_by_title(conn, "A different title")
    assert res3 is None
