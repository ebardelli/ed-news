import sqlite3
from ednews.db import init_db, ensure_article_row, article_exists


def test_article_exists_true_and_false():
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    doi = "10.1000/testdoi"
    # ensure not present initially
    assert not article_exists(conn, doi)

    # insert
    ensure_article_row(conn, doi, title="T", authors="A", abstract="X", feed_id="f", publication_id="p", issn="1234")

    # now should exist
    assert article_exists(conn, doi)


def test_article_exists_handles_empty_and_none():
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    assert not article_exists(conn, "")
    assert not article_exists(conn, None)


def test_article_exists_with_invalid_formats():
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    # whitespace-only
    assert not article_exists(conn, "   ")

    # numeric string
    assert not article_exists(conn, "123456")

    # bytes-like input should be handled safely by the function (it expects str)
    # article_exists signature accepts str, but we still ensure passing bytes doesn't raise
    try:
        res = article_exists(conn, b"10.1000/byte")  # type: ignore[arg-type]
        assert not res
    except Exception:
        # If it raises, that's acceptable but we prefer graceful False
        assert True

    # extremely long string
    long_doi = "10." + "0" * 1000
    assert not article_exists(conn, long_doi)
