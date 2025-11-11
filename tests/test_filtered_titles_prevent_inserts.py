import sqlite3
from ednews import db as eddb


def test_upsert_article_skips_filtered_title():
    conn = sqlite3.connect(':memory:')
    eddb.init_db(conn)

    # Use a DOI but filtered title
    doi = '10.1000/filtered.1'
    title = 'Editorial Board'  # appears in config.TITLE_FILTERS
    res = eddb.upsert_article(conn, doi, title=title, authors=None, abstract=None, feed_id='f1')
    # upsert should return Falsey and not insert
    assert not res

    cur = conn.cursor()
    cur.execute("SELECT doi, title FROM articles WHERE doi = ?", (doi,))
    row = cur.fetchone()
    assert row is None

    conn.close()


def test_ensure_article_row_skips_filtered_title():
    conn = sqlite3.connect(':memory:')
    eddb.init_db(conn)

    doi = '10.1000/filtered.2'
    title = '  Editorial Board  '  # whitespace should be normalized and match
    res = eddb.ensure_article_row(conn, doi, title=title, authors=None, abstract=None, feed_id='f1')
    assert res is None

    cur = conn.cursor()
    cur.execute("SELECT doi, title FROM articles WHERE doi = ?", (doi,))
    row = cur.fetchone()
    assert row is None

    conn.close()
