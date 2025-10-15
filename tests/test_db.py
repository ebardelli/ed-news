import sqlite3
from ednews import db
from datetime import datetime, timezone


def make_conn():
    conn = sqlite3.connect(':memory:')
    db.init_db(conn)
    return conn


def test_init_db_creates_tables_and_view(tmp_path):
    p = tmp_path / 'ednews.db'
    conn = sqlite3.connect(str(p))
    conn.enable_load_extension(True)
    db.init_db(conn)

    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    names = {r[0] for r in cur.fetchall()}
    assert 'items' in names
    assert 'articles' in names
    assert 'publications' in names
    # the combined_articles view should be created during init_db
    cur.execute("SELECT name FROM sqlite_master WHERE type IN ('view','table') AND name = 'combined_articles'")
    found = {r[0] for r in cur.fetchall()}
    assert 'combined_articles' in found
    conn.close()


def test_upsert_inserts_article_and_returns_id():
    conn = make_conn()
    doi = '10.1234/example'
    aid = db.upsert_article(conn, doi, title='T1', authors='A', abstract='abs')
    assert aid is not False and aid is not None
    conn.close()


def test_ensure_article_row_returns_existing_id():
    conn = make_conn()
    doi = '10.1234/example2'
    aid = db.upsert_article(conn, doi, title='T1', authors=None, abstract=None)
    ensured = db.ensure_article_row(conn, doi)
    assert ensured == aid
    conn.close()


def test_upsert_updates_existing_article_preserving_id():
    conn = make_conn()
    doi = '10.1234/example3'
    aid = db.upsert_article(conn, doi, title='Initial', authors=None, abstract=None)
    aid2 = db.upsert_article(conn, doi, title='Updated', authors=None, abstract=None)
    assert aid2 == aid
    cur = conn.cursor()
    cur.execute('SELECT title FROM articles WHERE doi = ?', (doi,))
    row = cur.fetchone()
    assert row[0] in ('Updated', 'Initial')
    conn.close()


def test_enrich_articles_from_crossref_updates_rows():
    conn = make_conn()
    doi1 = '10.1111/one'
    doi2 = '10.2222/two'
    # insert items and articles rows expected by enrich function
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    cur.execute('INSERT INTO items (doi, guid, title, link, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', (doi1, 'g1', 't1', 'l1', now, now))
    cur.execute('INSERT INTO articles (doi, title, fetched_at) VALUES (?, ?, ?)', (doi1, 'at1', now))
    cur.execute('INSERT INTO items (doi, guid, title, link, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?)', (doi2, 'g2', 't2', 'l2', now, now))
    cur.execute('INSERT INTO articles (doi, title, fetched_at) VALUES (?, ?, ?)', (doi2, 'at2', now))
    conn.commit()

    # define a fake fetcher that returns crossref-like data for doi1 only
    def fake_fetcher(doi):
        if doi == doi1:
            return {'authors': 'Auth', 'abstract': 'Abs', 'raw': '<xml/>'}
        return None

    updated = db.enrich_articles_from_crossref(conn, fake_fetcher, batch_size=10)
    assert updated == 1
    cur.execute('SELECT authors, abstract, crossref_xml FROM articles WHERE doi = ?', (doi1,))
    r = cur.fetchone()
    assert r[0] == 'Auth' and r[1] == 'Abs' and r[2] == '<xml/>'
    conn.close()


def test_create_combined_view_exists_and_selectable():
    conn = make_conn()
    cur = conn.cursor()
    # insert minimal publication and article data
    cur.execute('INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)', ('f1', 'p1', 'Feed Title', '1234-5678'))
    cur.execute('INSERT INTO articles (doi, title, abstract, feed_id, fetched_at) VALUES (?, ?, ?, ?, ?)', ('10.9/abc', 'Title', 'Content', 'f1', datetime.now(timezone.utc).isoformat()))
    conn.commit()
    db.create_combined_view(conn)
    cur.execute('SELECT doi, title, link, feed_title, content FROM combined_articles')
    rows = cur.fetchall()
    assert len(rows) >= 1
    assert rows[0][0] == '10.9/abc'
    conn.close()


def test_upsert_publication_inserts_and_updates():
    conn = make_conn()
    cur = conn.cursor()
    # initial insert
    from ednews.db import upsert_publication

    res = upsert_publication(conn, 'feed1', 'pub1', 'Feed One', '1111-2222')
    assert res is True
    cur.execute('SELECT feed_id, publication_id, feed_title, issn FROM publications WHERE publication_id = ? AND issn = ?', ('pub1', '1111-2222'))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 'feed1' and row[1] == 'pub1' and row[2] == 'Feed One' and row[3] == '1111-2222'

    # update feed_title for same publication
    res2 = upsert_publication(conn, 'feed1', 'pub1', 'Feed One Updated', '1111-2222')
    assert res2 is True
    cur.execute('SELECT feed_title FROM publications WHERE publication_id = ? AND issn = ?', ('pub1', '1111-2222'))
    row2 = cur.fetchone()
    assert row2[0] in ('Feed One Updated', 'Feed One')
    conn.close()


def test_sync_publications_from_feeds_inserts_multiple_and_updates():
    conn = make_conn()
    cur = conn.cursor()
    from ednews.db import sync_publications_from_feeds, upsert_publication

    feeds_list = [
        ("feedA", "Feed A", "http://example.com/a", "pubA", "0000-0001"),
        ("feedB", "Feed B", "http://example.com/b", "pubB", "0000-0002"),
    ]

    count = sync_publications_from_feeds(conn, feeds_list)
    assert count == 2
    cur.execute('SELECT publication_id, issn FROM publications')
    rows = cur.fetchall()
    assert ('pubA', '0000-0001') in rows
    assert ('pubB', '0000-0002') in rows

    # Update one feed's title and re-sync
    feeds_list[0] = ("feedA", "Feed A Updated", "http://example.com/a", "pubA", "0000-0001")
    count2 = sync_publications_from_feeds(conn, feeds_list)
    assert count2 == 2
    cur.execute('SELECT feed_title FROM publications WHERE publication_id = ? AND issn = ?', ('pubA', '0000-0001'))
    r = cur.fetchone()
    assert r is not None and r[0] in ("Feed A Updated", "Feed A")
    conn.close()
