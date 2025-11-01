import sqlite3
from ednews.processors import sciencedirect_postprocessor_db
from ednews.db.schema import init_db


def test_sciencedirect_postprocessor_upserts_article_and_attaches_item(tmp_path):
    conn = sqlite3.connect(':memory:')
    init_db(conn)
    cur = conn.cursor()
    # insert a dummy item representing a saved feed item
    link = 'http://example/article1'
    guid = 'gid-1'
    cur.execute("INSERT INTO items (feed_id, guid, title, link, url_hash, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)", ('feed1', guid, 'Title', link, 'uhash', '2020-01-01', '2020-01-01'))
    conn.commit()

    entries = [
        {
            'title': 'Test Article',
            'link': link,
            'guid': guid,
            'doi': '10.1234/example.doi',
            'published': '2020-01-01',
        }
    ]

    updated = sciencedirect_postprocessor_db(conn, 'feed1', entries, session=None, publication_id=None, issn=None)
    # Should have upserted the article
    assert updated >= 1

    cur.execute("SELECT id, doi FROM articles WHERE doi = ?", ('10.1234/example.doi',))
    row = cur.fetchone()
    assert row is not None
    # Item should have been updated to attach the DOI
    cur.execute("SELECT doi FROM items WHERE guid = ?", (guid,))
    item_row = cur.fetchone()
    assert item_row is not None and item_row[0] == '10.1234/example.doi'
    conn.close()
