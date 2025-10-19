import sqlite3
from ednews import embeddings


def test_embed_compat_wrapper_exists():
    """Ensure the legacy embed API name exists and delegates (smoke test)."""
    conn = sqlite3.connect(":memory:")
    # create minimal articles table expected by generator
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('INSERT INTO articles (title, abstract) VALUES (?, ?)', ('t', 'a'))
    # Create a simple articles_vec table so the upsert logic can insert rows
    conn.execute('CREATE TABLE articles_vec (rowid INTEGER PRIMARY KEY, embedding BLOB)')
    conn.commit()
    # should not raise
    assert hasattr(embeddings, 'generate_and_insert_embeddings_local')
    res = embeddings.generate_and_insert_embeddings_local(conn, model=None, batch_size=1)
    assert isinstance(res, int)
    conn.close()
