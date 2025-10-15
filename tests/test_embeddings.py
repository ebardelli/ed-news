import sqlite3
import sqlite_vec
from ednews import db
from ednews import embeddings
import types
import pytest

@pytest.fixture
def in_memory_db(monkeypatch):
    """Create an in-memory sqlite DB and monkeypatch sqlite_vec.load to noop.

    Tests may further monkeypatch serialization and embedding behavior.
    """
    import sqlite_vec as sv

    monkeypatch.setattr(sv, 'load', lambda conn=None: None)

    conn = sqlite3.connect(':memory:')
    conn.enable_load_extension(True)
    try:
        yield conn
    finally:
        conn.close()

def test_embeddings_create_database_in_memory(in_memory_db):
    """Ensure create_database runs without error in-memory (vec extension may be absent)."""
    conn = in_memory_db

    # create articles table expected by embeddings
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('INSERT INTO articles (title, abstract) VALUES (?, ?)', ('t', 'a'))
    conn.commit()

    # Run create_database to ensure virtual table creation (will attempt to call sqlite_vec.load but patched)
    embeddings.create_database(conn)
    # Ensure that executing SELECT name FROM sqlite_master doesn't crash (table may not exist if vec extension absent)
    cur = conn.cursor()
    cur.execute('SELECT name FROM sqlite_master WHERE name = "articles_vec"')
    _ = cur.fetchall()

def test_generate_and_insert_embeddings_for_ids_writes_embedding(monkeypatch, in_memory_db):
    """Ensure generate_and_insert_embeddings_for_ids writes an embedding row for provided ids."""
    import sqlite_vec as sv
    from ednews import embeddings as emb

    # patch sqlite_vec.load to no-op
    monkeypatch.setattr(sv, 'load', lambda conn=None: None)

    # patch nomic.embed.text to return deterministic embeddings that change between calls
    call_count = {'n': 0}

    class FakeEmbedModule:
        @staticmethod
        def text(texts, task_type, model, inference_mode):
            # increment call counter so subsequent calls return different embeddings
            call_count['n'] += 1
            base = 0.1 * call_count['n']
            # produce a list where first element encodes call number, rest zeros
            return {'embeddings': [[base] + [0.0] * 767 for _ in texts]}

    monkeypatch.setattr('ednews.embeddings.embed', FakeEmbedModule)

    # serialize_float32 will produce a small bytes value derived from the first float
    def fake_serialize(arr):
        try:
            first = float(arr[0])
        except Exception:
            first = 0.0
        b = int(first * 10) % 256
        return bytes([b])

    monkeypatch.setattr(sv, 'serialize_float32', fake_serialize)

    conn = in_memory_db

    # create articles and a simple articles_vec table that accepts inserts
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('CREATE TABLE articles_vec (rowid INTEGER PRIMARY KEY, embedding BLOB)')
    # insert an article
    conn.execute('INSERT INTO articles (title, abstract) VALUES (?, ?)', ('test title', 'test abstract'))
    conn.commit()

    # fetch the inserted id
    cur = conn.cursor()
    cur.execute('SELECT id FROM articles LIMIT 1')
    row = cur.fetchone()
    aid = row[0]

    written = emb.generate_and_insert_embeddings_for_ids(conn, [aid])
    assert written == 1

    # ensure the row was inserted into articles_vec
    cur.execute('SELECT rowid, embedding FROM articles_vec WHERE rowid = ?', (aid,))
    row2 = cur.fetchone()
    assert row2 is not None
    assert row2[0] == aid
    assert isinstance(row2[1], (bytes, bytearray))

    # store the first embedding blob
    first_blob = row2[1]

    # modify article text and regenerate embedding; fake embed will return a new blob
    conn.execute('UPDATE articles SET abstract = ? WHERE id = ?', ('changed abstract', aid))
    conn.commit()

    written2 = emb.generate_and_insert_embeddings_for_ids(conn, [aid])
    assert written2 == 1

    cur.execute('SELECT embedding FROM articles_vec WHERE rowid = ?', (aid,))
    row3 = cur.fetchone()
    assert row3 is not None
    second_blob = row3[0]
    assert first_blob != second_blob

    conn.close()


def test_generate_and_insert_embeddings_for_ids_batch(monkeypatch, in_memory_db):
    """Ensure batch generation writes embeddings for multiple ids."""
    import sqlite_vec as sv
    from ednews import embeddings as emb

    # patch sqlite_vec.load to no-op
    monkeypatch.setattr(sv, 'load', lambda conn=None: None)

    # fake serializer encodes a small marker from first float
    def fake_serialize(arr):
        try:
            first = float(arr[0])
        except Exception:
            first = 0.0
        b = int(first * 10) % 256
        return bytes([b])

    monkeypatch.setattr(sv, 'serialize_float32', fake_serialize)

    # fake embed returns different first element per text so serialized blobs differ
    def fake_text(texts, task_type, model, inference_mode):
        out = {'embeddings': []}
        for i, _ in enumerate(texts):
            # each embedding differs by index
            first = 0.1 * (i + 1)
            emb_vec = [first] + [0.0] * 767
            out['embeddings'].append(emb_vec)
        return out

    monkeypatch.setattr('ednews.embeddings.embed', types.SimpleNamespace(text=fake_text))

    conn = in_memory_db

    # create tables
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('CREATE TABLE articles_vec (rowid INTEGER PRIMARY KEY, embedding BLOB)')

    # insert multiple articles
    articles = [
        ('title1', 'abstract1'),
        ('title2', 'abstract2'),
        ('title3', 'abstract3'),
    ]
    for t, a in articles:
        conn.execute('INSERT INTO articles (title, abstract) VALUES (?, ?)', (t, a))
    conn.commit()

    # collect ids
    cur = conn.cursor()
    cur.execute('SELECT id FROM articles ORDER BY id')
    ids = [r[0] for r in cur.fetchall()]

    written = emb.generate_and_insert_embeddings_for_ids(conn, ids)
    assert written == len(ids)

    # ensure each id has an embedding and blobs are distinct
    cur.execute('SELECT rowid, embedding FROM articles_vec ORDER BY rowid')
    rows = cur.fetchall()
    assert len(rows) == len(ids)
    blobs = [r[1] for r in rows]
    assert len(set(blobs)) == len(blobs)

    conn.close()


def test_generate_and_insert_embeddings_for_ids_preserves_other_embeddings(monkeypatch, in_memory_db):
    """Ensure that running the per-id generator does not delete other embeddings."""
    import sqlite_vec as sv
    from ednews import embeddings as emb

    # patch sqlite_vec.load to no-op
    monkeypatch.setattr(sv, 'load', lambda conn=None: None)

    # serializer maps first float to a byte
    def fake_serialize(arr):
        try:
            first = float(arr[0])
        except Exception:
            first = 0.0
        b = int(first * 10) % 256
        return bytes([b])

    monkeypatch.setattr(sv, 'serialize_float32', fake_serialize)

    # fake embed will return a new embedding for the requested text(s)
    def fake_text(texts, task_type, model, inference_mode):
        # for each input text, return embedding with first element 7.7
        return {'embeddings': [[7.7] + [0.0] * 767 for _ in texts]}

    monkeypatch.setattr('ednews.embeddings.embed', types.SimpleNamespace(text=fake_text))

    conn = in_memory_db
    # create tables
    conn.execute('CREATE TABLE articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('CREATE TABLE articles_vec (rowid INTEGER PRIMARY KEY, embedding BLOB)')

    # insert three articles
    for i in range(3):
        conn.execute('INSERT INTO articles (title, abstract) VALUES (?, ?)', (f't{i}', f'a{i}'))
    conn.commit()

    cur = conn.cursor()
    cur.execute('SELECT id FROM articles ORDER BY id')
    ids = [r[0] for r in cur.fetchall()]

    # pre-populate articles_vec with distinct blobs for each id
    initial_blobs = {ids[0]: b'old1', ids[1]: b'old2', ids[2]: b'old3'}
    for k, v in initial_blobs.items():
        conn.execute('INSERT INTO articles_vec (rowid, embedding) VALUES (?, ?)', (k, v))
    conn.commit()

    # run generator only for the first id
    written = emb.generate_and_insert_embeddings_for_ids(conn, [ids[0]])
    assert written == 1

    # confirm other embeddings remain unchanged
    cur.execute('SELECT rowid, embedding FROM articles_vec ORDER BY rowid')
    rows = {r[0]: r[1] for r in cur.fetchall()}
    assert rows[ids[1]] == initial_blobs[ids[1]]
    assert rows[ids[2]] == initial_blobs[ids[2]]

    # first id should have been updated to a new blob (not equal to old1)
    assert rows[ids[0]] != initial_blobs[ids[0]]

    conn.close()
