import sqlite3
import sqlite_vec
import types
from ednews import embeddings as emb


def test_generate_and_insert_headline_embeddings_writes_all(monkeypatch):
    """Ensure headline embedding generator writes embeddings for all news items and is idempotent."""
    # noop load
    monkeypatch.setattr(sqlite_vec, 'load', lambda conn=None: None)

    # fake serialize maps first float to a byte
    def fake_serialize(arr):
        try:
            first = float(arr[0])
        except Exception:
            first = 0.0
        b = int(first * 10) % 256
        return bytes([b])

    monkeypatch.setattr(sqlite_vec, 'serialize_float32', fake_serialize)

    # fake embed returns different first element per text so serialized blobs differ
    def fake_text(texts, task_type, model, inference_mode):
        out = {'embeddings': []}
        for i, _ in enumerate(texts):
            first = 0.1 * (i + 1)
            emb_vec = [first] + [0.0] * 767
            out['embeddings'].append(emb_vec)
        return out

    monkeypatch.setattr('ednews.embeddings.embed', types.SimpleNamespace(text=fake_text))

    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()
    cur.execute('CREATE TABLE headlines (id INTEGER PRIMARY KEY AUTOINCREMENT, source TEXT, title TEXT, text TEXT, link TEXT, first_seen TEXT, published TEXT)')
    # create an articles-like vec table for headlines
    cur.execute('CREATE TABLE headlines_vec (rowid INTEGER PRIMARY KEY, embedding BLOB)')

    # Insert 50 rows (use 50 for speed; adjust to 200 if you want a larger test)
    count = 50
    for i in range(count):
        cur.execute('INSERT INTO headlines (source, title, text, link, first_seen, published) VALUES (?, ?, ?, ?, ?, ?)', (
            'src', f'Title {i}', f'Text {i}', f'https://example.com/{i}', None, None
        ))
    conn.commit()

    written = emb.generate_and_insert_headline_embeddings(conn, model=None, batch_size=20)
    assert written == count

    # Running again should write 0 new embeddings (idempotent)
    written2 = emb.generate_and_insert_headline_embeddings(conn, model=None, batch_size=20)
    assert written2 == 0

    # confirm number of rows in headlines_vec
    cur.execute('SELECT COUNT(*) FROM headlines_vec')
    rr = cur.fetchone()
    assert rr[0] == count
    conn.close()
