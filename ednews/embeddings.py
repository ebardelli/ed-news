import sqlite3
import numpy as np
import sqlite_vec
from typing import Optional
from . import config
from nomic import embed

DATABASE_NAME = str(config.DB_PATH)
MODEL_NAME = config.DEFAULT_MODEL


def create_database(conn: sqlite3.Connection):
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    
    cursor = conn.cursor()
    try:
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS articles_vec USING vec0(embedding float[768])
        ''')
    except sqlite3.OperationalError as e:
        # vec0 extension not available in this environment (tests/CI). Log and continue.
        import logging

        logging.getLogger("ednews.embeddings").warning("vec0 virtual table not available: %s", e)
        return
    conn.commit()


def generate_and_insert_embeddings_local(conn: sqlite3.Connection, model: Optional[str] = None, batch_size: int = 64):
    model = model or MODEL_NAME
    cursor = conn.cursor()
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cursor.execute("SELECT id, title, abstract FROM articles")
    rows = cursor.fetchall()
    if not rows:
        print("No articles found in the database. Nothing to embed.")
        return

    items = []
    for row in rows:
        _id, title, abstract = row
        title = title or ""
        abstract = abstract or ""
        combined = title.strip()
        if abstract.strip():
            combined = combined + "\n\n" + abstract.strip() if combined else abstract.strip()
        if not combined:
            continue
        items.append((_id, combined))

    cursor.execute("SELECT rowid FROM articles_vec")
    existing = {r[0] for r in cursor.fetchall()}
    to_process = [it for it in items if it[0] not in existing]
    if not to_process:
        print("All articles already have embeddings. Nothing to do.")
        return

    try:
        cursor.execute("DELETE FROM articles_vec")
    except Exception:
        pass

    def batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    total = 0
    for batch in batches(to_process, batch_size):
        ids = [it[0] for it in batch]
        texts = [it[1] for it in batch]
        try:
            output = embed.text(
                texts=texts,
                task_type='search_document',
                model=model,
                inference_mode='local'
            )
            embeddings = output.get('embeddings', [])
        except Exception as e:
            print(f"Error generating embeddings for batch starting with id {ids[0]}: {e}")
            continue

        for _id, embedding in zip(ids, embeddings):
            embedding_np = np.array(embedding, dtype=np.float32)
            embedding_blob = sqlite_vec.serialize_float32(embedding_np)
            cursor.execute("INSERT OR REPLACE INTO articles_vec (rowid, embedding) VALUES (?, ?)", (_id, embedding_blob))
            total += 1
        conn.commit()

    print(f"Successfully saved {total} embeddings to {DATABASE_NAME}.")


def find_similar_articles_local(conn: sqlite3.Connection, query_text: str, model: Optional[str] = None, limit: int = 3):
    model = model or MODEL_NAME
    try:
        query_output = embed.text(
            texts=[query_text],
            task_type='search_query',
            model=model,
            inference_mode='local'
        )
        query_embedding_np = np.array(query_output['embeddings'], dtype=np.float32)
        query_vector_blob = sqlite_vec.serialize_float32(query_embedding_np[0])
    except Exception as e:
        print(f"Error generating query embedding locally: {e}")
        return []

    cursor = conn.cursor()
    results = conn.execute(
        '''
        SELECT A.title, A.abstract, vec_distance_cosine(V.embedding, ?) AS distance
        FROM articles AS A, articles_vec AS V
        WHERE A.id = V.rowid
        ORDER BY distance ASC
        LIMIT ?
        ''',
        (query_vector_blob, limit)
    ).fetchall()

    out = []
    for title, abstract, distance in results:
        out.append((title, abstract, distance))
    return out
