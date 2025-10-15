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

    # prepare items (id, combined_text)
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

    # determine which items need embeddings
    cursor.execute("SELECT rowid FROM articles_vec")
    existing = {r[0] for r in cursor.fetchall()}
    to_process = [it for it in items if it[0] not in existing]
    if not to_process:
        print("All articles already have embeddings. Nothing to do.")
        return

    total = _generate_and_upsert_embeddings(conn, to_process, model=model, batch_size=batch_size)
    print(f"Successfully saved {total} embeddings to {DATABASE_NAME}.")


def generate_and_insert_embeddings_for_ids(conn: sqlite3.Connection, ids: list[int], model: Optional[str] = None) -> int:
    """Generate and insert embeddings for a list of article ids.

    Returns the number of embeddings written.

    This is a targeted helper to avoid regenerating the whole database when only
    a few articles changed.
    """
    if not ids:
        return 0
    model = model or MODEL_NAME
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cursor = conn.cursor()

    # fetch the articles for the given ids
    placeholders = ",".join("?" for _ in ids)
    cursor.execute(f"SELECT id, title, abstract FROM articles WHERE id IN ({placeholders})", tuple(ids))
    rows = cursor.fetchall()
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

    if not items:
        return 0

    # prepare items
    items = [(_id, (title or "") + ("\n\n" + (abstract or "") if abstract else "")) for (_id, title, abstract) in rows]
    # filter out empty combined text
    items = [it for it in items if it[1].strip()]
    if not items:
        return 0

    return _generate_and_upsert_embeddings(conn, items, model=model, batch_size=64)


def _generate_and_upsert_embeddings(conn: sqlite3.Connection, items: list[tuple], model: Optional[str] = None, batch_size: int = 64) -> int:
    """Generate embeddings for (id, text) items and upsert them into articles_vec.

    Returns number of embeddings written.
    """
    model = model or MODEL_NAME
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cursor = conn.cursor()

    def batches(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    total = 0
    for batch in batches(items, batch_size):
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
            import logging

            logging.getLogger("ednews.embeddings").error("Error generating embeddings for batch starting with id %s: %s", ids[0], e)
            continue

        for _id, embedding in zip(ids, embeddings):
            embedding_np = np.array(embedding, dtype=np.float32)
            embedding_blob = sqlite_vec.serialize_float32(embedding_np)
            try:
                cursor.execute("UPDATE articles_vec SET embedding = ? WHERE rowid = ?", (embedding_blob, _id))
                if cursor.rowcount == 0:
                    cursor.execute("INSERT INTO articles_vec (rowid, embedding) VALUES (?, ?)", (_id, embedding_blob))
            except sqlite3.OperationalError:
                try:
                    cursor.execute("DELETE FROM articles_vec WHERE rowid = ?", (_id,))
                    cursor.execute("INSERT INTO articles_vec (rowid, embedding) VALUES (?, ?)", (_id, embedding_blob))
                except Exception:
                    raise
            total += 1
        conn.commit()

    return total


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
