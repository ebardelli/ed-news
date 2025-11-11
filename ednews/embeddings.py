"""Embedding utilities for ed-news.

This module provides a small, test-friendly wrapper around sqlite-vec and
the local nomic embed API. It supports creating named virtual tables (for
articles and headlines), generating embeddings for arbitrary (id, text)
tuples and querying nearest neighbours using cosine distance.
"""

import sqlite3
import numpy as np
import sqlite_vec
from typing import Optional, Iterable, List, Tuple
from . import config
from nomic import embed

DATABASE_NAME = str(config.DB_PATH)
MODEL_NAME = config.DEFAULT_MODEL


def _ensure_vec_table(conn: sqlite3.Connection, table_name: str, dim: int = 768):
    """Create a vec0 virtual table with the given name if possible.

    The function silently logs and returns if the vec0 extension is not
    available (useful for CI/test environments).
    """
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cur = conn.cursor()
    try:
        cur.execute(
            f"CREATE VIRTUAL TABLE IF NOT EXISTS {table_name} USING vec0(embedding float[{dim}])"
        )
        conn.commit()
    except sqlite3.OperationalError as e:
        import logging

        logging.getLogger("ednews.embeddings").warning(
            "vec0 virtual table not available: %s", e
        )
        return


def _batches(lst: Iterable, n: int):
    lst = list(lst)
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _generate_embeddings(
    texts: List[str], model: Optional[str] = None
) -> List[List[float]]:
    model = model or MODEL_NAME
    output = embed.text(
        texts=texts, task_type="search_document", model=model, inference_mode="local"
    )
    return output.get("embeddings", [])


def _serialize(vec: List[float]):
    # sqlite_vec.serialize_float32 expects a sequence of floats (List[float]).
    # Ensure we pass a plain Python list of float (not a numpy ndarray) so Pyright
    # and downstream callers are satisfied about the type.
    if isinstance(vec, np.ndarray):
        lst = vec.astype(np.float32).tolist()
    else:
        # coerce items to float to be safe when input contains other numeric types
        lst = [float(x) for x in vec]
    return sqlite_vec.serialize_float32(lst)


def upsert_embeddings(
    conn: sqlite3.Connection,
    table_name: str,
    items: List[Tuple[int, str]],
    model: Optional[str] = None,
    batch_size: int = 64,
) -> int:
    """Generate embeddings for (id, text) items and upsert into named vec table.

    Returns the number of embeddings written.
    """
    if not items:
        return 0
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cur = conn.cursor()
    total = 0
    for batch in _batches(items, batch_size):
        ids = [it[0] for it in batch]
        texts = [it[1] for it in batch]
        try:
            embeddings = _generate_embeddings(texts, model=model)
        except Exception as e:
            import logging

            logging.getLogger("ednews.embeddings").error(
                "Error generating embeddings for batch starting with id %s: %s",
                ids[0] if ids else None,
                e,
            )
            continue

        for _id, emb in zip(ids, embeddings):
            blob = _serialize(emb)
            try:
                cur.execute(
                    f"UPDATE {table_name} SET embedding = ? WHERE rowid = ?",
                    (blob, _id),
                )
                if cur.rowcount == 0:
                    cur.execute(
                        f"INSERT INTO {table_name} (rowid, embedding) VALUES (?, ?)",
                        (_id, blob),
                    )
            except sqlite3.OperationalError:
                # Some sqlite builds don't allow UPDATE on virtual tables; fallback to delete/insert
                try:
                    cur.execute(f"DELETE FROM {table_name} WHERE rowid = ?", (_id,))
                    cur.execute(
                        f"INSERT INTO {table_name} (rowid, embedding) VALUES (?, ?)",
                        (_id, blob),
                    )
                except Exception:
                    raise
            total += 1
        conn.commit()
    return total


def create_articles_vec(conn: sqlite3.Connection, dim: int = 768):
    _ensure_vec_table(conn, "articles_vec", dim=dim)


def create_headlines_vec(conn: sqlite3.Connection, dim: int = 768):
    _ensure_vec_table(conn, "headlines_vec", dim=dim)


def generate_and_insert_article_embeddings(
    conn: sqlite3.Connection,
    model: Optional[str] = None,
    batch_size: int = 64,
    force: bool = False,
):
    """Generate and insert embeddings for articles.

    If force is False (default), only articles missing embeddings are processed.
    If force is True, all eligible articles are re-embedded.
    """
    model = model or MODEL_NAME
    cur = conn.cursor()
    cur.execute("SELECT id, title, abstract FROM articles")
    rows = cur.fetchall()
    if not rows:
        return 0
    items = []
    for _id, title, abstract in rows:
        title = title or ""
        abstract = abstract or ""
        combined = title.strip()
        if abstract.strip():
            combined = (
                combined + "\n\n" + abstract.strip() if combined else abstract.strip()
            )
        if not combined:
            continue
        items.append((_id, combined))

    # determine existing embeddings
    try:
        cur.execute("SELECT rowid FROM articles_vec")
        existing = {r[0] for r in cur.fetchall()}
    except Exception:
        existing = set()

    if force:
        to_process = items
    else:
        to_process = [it for it in items if it[0] not in existing]

    if not to_process:
        return 0
    return upsert_embeddings(
        conn, "articles_vec", to_process, model=model, batch_size=batch_size
    )


def generate_and_insert_embeddings_local(
    conn: sqlite3.Connection, model: Optional[str] = None, batch_size: int = 64
):
    """Compatibility wrapper for the older API name used by main.py.

    Historically the CLI used generate_and_insert_embeddings_local. Keep a
    wrapper that delegates to the current article embedding generator.
    """
    # Preserve compatibility while allowing callers to opt-in to force.
    return generate_and_insert_article_embeddings(
        conn, model=model, batch_size=batch_size, force=False
    )


def create_database(conn: sqlite3.Connection):
    """Compatibility wrapper: create the `articles_vec` virtual table if possible."""
    return create_articles_vec(conn)


def generate_and_insert_embeddings_for_ids(
    conn: sqlite3.Connection, ids: List[int], model: Optional[str] = None
) -> int:
    """Compatibility wrapper: generate embeddings for the provided article ids.

    Returns the number of embeddings written.
    """
    if not ids:
        return 0
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in ids)
    cur.execute(
        f"SELECT id, title, abstract FROM articles WHERE id IN ({placeholders})",
        tuple(ids),
    )
    rows = cur.fetchall()
    items = []
    for _id, title, abstract in rows:
        title = title or ""
        abstract = abstract or ""
        combined = title.strip()
        if abstract.strip():
            combined = (
                combined + "\n\n" + abstract.strip() if combined else abstract.strip()
            )
        if not combined:
            continue
        items.append((_id, combined))
    if not items:
        return 0
    return upsert_embeddings(conn, "articles_vec", items, model=model, batch_size=64)


def generate_and_insert_headline_embeddings(
    conn: sqlite3.Connection, model: Optional[str] = None, batch_size: int = 64
) -> int:
    """Generate and insert embeddings for news headlines by concatenating title and text.

    Embeddings are stored in `headlines_vec` with rowid equal to `headlines.id`.
    """
    model = model or MODEL_NAME
    cur = conn.cursor()
    cur.execute("SELECT id, title, text FROM headlines")
    rows = cur.fetchall()
    if not rows:
        return 0
    items = []
    for _id, title, text in rows:
        title = (title or "").strip()
        text = (text or "").strip()
        combined = title
        if text:
            combined = combined + "\n\n" + text if combined else text
        if not combined.strip():
            continue
        items.append((_id, combined))

    try:
        cur.execute("SELECT rowid FROM headlines_vec")
        existing = {r[0] for r in cur.fetchall()}
    except Exception:
        existing = set()
    to_process = [it for it in items if it[0] not in existing]
    if not to_process:
        return 0
    return upsert_embeddings(
        conn, "headlines_vec", to_process, model=model, batch_size=batch_size
    )


def find_similar_headlines_by_rowid(
    conn: sqlite3.Connection, rowid: int, top_n: int = 5
):
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)
    cur = conn.cursor()
    cur.execute("SELECT embedding FROM headlines_vec WHERE rowid = ?", (rowid,))
    res = cur.fetchone()
    if not res or not res[0]:
        return []
    target_blob = res[0]
    q = """
    SELECT N.id, N.title, N.text, N.link, vec_distance_cosine(V.embedding, ?) AS distance
    FROM headlines AS N, headlines_vec AS V
    WHERE N.id = V.rowid AND N.id != ?
    ORDER BY distance ASC
    LIMIT ?
    """
    results = cur.execute(q, (target_blob, rowid, top_n)).fetchall()
    out = []
    for nid, title, text, link, distance in results:
        out.append(
            {
                "id": nid,
                "title": title,
                "text": text,
                "link": link,
                "distance": float(distance) if distance is not None else None,
            }
        )
    return out
