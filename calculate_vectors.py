import sqlite3
import numpy as np
from nomic import embed
import sqlite_vec

DATABASE_NAME = "ednews.db"
MODEl_NOTE = "Embeddings are generated using the local Nomic model; adjust MODEL_NAME if you need a different one."
MODEL_NAME = 'nomic-embed-text-v1.5'

def create_database(conn):
    """
    Ensures the vector virtual table exists for article embeddings.
    The repository is expected to already have an `articles` table; we only create
    a virtual `articles_vec` table used for vector storage/search.
    """
    print("Creating tables in the database...")
    cursor = conn.cursor()
    
    # Enable the sqlite-vec extension
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)

    # Virtual table for vector storage and search for articles
    # We don't create or modify the existing `articles` table here.
    cursor.execute('''
        CREATE VIRTUAL TABLE IF NOT EXISTS articles_vec USING vec0(embedding float[768])
    ''')
    
    conn.commit()

def generate_and_insert_embeddings_local(conn, model, batch_size: int = 64):
    """
    Fetches title+abstract from the `articles` table, generates embeddings in batches
    using the local Nomic model, and stores them in `articles_vec`.

    Accepts a batch_size to control memory/throughput tradeoff.
    """
    cursor = conn.cursor()

    # Fetch article rows (id, title, abstract)
    cursor.execute("SELECT id, title, abstract FROM articles")
    rows = cursor.fetchall()
    if not rows:
        print("No articles found in the database. Nothing to embed.")
        return

    print(f"Preparing to generate embeddings for {len(rows)} articles using local model '{model}'...")

    # Prepare texts (concatenate title and abstract, handle NULLs)
    items = []  # list of (id, text)
    for row in rows:
        _id, title, abstract = row
        title = title or ""
        abstract = abstract or ""
        combined = title.strip()
        if abstract.strip():
            combined = combined + "\n\n" + abstract.strip() if combined else abstract.strip()
        if not combined:
            # Skip empty content
            continue
        items.append((_id, combined))

    # Filter out items that already have embeddings in articles_vec to avoid recalculation
    to_process = []
    cursor.execute("SELECT rowid FROM articles_vec")
    existing = {r[0] for r in cursor.fetchall()}
    for it in items:
        if it[0] not in existing:
            to_process.append(it)

    if not to_process:
        print("All articles already have embeddings. Nothing to do.")
        return

    if not items:
        print("No non-empty title+abstract content to embed.")
        return

    # Clear existing vectors for a fresh run
    try:
        cursor.execute("DELETE FROM articles_vec")
    except Exception:
        # If the table is empty or deletion fails, ignore and continue
        pass

    # Helper to yield batches
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

        print(f"Inserting batch of {len(embeddings)} embeddings into the database...")
        for _id, embedding in zip(ids, embeddings):
            embedding_np = np.array(embedding, dtype=np.float32)
            embedding_blob = sqlite_vec.serialize_float32(embedding_np)
            # Store using the article id as the rowid in the virtual table
            cursor.execute("INSERT OR REPLACE INTO articles_vec (rowid, embedding) VALUES (?, ?)", (_id, embedding_blob))
            total += 1

        conn.commit()

    print(f"Successfully saved {total} embeddings to {DATABASE_NAME}.")

def find_similar_articles_local(conn, query_text, model, limit=3):
    """
    Performs a similarity search over `articles` using the local model and `articles_vec`.
    (Unused by default; kept for convenience.)
    """
    print(f"\nSearching for articles similar to: '{query_text}'")

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
        return

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

    print("Search results:")
    for title, abstract, distance in results:
        snippet = (title or "")
        print(f"  - Title: '{snippet}' (Distance: {distance:.4f})")


def main():
    """
    Main function to run the complete workflow with local embedding.
    """
    # Connect to the database
    conn = sqlite3.connect(DATABASE_NAME)

    # Create/ensure the vector virtual table exists
    create_database(conn)

    # Generate embeddings for articles and insert them
    generate_and_insert_embeddings_local(conn, MODEL_NAME, batch_size=64)

    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()

