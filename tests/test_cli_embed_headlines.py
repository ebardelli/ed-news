import sqlite3
import types
from ednews import main
from ednews import embeddings


def test_cmd_embed_with_headlines_calls_headline_generator(monkeypatch, tmp_path):
    # Setup a temporary DB file
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute('CREATE TABLE IF NOT EXISTS articles (id INTEGER PRIMARY KEY, title TEXT, abstract TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS headlines (id INTEGER PRIMARY KEY, title TEXT, text TEXT)')
    conn.commit()
    conn.close()

    # Monkeypatch config.DB_PATH to point to our temp DB
    monkeypatch.setattr('ednews.config.DB_PATH', db_path)

    called = {'headlines': False}

    def fake_generate_headlines(conn, model=None, batch_size=64):
        called['headlines'] = True
        return 0

    monkeypatch.setattr(embeddings, 'generate_and_insert_headline_embeddings', fake_generate_headlines)

    # Simulate running main with --headlines
    class Args:
        model = None
        batch_size = 16
        headlines = True

    main.cmd_embed(Args())
    assert called['headlines'] is True
