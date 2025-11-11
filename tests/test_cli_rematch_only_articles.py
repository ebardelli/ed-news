import sqlite3
import sys

import pytest

from ednews.cli import run as cli_run


def test_cli_rematch_only_articles_invokes_postprocessor(monkeypatch, capsys, tmp_path):
    """Ensure `ednews manage-db rematch-dois --only-articles` runs the postprocessor
    against `articles` rows that are not present in `items` and that the
    postprocessor can update article DOIs.
    """
    # Create a file-backed sqlite DB so it survives CLI connection close
    db_file = tmp_path / "test_rematch_only_articles.db"
    conn = sqlite3.connect(str(db_file))
    # Initialize canonical schema (avoids test-specific columns like `link` on articles)
    from ednews.db import init_db

    init_db(conn)
    cur = conn.cursor()

    # Insert publication row so the rematch discovers the feed
    cur.execute(
        "INSERT INTO publications (feed_id, publication_id, feed_title, issn) VALUES (?, ?, ?, ?)",
        ("aerj", "aerj", "AERJ Feed", ""),
    )
    # Insert an article row with no DOI and no corresponding item
    cur.execute(
        "INSERT INTO articles (title, published, fetched_at, feed_id, publication_id) VALUES (?, ?, ?, ?, ?)",
        ("Test Article", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z", "aerj", ""),
    )
    conn.commit()

    # Monkeypatch get_conn in the manage_db module to open connections to the same DB file
    import ednews.cli.manage_db as manage_db_cli

    monkeypatch.setattr(manage_db_cli, "get_conn", lambda: sqlite3.connect(str(db_file)))

    # Instead of calling a feed postprocessor, rematch now looks up DOIs
    # via ednews.crossref.query_crossref_doi_by_title and updates DB rows
    # directly. Monkeypatch that function to return a known DOI for the
    # article title and assert the `articles` row was updated.

    def fake_query(title, preferred_publication_id=None):
        # Return a fixed DOI for the test article title
        if title and "Test Article" in title:
            return "10.1000/test"
        return None

    from ednews import crossref as cr_mod
    monkeypatch.setattr(cr_mod, "query_crossref_doi_by_title", fake_query, raising=False)

    # Run CLI: ednews manage-db rematch-dois --feed aerj --only-articles
    monkeypatch.setattr(sys, "argv", ["ednews", "manage-db", "rematch-dois", "--feed", "aerj", "--only-articles", "--only-missing"])

    # Execute the CLI
    cli_run()

    # Verify that the article row was updated with the DOI
    cur.execute("SELECT doi FROM articles WHERE feed_id = ?", ("aerj",))
    row = cur.fetchone()
    assert row and row[0] == "10.1000/test"

    # Verify the article DOI was updated by opening a fresh connection
    conn2 = sqlite3.connect(str(db_file))
    cur2 = conn2.cursor()
    cur2.execute("SELECT doi FROM articles WHERE feed_id = ?", ("aerj",))
    row = cur2.fetchone()
    assert row and row[0] == "10.1000/test"
    conn2.close()
