import sys

import pytest

from ednews import main as ed_main


class DummyConn:
    def close(self):
        pass


def _setup_monkeypatches(monkeypatch, called):
    # Prevent real DB connections
    monkeypatch.setattr(ed_main, "sqlite3", ed_main.sqlite3)

    def fake_connect(path):
        called.append(f"connect:{path}")
        return DummyConn()

    monkeypatch.setattr(ed_main.sqlite3, "connect", fake_connect)

    # Replace embedding functions with no-ops that record calls
    monkeypatch.setattr(ed_main.embeddings, "create_database", lambda conn: called.append("create_database"))
    monkeypatch.setattr(ed_main.embeddings, "generate_and_insert_embeddings_local", lambda conn, model=None, batch_size=None: called.append("articles"))
    monkeypatch.setattr(ed_main.embeddings, "create_headlines_vec", lambda conn: called.append("create_headlines_vec"))
    monkeypatch.setattr(ed_main.embeddings, "generate_and_insert_headline_embeddings", lambda conn, model=None, batch_size=None: called.append("headlines"))


def run_main_with_args(monkeypatch, argv, called):
    # Set argv and run main
    monkeypatch.setattr(sys, "argv", argv)
    ed_main.main()


def test_embed_no_flags_runs_both(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    run_main_with_args(monkeypatch, ["ednews", "embed"], called)
    # Expect both article and headline generators to be called
    assert "articles" in called
    assert "headlines" in called


def test_embed_articles_only(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    run_main_with_args(monkeypatch, ["ednews", "embed", "--articles"], called)
    assert "articles" in called
    assert "headlines" not in called


def test_embed_headlines_only(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    run_main_with_args(monkeypatch, ["ednews", "embed", "--headlines"], called)
    assert "headlines" in called
    assert "articles" not in called


def test_embed_both_flags(monkeypatch):
    called = []
    _setup_monkeypatches(monkeypatch, called)
    run_main_with_args(monkeypatch, ["ednews", "embed", "--articles", "--headlines"], called)
    assert "articles" in called
    assert "headlines" in called
