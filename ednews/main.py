"""Package-level CLI entrypoint for ed-news.

This module provides a small compatibility wrapper so `from ednews import main`
works for tests and callers that expect `ednews.main.main()` to exist.
"""

def main():
    """Run the CLI by delegating to `ednews.cli.run`.

    This keeps the package import lightweight while providing the expected
    `main()` symbol used by tests.
    """
    from .cli import run

    return run()


# Expose commonly-patched attributes at module level so tests can monkeypatch
# them via `from ednews import main as ed_main` and replace `sqlite3` or
# `embeddings` without importing deeper modules.
import sqlite3 as sqlite3  # re-export
from . import embeddings as embeddings  # re-export embeddings module
from . import cli as cli
from . import feeds as feeds
import requests as requests
from concurrent.futures import ThreadPoolExecutor as ThreadPoolExecutor, as_completed as as_completed

# Re-export common CLI handlers so tests can call them directly via `ednews.main.cmd_embed` etc.
cmd_embed = cli.cmd_embed
cmd_fetch = cli.cmd_fetch
cmd_build = cli.cmd_build
cmd_enrich_crossref = cli.cmd_enrich_crossref
cmd_issn_lookup = cli.cmd_issn_lookup
cmd_headlines = cli.cmd_headlines
cmd_manage_db_cleanup = cli.cmd_manage_db_cleanup
cmd_manage_db_vacuum = cli.cmd_manage_db_vacuum
cmd_manage_db_migrate = cli.cmd_manage_db_migrate
cmd_manage_db_sync_publications = cli.cmd_manage_db_sync_publications
cmd_manage_db_run_all = cli.cmd_manage_db_run_all
cmd_serve = cli.cmd_serve
# Module-level placeholder for a DB connection (tests monkeypatch this)
conn = None
