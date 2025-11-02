#!/usr/bin/env python3
"""Package-level CLI entrypoint for ed-news.

This module wires subcommands implemented in separate modules under
`ednews.cli` into a single `run()` function so the top-level
`main.py` can remain a thin wrapper.
"""
import argparse
import logging
from datetime import date
from typing import Any

from .fetch import cmd_fetch
from .build import cmd_build
from .embed import cmd_embed
from .db_init import cmd_db_init
from .issn_lookup import cmd_issn_lookup
from .headlines import cmd_headlines
from .manage_db import (
    cmd_manage_db_cleanup,
    cmd_manage_db_cleanup_filtered_title,
    cmd_manage_db_vacuum,
    cmd_manage_db_migrate,
    cmd_manage_db_rematch,
    cmd_manage_db_sync_publications,
    cmd_manage_db_run_all,
)
from .serve import cmd_serve
from .postprocess import cmd_postprocess
from .common import normalize_cli_date

logger = logging.getLogger("ednews.cli")


def run() -> None:
    """Entrypoint for the ednews CLI.

    Parses command-line arguments and dispatches to the appropriate
    `cmd_*` handler functions implemented in submodules.
    """
    parser = argparse.ArgumentParser(prog="ednews")
    # Default --from-date to the first day of the month six months prior to today.
    try:
        _today = date.today()
        _month = _today.month - 6
        _year = _today.year
        if _month <= 0:
            _month += 12
            _year -= 1
        _default_from_date = f"{_year:04d}-{_month:02d}-01"
    except Exception:
        _default_from_date = None

    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd")

    p_fetch = sub.add_parser("fetch", help="Fetch feeds and save items")
    p_fetch.add_argument("--articles", action="store_true", help="Also fetch article feeds (default: both articles and headlines if no flags are set)")
    p_fetch.add_argument("--headlines", action="store_true", help="Also fetch news headlines (default: both articles and headlines if no flags are set)")
    p_fetch.set_defaults(func=cmd_fetch)

    p_build = sub.add_parser("build", help="Render static site into build/")
    p_build.add_argument("--out-dir", help="Output directory")
    p_build.set_defaults(func=cmd_build)

    p_embed = sub.add_parser("embed", help="Generate local embeddings and store in DB")
    p_embed.add_argument("--model", help="Embedding model", default=None)
    p_embed.add_argument("--batch-size", type=int, default=64)
    p_embed.add_argument("--headlines", action="store_true", help="Also generate embeddings for news headlines")
    p_embed.add_argument("--articles", action="store_true", help="Generate embeddings for articles (default: both articles and headlines if no flags are set)")
    p_embed.set_defaults(func=cmd_embed)

    p_issn = sub.add_parser("issn-lookup", help="Fetch latest works for journals by ISSN and insert into DB")
    p_issn.add_argument("--per-journal", type=int, default=30, help="Number of works to fetch per journal (uses cursor pagination; no hard max)")
    p_issn.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    p_issn.add_argument("--delay", type=float, default=0.05, help="Delay between individual requests (seconds)")
    p_issn.add_argument("--sort-by", type=str, default="created", help="Field to sort by when requesting works (e.g. created, deposited)")
    p_issn.add_argument("--date-filter-type", type=str, choices=["created", "updated", "indexed"], default=None, help="Use Crossref date filters (from-*/until-*)")
    p_issn.add_argument(
        "--from-date",
        type=str,
        default=_default_from_date,
        help=(
            "Start date/time for date filter. Accepts: YYYY, YYYY-MM, YYYY-MM-DD, "
            "or datetimes like YYYY-MM-DDTHH:MM (interpreted as UTC if no timezone). "
            "Defaults to first day of month six months ago."
        ),
    )
    p_issn.add_argument(
        "--until-date",
        type=str,
        default=None,
        help=(
            "End date/time for date filter. Accepts the same formats as --from-date; "
            "datetimes without timezone are treated as UTC."
        ),
    )
    p_issn.set_defaults(func=cmd_issn_lookup)

    p_headlines = sub.add_parser("headlines", help="Fetch latest headlines from news.json sites")
    p_headlines.add_argument("--out", help="Write output JSON to this file")
    p_headlines.add_argument("--no-persist", action="store_true", help="Do not persist fetched headlines to the configured DB (default: persist)")
    p_headlines.set_defaults(func=cmd_headlines)

    p_dbinit = sub.add_parser("db-init", help="Create DB schema and views (run once)")
    p_dbinit.set_defaults(func=cmd_db_init)

    p_manage = sub.add_parser("manage-db", help="Database maintenance commands")
    manage_sub = p_manage.add_subparsers(dest="manage_cmd")
    p_manage.set_defaults(func=lambda args: p_manage.print_help())

    p_cleanup = manage_sub.add_parser("cleanup-empty-articles", help="Remove articles with no title and no abstract")
    p_cleanup.add_argument("--older-than-days", type=int, default=None, help="Only delete articles older than this many days (based on fetched_at or published)")
    p_cleanup.add_argument("--dry-run", action="store_true", help="Do not delete; only report how many rows would be deleted")
    p_cleanup.set_defaults(func=lambda args: cmd_manage_db_cleanup(args))

    p_cleanup_ft = manage_sub.add_parser("cleanup-filtered-title", help="Remove articles whose titles match configured filters")
    p_cleanup_ft.add_argument("--filter", action="append", help="A single title to filter (repeatable)")
    p_cleanup_ft.add_argument("--filters", type=str, default=None, help="Comma-separated list of titles to filter")
    p_cleanup_ft.add_argument("--dry-run", action="store_true", help="Do not delete; only report how many rows would be deleted")
    p_cleanup_ft.set_defaults(func=lambda args: cmd_manage_db_cleanup_filtered_title(args))

    p_vacuum = manage_sub.add_parser("vacuum", help="Run VACUUM on the configured DB")
    p_vacuum.set_defaults(func=lambda args: cmd_manage_db_vacuum(args))

    p_migrate = manage_sub.add_parser("migrate", help="Run schema migrations (no-op placeholder)")
    p_migrate.set_defaults(func=lambda args: cmd_manage_db_migrate(args))

    p_sync = manage_sub.add_parser("sync-publications", help="Sync publications table from feeds list")
    p_sync.set_defaults(func=lambda args: cmd_manage_db_sync_publications(args))

    p_rematch = manage_sub.add_parser("rematch-dois", help="Clear DOIs for a publication or feeds and re-run Crossref matching")
    p_rematch.add_argument("--publication-id", help="Publication ID to target (will resolve feeds from publications table)")
    p_rematch.add_argument("--feed", action="append", help="Feed key to target (repeatable). If omitted, publication-id will be used to resolve feeds")
    p_rematch.add_argument("--dry-run", action="store_true", help="Do not modify DB; only report what would be done")
    p_rematch.add_argument("--remove-orphan-articles", action="store_true", help="Remove articles for the publication that are no longer referenced by any items")
    p_rematch.add_argument("--only-wrong", action="store_true", help="Only operate on items whose DOI is missing or whose DOI does not match the configured publication_id")
    p_rematch.set_defaults(func=lambda args: cmd_manage_db_rematch(args))

    p_runall = manage_sub.add_parser("run-all", help="Run migrations, sync publications, cleanup, and vacuum in sequence")
    p_runall.add_argument("--older-than-days", type=int, default=None, help="Pass-through to cleanup step")
    p_runall.add_argument("--dry-run", action="store_true", help="Do not perform destructive actions; only report")
    p_runall.set_defaults(func=lambda args: cmd_manage_db_run_all(args))

    p_serve = sub.add_parser("serve", help="Serve the built static site from the build directory")
    p_serve.add_argument("--host", help="Host to bind to (default: 127.0.0.1)")
    p_serve.add_argument("--port", type=int, help="Port to listen on (default: 8000)")
    p_serve.add_argument("--directory", help="Directory to serve (default: build)")
    p_serve.set_defaults(func=cmd_serve)

    p_post = sub.add_parser("postprocess", help="Run a DB-level postprocessor (e.g. crossref) for feeds")
    p_post.add_argument("--processor", required=True, help="Name of the processor to run (e.g. crossref)")
    p_post.add_argument("--feed", action="append", help="Feed key to limit processing to (repeatable). If omitted, feeds are auto-detected from feeds list")
    p_post.add_argument("--only-missing", action="store_true", help="Only include items where the specified field is missing/empty (default: doi)")
    p_post.add_argument("--missing-field", type=str, default="doi", help="Field to check for missingness when --only-missing is set (default: doi)")
    p_post.add_argument("--force", action="store_true", help="Force re-fetch even when existing metadata appears present")
    p_post.add_argument("--check-fields", type=str, default=None, help="Comma-separated list of article fields to require before skipping (e.g. raw,authors,abstract). If omitted, processors choose their defaults.")
    p_post.set_defaults(func=cmd_postprocess)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


__all__ = ["run", "normalize_cli_date"]
