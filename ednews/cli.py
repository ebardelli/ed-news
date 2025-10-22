#!/usr/bin/env python3
"""Terminal UI / CLI for ed-news.

This module contains the main CLI implementation previously in top-level
`main.py`. It exposes a `run()` function so the project entrypoint can be
kept thin.
"""
import argparse
import logging
from pathlib import Path
from ednews import feeds, build as build_mod, embeddings
from ednews import config
import sqlite3
import requests
import re
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("ednews.cli")


def normalize_cli_date(s: str | None) -> str | None:
    """Normalize CLI date inputs for issn-lookup and similar commands.

    - Preserve date fragments like 'YYYY', 'YYYY-MM', 'YYYY-MM-DD'.
    - If given a datetime-like string without timezone (contains 'T' and
      no timezone suffix), parse it and treat it as UTC, returning an
      ISO-formatted string with timezone (+00:00).
    - Otherwise return the input string trimmed.
    """
    if not s:
        return None
    try:
        s2 = str(s).strip()
        # Preserve year/month/day-only fragments unchanged
        if re.match(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?$", s2):
            return s2
        # If a full datetime-like string without timezone is provided,
        # attempt to parse and append UTC timezone.
        if "T" in s2 and not re.search(r"Z|[+-]\d{2}:?\d{2}$", s2):
            try:
                dt = datetime.fromisoformat(s2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                return s2
        return s2
    except Exception:
        return s


def cmd_fetch(args):
    # Determine which sources to fetch. If neither flag is provided, fetch both.
    want_articles = getattr(args, "articles", False)
    want_headlines = getattr(args, "headlines", False)

    if not want_articles and not want_headlines:
        want_articles = True
        want_headlines = True

    feeds_list = feeds.load_feeds() if want_articles else []
    if not feeds_list:
        if want_articles:
            logger.error("No feeds found; aborting fetch")
            return
    conn = sqlite3.connect(str(config.DB_PATH))
    # Ensure DB schema exists so we can insert items/publications/headlines.
    # Historically `fetch` would create tables if missing; keep that convenient
    # behavior so users can run `fetch` without an explicit `db-init` step.
    try:
        from ednews.db import init_db

        try:
            init_db(conn)
        except Exception:
            logger.debug("failed to initialize DB schema before fetch")
    except Exception:
        logger.debug("ednews.db.init_db not importable; skipping init_db call")

    # Ensure publications table is populated/upserted from planet.json feeds
    try:
        from ednews.db.manage_db import sync_publications_from_feeds

        try:
            synced = sync_publications_from_feeds(conn, feeds_list)
            logger.info("synced %d publications from feeds", synced)
        except Exception:
            logger.debug("failed to sync publications from feeds list")
    except Exception:
        logger.debug("failed to import sync_publications_from_feeds")

    session = requests.Session()
    # Run article feed fetching if requested
    if want_articles:
        with ThreadPoolExecutor(max_workers=8) as ex:
            futures = {}
            for item in feeds_list:
                # Support tuples of shape (key, title, url, publication_doi, issn, processor)
                if len(item) >= 4:
                    key = item[0]
                    title = item[1] if len(item) > 1 else None
                    url = item[2] if len(item) > 2 else None
                    publication_doi = item[3] if len(item) > 3 else None
                    issn = item[4] if len(item) > 4 else None
                    processor = item[5] if len(item) > 5 else None
                else:
                    continue

                if processor:
                    # Processor may be a string or a list of processor names.
                    # Normalize to a list of names.
                    proc_names = []
                    if isinstance(processor, (list, tuple)):
                        proc_names = list(processor)
                    else:
                        proc_names = [processor]

                    # Submit a wrapper that invokes each named processor and merges results
                    def _proc_multi(session, key, title, url, publication_doi, issn, proc_names):
                        try:
                            import importlib
                            import ednews.processors as proc_mod

                            merged = []
                            seen = set()
                            for name in proc_names:
                                if not name:
                                    continue
                                fn_name = f"{name}_feed_processor"
                                proc_fn = getattr(proc_mod, fn_name, None)
                                if not proc_fn:
                                    # Try dynamic import as fallback
                                    try:
                                        mod = importlib.import_module(name)
                                        proc_fn = getattr(mod, fn_name, None)
                                    except Exception:
                                        proc_fn = None
                                if not proc_fn:
                                    logger.warning("processor %s not found for feed %s", name, key)
                                    continue
                                try:
                                    # Determine if this processor is an enricher (accepts entries)
                                    import inspect

                                    sig = inspect.signature(proc_fn)
                                    params = list(sig.parameters.keys())
                                    if params and params[0] in ("entries", "items", "rows"):
                                        # enricher-style: call with current entries; if we don't have entries yet, call fetch first
                                        if merged:
                                            entries = proc_fn(merged, session=session, publication_id=publication_doi, issn=issn)
                                        else:
                                            # nothing to enrich; skip
                                            entries = []
                                    else:
                                        # fetcher-style: call with session and url
                                        entries = proc_fn(session, url, publication_id=publication_doi, issn=issn)
                                except TypeError:
                                    # Fallbacks for varied signatures
                                    try:
                                        entries = proc_fn(session, url)
                                    except Exception:
                                        try:
                                            entries = proc_fn(merged, session)
                                        except Exception:
                                            entries = []
                                except Exception as e:
                                    logger.exception("processor %s failed for feed %s: %s", name, key, e)
                                    entries = []

                                for e in entries or []:
                                    # Deduplicate by link or guid when merging
                                    link = (e.get('link') or '').strip()
                                    guid = (e.get('guid') or '').strip()
                                    key_id = link or guid or (e.get('title') or '')
                                    if key_id in seen:
                                        continue
                                    seen.add(key_id)
                                    merged.append(e)

                            return {"key": key, "title": title, "url": url, "publication_id": publication_doi, "error": None, "entries": merged}
                        except Exception as e:
                            return {"key": key, "title": title, "url": url, "publication_id": publication_doi, "error": str(e), "entries": []}

                    fut = ex.submit(_proc_multi, session, key, title, url, publication_doi, issn, proc_names)
                    futures[fut] = (key, title, url, publication_doi)
                else:
                    fut = ex.submit(feeds.fetch_feed, session, key, title, url, publication_doi, issn)
                    futures[fut] = (key, title, url, publication_doi)

            for fut in as_completed(futures):
                meta = futures[fut]
                try:
                    res = fut.result()
                except Exception as exc:
                    logger.error("Error fetching %s: %s", meta[2], exc)
                    continue
                if res.get("error"):
                    logger.warning("Failed: %s -> %s", meta[2], res["error"])
                    continue
                try:
                    from ednews.feeds import save_entries

                    cnt = save_entries(conn, res["key"], res["title"], res["entries"])
                    logger.info("%s: fetched %d entries, inserted %d", res["key"], len(res["entries"]), cnt)
                except Exception as e:
                    logger.exception("Failed to save entries for %s: %s", res.get("key"), e)
    else:
        # articles not requested; skip article fetching block
        pass

    # Run headlines fetch if requested
    if want_headlines:
        try:
            from ednews.news import fetch_all

            # use the same session and DB connection to persist headlines
            results = fetch_all(session=session, conn=conn)
            logger.info("Fetched headlines for %d sites", len(results))
        except Exception:
            logger.exception("Failed to fetch headlines")

    conn.close()


def cmd_build(args):
    out_dir = Path(args.out_dir) if args.out_dir else Path("build")
    build_mod.build(out_dir)


def cmd_embed(args):
    conn = sqlite3.connect(str(config.DB_PATH))
    embeddings.create_database(conn)
    # Determine which embedding sets to generate.
    want_articles = getattr(args, 'articles', False)
    want_headlines = getattr(args, 'headlines', False)

    # If neither flag is provided, default to generating both articles and headlines.
    if not want_articles and not want_headlines:
        want_articles = True
        want_headlines = True

    # Generate article embeddings if requested
    if want_articles:
        try:
            embeddings.generate_and_insert_embeddings_local(conn, model=args.model, batch_size=args.batch_size)
        except Exception:
            logger.exception("Failed to generate article embeddings")

    # Generate headline embeddings if requested
    if want_headlines:
        try:
            embeddings.create_headlines_vec(conn)
            embeddings.generate_and_insert_headline_embeddings(conn, model=args.model, batch_size=args.batch_size)
        except Exception:
            logger.exception("Failed to generate headline embeddings")
    conn.close()


def cmd_enrich_crossref(args):
    """Enrich articles missing Crossref XML by querying Crossref for metadata."""
    conn = sqlite3.connect(str(config.DB_PATH))
    # NOTE: database initialization (schema + views) is now managed via the
    # top-level `db-init` command. This command assumes the DB exists.
    from ednews.db import enrich_articles_from_crossref
    from ednews.crossref import fetch_crossref_metadata

    # db-init should be run explicitly; do not initialize here.
    # Use the existing fetcher function as a callable that takes a DOI and returns a dict
    def fetcher(doi):
        return fetch_crossref_metadata(doi, conn=conn)

    updated_ids = enrich_articles_from_crossref(conn, fetcher, batch_size=args.batch_size, delay=args.delay, return_ids=True)
    logger.info("Enriched %d articles from Crossref", len(updated_ids) if hasattr(updated_ids, '__len__') else updated_ids)
    # Update embeddings only for the affected article ids
    if updated_ids:
        try:
            embeddings.create_database(conn)
            embeddings.generate_and_insert_embeddings_for_ids(conn, updated_ids, model=args.model if hasattr(args, 'model') else None)
        except Exception:
            logger.exception("Failed to regenerate embeddings for updated articles after Crossref enrichment")
    conn.close()


def cmd_db_init(args):
    """Create DB schema and views. Intended to be run once when setting up the DB."""
    conn = sqlite3.connect(str(config.DB_PATH))
    try:
        from ednews.db import init_db

        init_db(conn)
        print("Database initialized (tables and views created)")
    except Exception:
        logger.exception("db-init failed")
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def cmd_issn_lookup(args):
    """Fetch latest works for journals that have an ISSN in the feeds list."""
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        logger.error("No feeds found; aborting ISSN lookup")
        return
    conn = sqlite3.connect(str(config.DB_PATH))
    from ednews.db.manage_db import fetch_latest_journal_works
    # Normalize user-provided date fragments (module-level helper)
    try:
        inserted = fetch_latest_journal_works(
            conn,
            feeds_list,
            per_journal=args.per_journal,
            timeout=args.timeout,
            delay=args.delay,
            sort_by=args.sort_by if hasattr(args, 'sort_by') else 'created',
            date_filter_type=getattr(args, 'date_filter_type', None),
            from_date=normalize_cli_date(getattr(args, 'from_date', None)),
            until_date=normalize_cli_date(getattr(args, 'until_date', None)),
        )
        logger.info("Inserted %d articles from ISSN lookups", inserted)
    except Exception:
        logger.exception("ISSN lookup failed")
    finally:
        conn.close()


def cmd_headlines(args):
    """Fetch configured news sites and print headlines as JSON.

    This is intentionally simple: it prints a JSON object mapping site
    keys to arrays of headlines. Use --out to write to file.
    """
    from ednews.news import fetch_all
    import json

    session = requests.Session()
    conn = None
    try:
        if not getattr(args, "no_persist", False):
            conn = sqlite3.connect(str(config.DB_PATH))
        results = fetch_all(session=session, conn=conn)
        if args.out:
            with open(args.out, "w", encoding="utf-8") as fh:
                json.dump(results, fh, ensure_ascii=False, indent=2)
            logger.info("Wrote news JSON to %s", args.out)
        else:
            # Do not emit JSON to stdout; persistence to DB is the default behaviour.
            logger.info("Fetched news%s", (" and persisted to DB" if conn else ""))
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def cmd_manage_db_cleanup(args):
    """CLI handler for cleaning up empty articles."""
    from ednews.db import manage_db
    conn = sqlite3.connect(str(config.DB_PATH))
    started = None
    run_id = None
    try:
        from datetime import datetime, timezone

        started = datetime.now(timezone.utc).isoformat()
        run_id = manage_db.log_maintenance_run(conn, "cleanup-empty-articles", "started", started, None, None, {"args": vars(args)})
        # If dry-run, compute count via a SELECT without deleting
        if getattr(args, 'dry_run', False):
            cur = conn.cursor()
            params = []
            where_clauses = ["(COALESCE(title, '') = '' AND COALESCE(abstract, '') = '')"]
            if getattr(args, 'older_than_days', None) is not None:
                from datetime import datetime, timezone, timedelta
                cutoff = (datetime.now(timezone.utc) - timedelta(days=int(args.older_than_days))).isoformat()
                where_clauses.append("(COALESCE(fetched_at, '') != '' AND COALESCE(fetched_at, '') < ? OR COALESCE(published, '') != '' AND COALESCE(published, '') < ?)")
                params.extend([cutoff, cutoff])
            where_sql = " AND ".join(where_clauses)
            cur.execute(f"SELECT COUNT(1) FROM articles WHERE {where_sql}", tuple(params))
            row = cur.fetchone()
            count = row[0] if row and row[0] else 0
            print(f"dry-run: would delete {count} rows")
            status = "dry-run"
            details = {"would_delete": count}
        else:
            deleted = manage_db.cleanup_empty_articles(conn, older_than_days=getattr(args, 'older_than_days', None))
            print(f"deleted {deleted} rows")
            status = "ok"
            details = {"deleted": deleted}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        try:
            from datetime import datetime, timezone

            finished = datetime.now(timezone.utc).isoformat()
            duration = None
            if started:
                from datetime import datetime as _dt
                duration = (_dt.fromisoformat(finished) - _dt.fromisoformat(started)).total_seconds()
            if run_id and conn:
                manage_db.log_maintenance_run(conn, "cleanup-empty-articles", status, started, finished, duration, details)
        except Exception:
            pass
        conn.close()


def cmd_manage_db_vacuum(args):
    from ednews.db import manage_db
    conn = sqlite3.connect(str(config.DB_PATH))
    started = None
    run_id = None
    try:
        from datetime import datetime, timezone

        started = datetime.now(timezone.utc).isoformat()
        run_id = manage_db.log_maintenance_run(conn, "vacuum", "started", started, None, None, {})
        ok = manage_db.vacuum_db(conn)
        print("vacuum: ok" if ok else "vacuum: failed")
        status = "ok" if ok else "failed"
        details = {}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        try:
            from datetime import datetime, timezone

            finished = datetime.now(timezone.utc).isoformat()
            duration = None
            if started:
                from datetime import datetime as _dt
                duration = (_dt.fromisoformat(finished) - _dt.fromisoformat(started)).total_seconds()
            if run_id and conn:
                manage_db.log_maintenance_run(conn, "vacuum", status, started, finished, duration, details)
        except Exception:
            pass
        conn.close()


def cmd_manage_db_migrate(args):
    from ednews.db import manage_db
    conn = sqlite3.connect(str(config.DB_PATH))
    started = None
    run_id = None
    try:
        from datetime import datetime, timezone

        started = datetime.now(timezone.utc).isoformat()
        run_id = manage_db.log_maintenance_run(conn, "migrate", "started", started, None, None, {})
        ok = manage_db.migrate_db(conn)
        print("migrate: ok" if ok else "migrate: failed")
        status = "ok" if ok else "failed"
        details = {}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        try:
            from datetime import datetime, timezone

            finished = datetime.now(timezone.utc).isoformat()
            duration = None
            if started:
                from datetime import datetime as _dt
                duration = (_dt.fromisoformat(finished) - _dt.fromisoformat(started)).total_seconds()
            if run_id and conn:
                manage_db.log_maintenance_run(conn, "migrate", status, started, finished, duration, details)
        except Exception:
            pass
        conn.close()


def cmd_manage_db_sync_publications(args):
    from ednews.db import manage_db
    from ednews import feeds
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        print("No feeds found; nothing to sync")
        return
    conn = sqlite3.connect(str(config.DB_PATH))
    started = None
    run_id = None
    try:
        from datetime import datetime, timezone

        started = datetime.now(timezone.utc).isoformat()
        run_id = manage_db.log_maintenance_run(conn, "sync-publications", "started", started, None, None, {"feed_count": len(feeds_list)})
        count = manage_db.sync_publications_from_feeds(conn, feeds_list)
        print(f"synced {count} publications")
        status = "ok"
        details = {"synced": count}
    except Exception as e:
        status = "failed"
        details = {"error": str(e)}
        raise
    finally:
        try:
            from datetime import datetime, timezone

            finished = datetime.now(timezone.utc).isoformat()
            duration = None
            if started:
                from datetime import datetime as _dt
                duration = (_dt.fromisoformat(finished) - _dt.fromisoformat(started)).total_seconds()
            if run_id and conn:
                manage_db.log_maintenance_run(conn, "sync-publications", status, started, finished, duration, details)
        except Exception:
            pass
        conn.close()


def cmd_manage_db_run_all(args):
    """Run migrate, sync publications, cleanup, and vacuum in order."""
    from ednews.db import manage_db
    from ednews import feeds
    # migrate
    print("Running migrations...")
    conn = sqlite3.connect(str(config.DB_PATH))
    try:
        mig_ok = manage_db.migrate_db(conn)
        print("migrate: ok" if mig_ok else "migrate: failed")
    finally:
        conn.close()


def cmd_serve(args):
    """Serve the static `build` directory over HTTP.

    Uses Python's built-in http.server. This command is useful for local
    development and previewing the generated site.
    """
    import http.server
    import socketserver
    from pathlib import Path

    directory = Path(args.directory) if getattr(args, "directory", None) else Path("build")
    if not directory.exists():
        logger.error("Build directory does not exist: %s", str(directory))
        return

    host = args.host if getattr(args, "host", None) else "127.0.0.1"
    port = int(args.port) if getattr(args, "port", None) else 8000

    handler_class = http.server.SimpleHTTPRequestHandler

    # Python >=3.7 accepts the `directory` kwarg to SimpleHTTPRequestHandler
    try:
        handler = lambda *p, directory=str(directory), **kw: handler_class(*p, directory=directory, **kw)
    except TypeError:
        # Fallback for older Python versions: chdir into the directory
        import os

        os.chdir(str(directory))
        handler = handler_class

    with socketserver.TCPServer((host, port), handler) as httpd:
        sa = httpd.socket.getsockname()
        logger.info("Serving %s on http://%s:%d", str(directory), sa[0], sa[1])
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("Shutting down server")
            httpd.shutdown()
    


def run():
    parser = argparse.ArgumentParser(prog="ednews")
    # Default --from-date to the first day of the month six months prior to today.
    # For example, 2025-10-20 -> 2025-04-01.
    try:
        from datetime import date

        _today = date.today()
        _month = _today.month - 6
        _year = _today.year
        if _month <= 0:
            _month += 12
            _year -= 1
        _default_from_date = f"{_year:04d}-{_month:02d}-01"
    except Exception:
        # Fallback to None if anything goes wrong computing the default
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

    p_enrich = sub.add_parser("enrich-crossref", help="Enrich articles missing Crossref XML")
    p_enrich.add_argument("--batch-size", type=int, default=20, help="Number of articles to enrich in one run")
    p_enrich.add_argument("--delay", type=float, default=0.1, help="Delay between individual fetches (seconds)")
    p_enrich.set_defaults(func=cmd_enrich_crossref)

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

    # One-time DB initialization command. Creates tables and the combined view.
    p_dbinit = sub.add_parser("db-init", help="Create DB schema and views (run once)")
    p_dbinit.set_defaults(func=cmd_db_init)

    # DB maintenance commands
    p_manage = sub.add_parser("manage-db", help="Database maintenance commands")
    manage_sub = p_manage.add_subparsers(dest="manage_cmd")
    # If manage-db is invoked without a subcommand, show its help instead of
    # leaving args.func unset which causes an AttributeError later.
    p_manage.set_defaults(func=lambda args: p_manage.print_help())

    p_cleanup = manage_sub.add_parser("cleanup-empty-articles", help="Remove articles with no title and no abstract")
    p_cleanup.add_argument("--older-than-days", type=int, default=None, help="Only delete articles older than this many days (based on fetched_at or published)")
    p_cleanup.add_argument("--dry-run", action="store_true", help="Do not delete; only report how many rows would be deleted")
    p_cleanup.set_defaults(func=lambda args: cmd_manage_db_cleanup(args))

    p_vacuum = manage_sub.add_parser("vacuum", help="Run VACUUM on the configured DB")
    p_vacuum.set_defaults(func=lambda args: cmd_manage_db_vacuum(args))

    p_migrate = manage_sub.add_parser("migrate", help="Run schema migrations (no-op placeholder)")
    p_migrate.set_defaults(func=lambda args: cmd_manage_db_migrate(args))

    p_sync = manage_sub.add_parser("sync-publications", help="Sync publications table from feeds list")
    p_sync.set_defaults(func=lambda args: cmd_manage_db_sync_publications(args))

    p_runall = manage_sub.add_parser("run-all", help="Run migrations, sync publications, cleanup, and vacuum in sequence")
    p_runall.add_argument("--older-than-days", type=int, default=None, help="Pass-through to cleanup step")
    p_runall.add_argument("--dry-run", action="store_true", help="Do not perform destructive actions; only report")
    p_runall.set_defaults(func=lambda args: cmd_manage_db_run_all(args))

    p_serve = sub.add_parser("serve", help="Serve the built static site from the build directory")
    p_serve.add_argument("--host", help="Host to bind to (default: 127.0.0.1)")
    p_serve.add_argument("--port", type=int, help="Port to listen on (default: 8000)")
    p_serve.add_argument("--directory", help="Directory to serve (default: build)")
    p_serve.set_defaults(func=cmd_serve)

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    run()
