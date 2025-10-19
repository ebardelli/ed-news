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
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("ednews.cli")


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
    # ensure DB initialized (moved to manage_db)
    from ednews.manage_db import init_db

    init_db(conn)

    # Ensure publications table is populated/upserted from planet.json feeds
    try:
        from ednews.manage_db import sync_publications_from_feeds

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
                if len(item) >= 4:
                    key, title, url, publication_doi = item[:4]
                else:
                    continue
                fut = ex.submit(feeds.fetch_feed, session, key, title, url, publication_doi)
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
    # ensure DB initialized
    from ednews.manage_db import init_db
    from ednews.db import enrich_articles_from_crossref
    from ednews.crossref import fetch_crossref_metadata

    init_db(conn)
    # Use the existing fetcher function as a callable that takes a DOI and returns a dict
    def fetcher(doi):
        return fetch_crossref_metadata(doi)

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


def cmd_issn_lookup(args):
    """Fetch latest works for journals that have an ISSN in the feeds list."""
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        logger.error("No feeds found; aborting ISSN lookup")
        return
    conn = sqlite3.connect(str(config.DB_PATH))
    from ednews.manage_db import init_db, fetch_latest_journal_works

    init_db(conn)
    try:
        inserted = fetch_latest_journal_works(conn, feeds_list, per_journal=args.per_journal, timeout=args.timeout, delay=args.delay)
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
            # ensure DB initialized
            from ednews.db import init_db

            init_db(conn)
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
    from ednews import manage_db
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
    from ednews import manage_db
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
    from ednews import manage_db
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
    from ednews import manage_db, feeds
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
    from ednews import manage_db, feeds
    # migrate
    print("Running migrations...")
    conn = sqlite3.connect(str(config.DB_PATH))
    try:
        mig_ok = manage_db.migrate_db(conn)
        print("migrate: ok" if mig_ok else "migrate: failed")
    finally:
        conn.close()

    # sync publications
    print("Syncing publications from feeds...")
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        print("No feeds found; skipping sync-publications")
    else:
        if getattr(args, 'dry_run', False):
            print("dry-run: would sync publications from feeds (skipped)")
        else:
            conn = sqlite3.connect(str(config.DB_PATH))
            try:
                count = manage_db.sync_publications_from_feeds(conn, feeds_list)
                print(f"synced {count} publications")
            finally:
                conn.close()

    # cleanup
    print("Cleaning up empty articles...")
    if getattr(args, 'dry_run', False):
        # compute count without deleting
        conn = sqlite3.connect(str(config.DB_PATH))
        try:
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
            print(f"dry-run: would delete {count} empty articles")
        finally:
            conn.close()
    else:
        conn = sqlite3.connect(str(config.DB_PATH))
        try:
            deleted = manage_db.cleanup_empty_articles(conn, older_than_days=getattr(args, 'older_than_days', None))
            print(f"deleted {deleted} empty articles")
        finally:
            conn.close()

    # vacuum
    print("Running VACUUM...")
    if getattr(args, 'dry_run', False):
        print("dry-run: would vacuum DB")
    else:
        conn = sqlite3.connect(str(config.DB_PATH))
        try:
            ok = manage_db.vacuum_db(conn)
            print("vacuum: ok" if ok else "vacuum: failed")
        finally:
            conn.close()


def run():
    parser = argparse.ArgumentParser(prog="ednews")
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
    p_issn.add_argument("--per-journal", type=int, default=30, help="Number of works to fetch per journal (max 100)")
    p_issn.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    p_issn.add_argument("--delay", type=float, default=0.05, help="Delay between individual requests (seconds)")
    p_issn.set_defaults(func=cmd_issn_lookup)

    p_headlines = sub.add_parser("headlines", help="Fetch latest headlines from news.json sites")
    p_headlines.add_argument("--out", help="Write output JSON to this file")
    p_headlines.add_argument("--no-persist", action="store_true", help="Do not persist fetched headlines to the configured DB (default: persist)")
    p_headlines.set_defaults(func=cmd_headlines)

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

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    run()
