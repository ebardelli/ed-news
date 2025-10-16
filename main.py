#!/usr/bin/env python3
"""Unified CLI entrypoint for ed-news.

Subcommands: fetch, build, embed
"""
import argparse
import logging
from pathlib import Path
from ednews import feeds, build as build_mod, embeddings
from ednews import config
import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger("ednews.main")


def cmd_fetch(args):
    feeds_list = feeds.load_feeds()
    if not feeds_list:
        logger.error("No feeds found; aborting fetch")
        return
    conn = sqlite3.connect(str(config.DB_PATH))
    # ensure DB initialized
    from ednews.db import init_db

    init_db(conn)

    # Ensure publications table is populated/upserted from planet.json feeds
    try:
        from ednews.db import sync_publications_from_feeds

        try:
            synced = sync_publications_from_feeds(conn, feeds_list)
            logger.info("synced %d publications from feeds", synced)
        except Exception:
            logger.debug("failed to sync publications from feeds list")
    except Exception:
        logger.debug("failed to import sync_publications_from_feeds")

    session = requests.Session()
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

    conn.close()


def cmd_build(args):
    out_dir = Path(args.out_dir) if args.out_dir else Path("build")
    build_mod.build(out_dir)


def cmd_embed(args):
    conn = sqlite3.connect(str(config.DB_PATH))
    embeddings.create_database(conn)
    embeddings.generate_and_insert_embeddings_local(conn, model=args.model, batch_size=args.batch_size)
    conn.close()


def cmd_enrich_crossref(args):
    """Enrich articles missing Crossref XML by querying Crossref for metadata."""
    conn = sqlite3.connect(str(config.DB_PATH))
    # ensure DB initialized
    from ednews.db import init_db, enrich_articles_from_crossref
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
    from ednews.db import init_db, fetch_latest_journal_works

    init_db(conn)
    try:
        inserted = fetch_latest_journal_works(conn, feeds_list, per_journal=args.per_journal, timeout=args.timeout, delay=args.delay)
        logger.info("Inserted %d articles from ISSN lookups", inserted)
    except Exception:
        logger.exception("ISSN lookup failed")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(prog="ednews")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="cmd")

    p_fetch = sub.add_parser("fetch", help="Fetch feeds and save items")
    p_fetch.set_defaults(func=cmd_fetch)

    p_build = sub.add_parser("build", help="Render static site into build/")
    p_build.add_argument("--out-dir", help="Output directory")
    p_build.set_defaults(func=cmd_build)

    p_embed = sub.add_parser("embed", help="Generate local embeddings and store in DB")
    p_embed.add_argument("--model", help="Embedding model", default=None)
    p_embed.add_argument("--batch-size", type=int, default=64)
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

    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
