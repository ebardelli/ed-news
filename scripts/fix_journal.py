#!/usr/bin/env python3
"""Maintenance script: enrich all articles for a feed from Crossref and update embeddings.

This mirrors the style of `scripts/fix_ellipses.py` but targets all articles with a
given `feed_id`. By default the script runs in dry-run mode and only prints actions.
Use --apply to perform DB updates and regenerate embeddings.
"""
from __future__ import annotations

import argparse
import logging
import sqlite3
from typing import List

import os
import sys

# Ensure project root is on sys.path so `from ednews import ...` works when this
# script is run from scripts/ or other locations.
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from ednews import config
from ednews import db as eddb
from ednews import crossref
from ednews import embeddings

logger = logging.getLogger("scripts.fix_journal")


def find_articles_for_feed(conn: sqlite3.Connection, feed_id: str) -> List[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute(
        "SELECT id, doi, title, abstract, authors, crossref_xml, published FROM articles WHERE feed_id = ?",
        (feed_id,),
    )
    rows = cur.fetchall()
    return rows


def augment_article_from_crossref(conn: sqlite3.Connection, doi: str, dry_run: bool = True) -> bool:
    """Fetch crossref metadata for DOI and update articles row if useful data found.

    Returns True if an update would be or was performed.
    """
    cr = None
    try:
        cr = crossref.fetch_crossref_metadata(doi)
    except Exception:
        logger.exception("Crossref fetch failed for %s", doi)
        return False

    if not cr:
        logger.debug("No crossref data for %s", doi)
        return False

    authors = cr.get("authors")
    abstract = cr.get("abstract")
    raw = cr.get("raw")
    published = cr.get("published")

    if not any((authors, abstract, raw, published)):
        logger.debug("Crossref returned no useful fields for %s", doi)
        return False

    if dry_run:
        logger.info(
            "[dry-run] would update doi=%s authors=%s abstract_present=%s published=%s",
            doi,
            bool(authors),
            bool(abstract),
            published,
        )
        return True

    # perform update
    cur = conn.cursor()
    try:
        if authors:
            cur.execute("UPDATE articles SET authors = COALESCE(?, authors) WHERE doi = ?", (authors, doi))
        if abstract:
            cur.execute("UPDATE articles SET abstract = COALESCE(?, abstract) WHERE doi = ?", (abstract, doi))
        if raw:
            cur.execute("UPDATE articles SET crossref_xml = ? WHERE doi = ?", (raw, doi))
        if published:
            cur.execute("UPDATE articles SET published = COALESCE(?, published) WHERE doi = ?", (published, doi))
        conn.commit()
        logger.info("Updated article doi=%s", doi)
        return True
    except Exception:
        logger.exception("Failed to update article doi=%s", doi)
        return False


def main(argv=None):
    p = argparse.ArgumentParser(description="Enrich all articles for a feed using Crossref and update embeddings")
    p.add_argument("--feed", required=True, help="Feed id to process (matches articles.feed_id)")
    p.add_argument("--apply", action="store_true", help="Apply changes to the DB and regenerate embeddings (default: dry-run)")
    p.add_argument("--recompute-all", action="store_true", help="Force recomputing embeddings for all articles (default: will call generator which manages existing rows)")
    p.add_argument("--db", default=str(config.DB_PATH), help="Path to sqlite database")
    p.add_argument("--limit", type=int, default=0, help="Limit number of articles to process (0 means no limit)")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO)

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row

    rows = find_articles_for_feed(conn, args.feed)
    total = len(rows)
    logger.info("Found %d articles for feed %s", total, args.feed)

    if args.limit and args.limit > 0:
        rows = rows[: args.limit]

    updated_ids = []
    updated_count = 0
    for r in rows:
        aid = r[0]
        doi = r[1]
        title = r[2]
        abstract = r[3]
        authors = r[4]
        logger.info("Processing id=%s doi=%s title=%s", aid, doi, (title or '')[:80])
        if not doi:
            logger.info("Skipping id=%s (no DOI)", aid)
            continue
        ok = augment_article_from_crossref(conn, doi, dry_run=not args.apply)
        if ok and args.apply:
            updated_count += 1
            updated_ids.append(aid)
            # regenerate embedding immediately for the updated article(s).
            try:
                logger.info("Generating embedding for article id=%s doi=%s", aid, doi)
                written = embeddings.generate_and_insert_embeddings_for_ids(conn, [aid])
                logger.info("Wrote %d embeddings for doi=%s id=%s", written, doi, aid)
            except Exception:
                logger.exception("Failed to generate embeddings after updating doi=%s", doi)

    logger.info("Augmentation pass complete: %d updated (apply=%s)", updated_count, args.apply)

    # Recompute embeddings. The existing embedding function will skip if all embeddings exist.
    if args.apply:
        try:
            if args.recompute_all:
                logger.info("Recomputing embeddings for all articles")
                embeddings.generate_and_insert_embeddings_local(conn)
            else:
                logger.info("Generating embeddings for updated ids (will skip existing ones)")
                if updated_ids:
                    embeddings.generate_and_insert_embeddings_for_ids(conn, updated_ids)
                else:
                    logger.info("No updated articles; skipping targeted embedding generation")
        except Exception:
            logger.exception("Failed to generate embeddings")


if __name__ == "__main__":
    main()
