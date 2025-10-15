import logging
from typing import List
import sqlite3
from . import crossref
from . import db as eddb

logger = logging.getLogger("ednews.sciencedirect")


def find_sciencedirect_items_missing_metadata(conn: sqlite3.Connection, limit: int | None = None) -> List[dict]:
    cur = conn.cursor()
    q = (
        "SELECT i.id, i.doi, i.link, i.title, a.id as article_id, a.authors, a.abstract, a.crossref_xml "
        "FROM items i LEFT JOIN articles a ON a.doi = i.doi "
        "WHERE i.link LIKE '%sciencedirect.com%'"
    )
    if limit:
        q = q + f" LIMIT {int(limit)}"
    cur.execute(q)
    rows = cur.fetchall()
    results = []
    for r in rows:
        item_id, doi, link, title, article_id, authors, abstract, crossref_xml = r
        results.append({
            "item_id": item_id,
            "doi": doi,
            "link": link,
            "title": title,
            "article_id": article_id,
            "authors": authors,
            "abstract": abstract,
            "crossref_xml": crossref_xml,
        })
    return results


def enrich_sciencedirect(conn: sqlite3.Connection, limit: int | None = None, apply: bool = False, delay: float = 0.05) -> int:
    cur = conn.cursor()
    candidates = find_sciencedirect_items_missing_metadata(conn, limit=limit)
    updated = 0
    if not candidates:
        logger.info("No ScienceDirect items found for enrichment")
        return 0

    logger.info("Found %d ScienceDirect items to examine", len(candidates))
    for c in candidates:
        doi = c.get("doi")
        link = c.get("link")
        title = c.get("title")
        article_id = c.get("article_id")

        if doi:
            norm = None
            # reuse db.normalize? keep simple and call crossref.normalize in caller if needed
            try:
                from .feeds import normalize_doi

                norm = normalize_doi(doi)
            except Exception:
                norm = None
        else:
            norm = None

        logger.debug("Candidate: %s doi=%s article_id=%s", link, doi, article_id)

        if not norm and title:
            try:
                found = crossref.query_crossref_doi_by_title(title)
                if found:
                    norm = found
            except Exception:
                logger.debug("CrossRef title lookup failed for title: %s", title)

        if not norm:
            logger.info("Could not determine DOI for %s; skipping", link)
            continue

        cr = crossref.fetch_crossref_metadata(norm)
        if not cr:
            logger.info("CrossRef returned no metadata for DOI %s", norm)
            continue

        authors = cr.get("authors")
        abstract = cr.get("abstract")
        raw = cr.get("raw")

        logger.info("CrossRef: doi=%s authors=%s abstract=%s raw_len=%d", norm, bool(authors), bool(abstract), len(raw) if raw else 0)

        if not apply:
            logger.info("Dry-run: would upsert article with DOI %s", norm)
            continue

        try:
            from .db import ensure_article_row

            aid = ensure_article_row(conn, norm, title=title, authors=authors, abstract=abstract, feed_id=None, publication_id=None, issn=None)
            if aid:
                updated += 1
                logger.info("Updated article id=%s for DOI %s", aid, norm)
        except Exception as e:
            logger.warning("Failed to upsert article for DOI %s: %s", norm, e)

    if apply:
        conn.commit()
    return updated
