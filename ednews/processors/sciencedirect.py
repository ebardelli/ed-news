"""Helpers specific to ScienceDirect items.

Provides utilities to find ScienceDirect-hosted items in the DB and to
attempt enrichment by resolving DOIs and fetching Crossref metadata.
"""

import logging
from typing import List
import sqlite3
from .. import crossref
from .. import db as eddb

logger = logging.getLogger("ednews.processors.sciencedirect")


def find_sciencedirect_items_missing_metadata(conn: sqlite3.Connection, limit: int | None = None) -> List[dict]:
    """Return ScienceDirect items from `items` joining articles where available.

    This helper selects rows from `items` whose link contains 'sciencedirect.com'
    and returns a list of dicts describing each candidate for enrichment.
    """
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
    """Enrich ScienceDirect items by attempting to resolve DOIs and Crossref metadata.

    If `apply` is False this function performs a dry-run and logs actions it
    would take. When `apply` is True it will insert or update article rows
    accordingly and commit changes.
    """
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
            try:
                from ..feeds import normalize_doi

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

        # If the DOI already exists in the articles table, skip the Crossref
        # network lookup to avoid unnecessary API requests.
        cr = None
        try:
            if eddb.article_exists(conn, norm):
                logger.info("Skipping CrossRef lookup for DOI %s because it already exists in DB; loading stored metadata", norm)
                cr = eddb.get_article_metadata(conn, norm) or None
            else:
                cr = crossref.fetch_crossref_metadata(norm, conn=conn)
        except Exception:
            # Fall back to attempting the fetch if the existence check or metadata load fails
            try:
                cr = crossref.fetch_crossref_metadata(norm)
            except Exception:
                cr = None
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
            from ..db import ensure_article_row

            aid = ensure_article_row(conn, norm, title=title, authors=authors, abstract=abstract, feed_id=None, publication_id=None, issn=None)
            if aid:
                updated += 1
                logger.info("Updated article id=%s for DOI %s", aid, norm)
        except Exception as e:
            logger.warning("Failed to upsert article for DOI %s: %s", norm, e)

    if apply:
        conn.commit()
    return updated


def sciencedirect_feed_processor(session, feed_url: str, publication_id: str | None = None, issn: str | None = None):
    """Fetch a ScienceDirect RSS/Atom feed and augment entries with DOI when possible.

    This behaves like a feed fetcher: it returns a list of entry dicts similar
    to what `feeds.fetch_feed` returns in its `entries` list so callers can
    pass the result to `ednews.feeds.save_entries` unchanged.

    The processor attempts a CrossRef title->DOI lookup for items whose link
    contains 'sciencedirect.com' and which do not already include a DOI.
    If a DOI is found, the returned entry will include a top-level 'doi' key
    so the downstream `save_entries` flow can attach and upsert articles.
    """
    import feedparser
    from .. import crossref

    try:
        resp = session.get(feed_url, timeout=20, headers={"User-Agent": "ed-news-fetcher/1.0"})
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as e:
        logger.warning("sciencedirect_processor: failed to fetch %s: %s", feed_url, e)
        return []

    out = []
    for e in parsed.entries:
        guid = e.get("id") or e.get("guid") or e.get("link") or e.get("title")
        title = e.get("title", "")
        link = e.get("link", "")
        published = e.get("published") or e.get("updated") or ""
        summary = e.get("summary", "")

        entry = {
            "guid": guid,
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "_entry": e,
            "_feed_publication_id": publication_id,
            "_feed_issn": issn,
        }

        # If no DOI is present, and this looks like a ScienceDirect link,
        # try a Crossref title lookup (prefer the provided publication id).
        try:
            # extract existing doi-like fields from the feedparser entry
            existing_doi = None
            for k in ("doi", "dc:identifier"):
                v = e.get(k) if isinstance(e, dict) else None
                if v:
                    existing_doi = v
                    break
            if not existing_doi and ("sciencedirect.com" in (link or "") or "sciencedirect.com" in (e.get('link') or "")):
                if title and len(title) > 10:
                    # Try to find a DOI for this title in the local DB first to avoid
                    # performing a Crossref title lookup when the article is already known.
                    try:
                        from ednews import config as _cfg
                        import sqlite3
                        from ..db import get_article_by_title

                        try:
                            conn = sqlite3.connect(str(_cfg.DB_PATH))
                            try:
                                art = get_article_by_title(conn, title)
                                if art and art.get('doi'):
                                    entry['doi'] = art.get('doi')
                                    logger.info("sciencedirect_processor: found DOI %s for title %s from local DB", art.get('doi'), title)
                                    out.append(entry)
                                    continue
                            finally:
                                try:
                                    conn.close()
                                except Exception:
                                    pass
                        except Exception:
                            # DB lookup failed; fall back to Crossref title lookup
                            pass
                    except Exception:
                        # If imports fail, fall back to Crossref lookup
                        pass

                    try:
                        found = crossref.query_crossref_doi_by_title(title, preferred_publication_id=publication_id)
                        if found:
                            entry["doi"] = found
                            logger.info("sciencedirect_processor: found DOI %s for title %s", found, title)
                    except Exception:
                        logger.debug("sciencedirect_processor: CrossRef title lookup failed for %s", title)
        except Exception:
            logger.debug("sciencedirect_processor: DOI extraction/lookup failed for entry %s", title)

        out.append(entry)
    return out
