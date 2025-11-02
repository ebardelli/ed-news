"""Processor adapter that enriches feed entries using ednews.crossref helpers.

This module exposes an enricher-style processor named
`crossref_enricher_processor(entries, session=None, publication_id=None, issn=None)`
which accepts a list of feed entry dicts and returns an enriched list.
"""
from typing import List
import logging

from ednews import crossref as cr
from ednews import feeds as feeds_mod

logger = logging.getLogger("ednews.processors.crossref")


def crossref_enricher_processor(entries: List[dict], session=None, publication_id: str | None = None, issn: str | None = None) -> List[dict]:
    """Enrich a list of feed entries using Crossref metadata.

    For each entry, attempt to determine a DOI (from the entry fields or
    via a Crossref title lookup) and fetch Crossref metadata. When found,
    merge authors, abstract, published and raw crossref payload into the
    entry dict under keys 'doi', 'authors', 'abstract', 'crossref_raw', 'published'.
    """
    if not entries:
        return []

    out = []
    for e in entries:
        entry = dict(e) if isinstance(e, dict) else {}
        # Try existing DOI extraction from feed helpers
        doi = None
        try:
            # prefer explicit fields
            if entry.get('doi'):
                doi = feeds_mod.normalize_doi(entry.get('doi'), preferred_publication_id=publication_id)
            else:
                src = entry.get('_entry') or entry
                doi = feeds_mod.extract_and_normalize_doi(src, preferred_publication_id=publication_id) if src else None
        except Exception:
            doi = None

        title = entry.get('title') or (entry.get('_entry') or {}).get('title') if isinstance(entry.get('_entry'), dict) else entry.get('title')

        if not doi and title and feeds_mod.title_suitable_for_crossref_lookup(title):
            try:
                found = cr.query_crossref_doi_by_title(title, preferred_publication_id=publication_id)
                if found:
                    doi = found
            except Exception:
                logger.debug("crossref_enricher: title lookup failed for %s", title)

        if doi:
            try:
                meta = cr.fetch_crossref_metadata(doi)
            except Exception:
                meta = None
            if isinstance(meta, dict):
                # prefer Crossref-provided values but don't clobber existing ones
                if meta.get('authors'):
                    entry['authors'] = meta.get('authors')
                if meta.get('abstract'):
                    entry['abstract'] = meta.get('abstract')
                if meta.get('raw'):
                    entry['crossref_raw'] = meta.get('raw')
                if meta.get('published'):
                    entry['published'] = meta.get('published')
                entry['doi'] = doi
        out.append(entry)
    return out


def crossref_postprocessor_db(conn, feed_key: str, entries, session=None, publication_id: str | None = None, issn: str | None = None, force: bool = False, check_fields: list[str] | None = None):
    """DB-level postprocessor: for each entry, determine DOI and upsert article rows.

    This function mirrors the behavior of `crossref_enricher_processor` but
    performs DB writes using `ednews.db` helpers so that articles and items
    are persisted with Crossref metadata attached.
    """
    if not entries:
        return 0
    try:
        from ednews import feeds as feeds_mod
        from ednews import db as eddb
        from ednews import crossref as crossref_mod
    except Exception:
        return 0

    cur = conn.cursor()
    updated = 0
    for e in entries:
        try:
            doi = None
            try:
                if e.get('doi'):
                    doi = feeds_mod.normalize_doi(e.get('doi'), preferred_publication_id=publication_id)
                else:
                    src = e.get('_entry') or e
                    doi = feeds_mod.extract_and_normalize_doi(src, preferred_publication_id=publication_id) if src else None
            except Exception:
                doi = None

            title = e.get('title') or (e.get('_entry') or {}).get('title') if isinstance(e.get('_entry'), dict) else e.get('title')

            if not doi and title and feeds_mod.title_suitable_for_crossref_lookup(title):
                try:
                    found = crossref_mod.query_crossref_doi_by_title(title, preferred_publication_id=publication_id)
                    if found:
                        doi = found
                except Exception:
                    doi = None

            if not doi:
                continue

            try:
                doi = feeds_mod.normalize_doi(doi) or doi
            except Exception:
                pass

            # Avoid repeated network lookups when article exists. If the
            # article already has Crossref-derived metadata (raw, authors,
            # and abstract) skip it. If some pieces are missing, perform a
            # network lookup (do not pass the DB conn to force fetching).
            cr = None
            try:
                if eddb.article_exists(conn, doi):
                    existing = eddb.get_article_metadata(conn, doi) or {}
                    # Determine if we should skip based on check_fields or default set
                    if check_fields and isinstance(check_fields, (list, tuple)) and len(check_fields) > 0:
                        all_present = True
                        for f in check_fields:
                            if not existing.get(f):
                                all_present = False
                                break
                    else:
                        # default behavior: require raw, authors, and abstract
                        all_present = bool(existing.get('raw')) and bool(existing.get('authors')) and bool(existing.get('abstract'))

                    if all_present and not force:
                        # nothing to do for this DOI
                        continue

                    # Otherwise, fetch from Crossref (force network fetch when requested)
                    try:
                        cr = crossref_mod.fetch_crossref_metadata(doi, conn=conn, force=force)
                    except Exception:
                        cr = None
                else:
                    # No article row exists yet; use fetcher that may avoid
                    # redundant work by checking DB when supported.
                    try:
                        cr = crossref_mod.fetch_crossref_metadata(doi, conn=conn, force=force)
                    except Exception:
                        cr = None
            except Exception:
                try:
                    cr = crossref_mod.fetch_crossref_metadata(doi)
                except Exception:
                    cr = None

            authors = cr.get('authors') if isinstance(cr, dict) else None
            abstract = cr.get('abstract') if isinstance(cr, dict) else None
            raw = cr.get('raw') if isinstance(cr, dict) else None
            published = cr.get('published') if isinstance(cr, dict) else (e.get('published') or None)

            title_final = title or e.get('title')
            authors_final = authors or (e.get('authors') if e.get('authors') else None)
            abstract_final = abstract or (e.get('abstract') if e.get('abstract') else None)
            published_final = published

            aid = eddb.upsert_article(conn, doi, title=title_final, authors=authors_final, abstract=abstract_final, feed_id=feed_key, publication_id=publication_id, issn=issn, published=published_final)
            if aid:
                updated += 1
                if raw:
                    try:
                        eddb.update_article_crossref(conn, doi, authors=authors_final, abstract=abstract_final, raw=raw, published=published_final)
                    except Exception:
                        pass

                # Attach DOI to items rows (by link/guid/url_hash)
                try:
                    link = (e.get('link') or '').strip()
                    guid = (e.get('guid') or '').strip()
                    if link:
                        cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND link = ?", (doi, feed_key, link))
                    if guid:
                        cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?", (doi, feed_key, guid))
                    try:
                        import hashlib
                        if link:
                            url_hash = hashlib.sha256(link.encode('utf-8')).hexdigest()
                            cur.execute("UPDATE items SET doi = ? WHERE feed_id = ? AND url_hash = ?", (doi, feed_key, url_hash))
                    except Exception:
                        pass
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            continue
    return updated
