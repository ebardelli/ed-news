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
                doi = feeds_mod.normalize_doi(entry.get('doi'))
            else:
                src = entry.get('_entry') or entry
                doi = feeds_mod.extract_and_normalize_doi(src) if src else None
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
