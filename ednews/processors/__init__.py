"""Collection of site/feed processors for ednews.

This package contains lightweight processors that extract or filter
headlines from HTML pages or RSS/Atom feeds. Individual modules export
processor functions which are imported and used by `ednews.news`.
"""
from .fcmat import fcmat_processor
from .pressdemocrat import pd_education_feed_processor
from .sciencedirect import find_sciencedirect_items_missing_metadata, enrich_sciencedirect, sciencedirect_feed_processor
from .crossref import crossref_enricher_processor

__all__ = [
    "fcmat_processor",
    "pd_education_feed_processor",
    "find_sciencedirect_items_missing_metadata",
    "enrich_sciencedirect",
    "sciencedirect_feed_processor",
    "crossref_enricher_processor",
]
