"""Collection of site/feed processors for ednews.

This package contains lightweight processors that extract or filter
headlines from HTML pages or RSS/Atom feeds. Individual modules export
processor functions which are imported and used by `ednews.news`.
"""
from .fcmat import fcmat_processor
from .pressdemocrat import pd_education_feed_processor
from .sciencedirect import (
    find_sciencedirect_items_missing_metadata,
    enrich_sciencedirect,
    sciencedirect_feed_processor,
    sciencedirect_postprocessor_db,
)
from .crossref import crossref_enricher_processor, crossref_postprocessor_db
from .edworkingpapers import edworkingpapers_processor, edworkingpapers_feed_processor, edworkingpapers_postprocessor_db
from .rss import rss_preprocessor

# Backwards-compatible alias: some feed entries/configs reference a
# `crossref` processor by name expecting a `*_feed_processor` callable.
# Expose `crossref_feed_processor` as an alias to the enricher implementation
# so the CLI's processor merger will call it as an enricher when entries are
# available (and skip it when there are no entries to enrich).
crossref_feed_processor = crossref_enricher_processor

# New pre/postprocessor aliases for migration
# Treat existing feed processors as preprocessors
sciencedirect_preprocessor = sciencedirect_feed_processor
edworkingpapers_preprocessor = edworkingpapers_feed_processor
pd_education_preprocessor = pd_education_feed_processor
fcmat_preprocessor = fcmat_processor

# Expose DB-level postprocessor aliases where implementations exist
sciencedirect_postprocessor_db = sciencedirect_postprocessor_db
crossref_postprocessor = crossref_enricher_processor
crossref_postprocessor_db = crossref_postprocessor_db

__all__ = [
    "fcmat_processor",
    "pd_education_feed_processor",
    "find_sciencedirect_items_missing_metadata",
    "enrich_sciencedirect",
    "sciencedirect_feed_processor",
    "crossref_enricher_processor",
    "edworkingpapers_processor",
    "edworkingpapers_feed_processor",
    "edworkingpapers_postprocessor_db",
    # migration aliases
    "sciencedirect_preprocessor",
    "edworkingpapers_preprocessor",
    "pd_education_preprocessor",
    "fcmat_preprocessor",
    "sciencedirect_postprocessor_db",
    "crossref_postprocessor",
    "crossref_postprocessor_db",
    "rss_preprocessor",
]
"""Collection of site/feed processors for ednews.

This package contains lightweight processors that extract or filter
headlines from HTML pages or RSS/Atom feeds. Individual modules export
processor functions which are imported and used by `ednews.news`.
"""
from .fcmat import fcmat_processor
from .pressdemocrat import pd_education_feed_processor
from .sciencedirect import find_sciencedirect_items_missing_metadata, enrich_sciencedirect, sciencedirect_feed_processor
from .crossref import crossref_enricher_processor
from .edworkingpapers import edworkingpapers_processor, edworkingpapers_feed_processor

# Backwards-compatible alias: some feed entries/configs reference a
# `crossref` processor by name expecting a `*_feed_processor` callable.
# Expose `crossref_feed_processor` as an alias to the enricher implementation
# so the CLI's processor merger will call it as an enricher when entries are
# available (and skip it when there are no entries to enrich).
crossref_feed_processor = crossref_enricher_processor
# New pre/postprocessor aliases for migration
# Treat existing feed processors as preprocessors
sciencedirect_preprocessor = sciencedirect_feed_processor
edworkingpapers_preprocessor = edworkingpapers_feed_processor
pd_education_preprocessor = pd_education_feed_processor
fcmat_preprocessor = fcmat_processor

# Expose DB-level postprocessor aliases where implementations exist
sciencedirect_postprocessor_db = sciencedirect_postprocessor_db if 'sciencedirect_postprocessor_db' in globals() else enrich_sciencedirect
crossref_postprocessor = crossref_enricher_processor
crossref_postprocessor_db = crossref_postprocessor_db if 'crossref_postprocessor_db' in globals() else None

__all__ = [
    "fcmat_processor",
    "pd_education_feed_processor",
    "find_sciencedirect_items_missing_metadata",
    "enrich_sciencedirect",
    "sciencedirect_feed_processor",
    "crossref_enricher_processor",
    "edworkingpapers_processor",
    "edworkingpapers_feed_processor",
    # migration aliases
    "sciencedirect_preprocessor",
    "edworkingpapers_preprocessor",
    "pd_education_preprocessor",
    "fcmat_preprocessor",
    "sciencedirect_postprocessor_db",
    "crossref_postprocessor",
]
