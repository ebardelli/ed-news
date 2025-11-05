"""Collection of site/feed processors for ednews.

This package aggregates site-specific preprocessors and postprocessors
and exposes a stable set of symbols used by the rest of the application.
Keep this module small and single-purpose to avoid accidental
redefinitions that can hide processor implementations.
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
from .edworkingpapers import (
    edworkingpapers_processor,
    edworkingpapers_feed_processor,
    edworkingpapers_postprocessor_db,
)
from .rss import rss_preprocessor

# Backwards-compatible alias: some feed entries/configs reference a
# `crossref` processor by name expecting a `*_feed_processor` callable.
crossref_feed_processor = crossref_enricher_processor

# Treat existing feed processors as preprocessors (migration aliases)
sciencedirect_preprocessor = sciencedirect_feed_processor
edworkingpapers_preprocessor = edworkingpapers_feed_processor
pd_education_preprocessor = pd_education_feed_processor
fcmat_preprocessor = fcmat_processor

# Expose DB-level postprocessor aliases where implementations exist
sciencedirect_postprocessor_db = sciencedirect_postprocessor_db
crossref_postprocessor = crossref_enricher_processor
crossref_postprocessor_db = crossref_postprocessor_db

__all__ = [
    # processor functions
    "fcmat_processor",
    "pd_education_feed_processor",
    "find_sciencedirect_items_missing_metadata",
    "enrich_sciencedirect",
    "sciencedirect_feed_processor",
    "crossref_enricher_processor",
    "edworkingpapers_processor",
    "edworkingpapers_feed_processor",
    "edworkingpapers_postprocessor_db",
    "sciencedirect_postprocessor_db",
    "crossref_postprocessor_db",
    "rss_preprocessor",
    # migration aliases
    "sciencedirect_preprocessor",
    "edworkingpapers_preprocessor",
    "pd_education_preprocessor",
    "fcmat_preprocessor",
]
