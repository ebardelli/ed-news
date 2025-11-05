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
from typing import Any, Callable


def resolve_postprocessor(proc_config: Any, preferred_proc_name: str | None = None) -> Callable | None:
    """Resolve a DB-level postprocessor callable.

    Resolution order:
    - If proc_config is set for a feed, try its 'post' name(s) or string(s) first.
    - Next, try the preferred_proc_name (typically from CLI --processor).
    - Finally, attempt to return the `crossref_postprocessor_db` if available.

    `proc_config` may be a string, a list/tuple of strings, or a dict with a
    `post` key. This helper will attempt to lookup `<name>_postprocessor_db`
    in this package first, then attempt to import the module by name and
    look for the same symbol as a fallback.
    """
    import importlib

    post_names = []
    if proc_config:
        if isinstance(proc_config, (list, tuple)):
            post_names = list(proc_config)
        elif isinstance(proc_config, dict):
            p = proc_config.get('post')
            if isinstance(p, (list, tuple)):
                post_names = list(p)
            elif isinstance(p, str):
                post_names = [p]
        elif isinstance(proc_config, str):
            post_names = [proc_config]

    # Try feed-configured names first
    for name in post_names:
        if not name:
            continue
        fn = getattr(globals().get('__name__') and globals(), f"{name}_postprocessor_db", None)
        # The above getattr usage will not find module-level symbols; instead
        # consult this module's globals directly
        if fn is None:
            fn = globals().get(f"{name}_postprocessor_db")
        if fn:
            return fn
        # Try importing the module by name and looking for the symbol
        try:
            mod = importlib.import_module(name)
            fn = getattr(mod, f"{name}_postprocessor_db", None)
            if fn:
                return fn
        except Exception:
            continue

    # Next try preferred name
    if preferred_proc_name:
        fn = globals().get(f"{preferred_proc_name}_postprocessor_db")
        if fn:
            return fn
        try:
            mod = importlib.import_module(preferred_proc_name)
            fn = getattr(mod, f"{preferred_proc_name}_postprocessor_db", None)
            if fn:
                return fn
        except Exception:
            pass

    # Final fallback: crossref_postprocessor_db if present
    fn = globals().get('crossref_postprocessor_db')
    if fn:
        return fn
    return None

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

# Export resolver helper
__all__.append('resolve_postprocessor')
