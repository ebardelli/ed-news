"""Compatibility shim for ednews.db.manage_db used by CLI modules.

Pylance/static analyzers can't see the runtime synthesis of
``ednews.db.manage_db`` performed in ``ednews.db.__init__``. Create a
lightweight shim that re-exports the maintenance and migrations helpers so
imports like ``from ednews.db import manage_db`` resolve statically.

This module intentionally avoids heavyweight imports at top-level to keep
startup cheap; it re-exports names from sibling modules.
"""

from .schema import init_db, create_combined_view
from .maintenance import (
    sync_publications_from_feeds,
    fetch_latest_journal_works,
    vacuum_db,
    log_maintenance_run,
    cleanup_empty_articles,
    cleanup_filtered_titles,
    rematch_publication_dois,
)
from .migrations import migrate_db, migrate_add_items_url_hash

__all__ = [
    "init_db",
    "create_combined_view",
    "sync_publications_from_feeds",
    "fetch_latest_journal_works",
    "vacuum_db",
    "log_maintenance_run",
    "cleanup_empty_articles",
    "cleanup_filtered_titles",
    "rematch_publication_dois",
    "migrate_db",
    "migrate_add_items_url_hash",
]
from .maintenance import sync_articles_from_items

__all__.append("sync_articles_from_items")

from .maintenance import remove_feed_articles

__all__.append("remove_feed_articles")

# Convenience alias: historically callers may expect a `remove_` prefix.
from .maintenance import cleanup_filtered_titles as remove_filtered_titles

__all__.append("remove_filtered_titles")
