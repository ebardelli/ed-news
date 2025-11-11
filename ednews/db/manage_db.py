"""Compatibility shim for ednews.db.manage_db used by CLI modules.

Pylance/static analyzers can't see the runtime synthesis of
``ednews.db.manage_db`` performed in ``ednews.db.__init__``. Create a
lightweight shim that re-exports the maintenance and migrations helpers so
imports like ``from ednews.db import manage_db`` resolve statically.

This module intentionally avoids heavyweight imports at top-level to keep
startup cheap; it re-exports names from sibling modules.
"""

from .schema import init_db, create_combined_view
from .maintenance_sync import sync_publications_from_feeds, sync_articles_from_items
from .maintenance_journal import fetch_latest_journal_works
from .maintenance_vacuum import vacuum_db
from .maintenance_log import log_maintenance_run
from .maintenance_cleanup import cleanup_empty_articles, cleanup_filtered_titles
from .maintenance_rematch import rematch_publication_dois
from .maintenance_remove import remove_feed_articles
from .migrations import migrate_db, migrate_add_items_url_hash

__all__ = [
    "init_db",
    "create_combined_view",
    "sync_publications_from_feeds",
    "sync_articles_from_items",
    "fetch_latest_journal_works",
    "vacuum_db",
    "log_maintenance_run",
    "cleanup_empty_articles",
    "cleanup_filtered_titles",
    "remove_filtered_titles",
    "rematch_publication_dois",
    "remove_feed_articles",
    "migrate_db",
    "migrate_add_items_url_hash",
]

# Convenience alias: historically callers may expect a `remove_` prefix.
remove_filtered_titles = cleanup_filtered_titles
