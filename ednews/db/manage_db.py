"""Compatibility shim module exposing maintenance and migration helpers.

This module re-exports functions from the focused modules in the db package.
It exists to preserve the historical public API `ednews.db.manage_db` while
keeping implementation split across smaller modules.
"""

from .schema import init_db, create_combined_view
from .migrations import migrate_db, migrate_add_items_url_hash
from .maintenance_vacuum import vacuum_db
from .maintenance_log import log_maintenance_run
from .maintenance_cleanup import cleanup_empty_articles, cleanup_filtered_titles
from .maintenance_sync import sync_articles_from_items
from .maintenance_journal import fetch_latest_journal_works
from .maintenance_rematch import rematch_publication_dois
from .maintenance_remove import remove_feed_articles
from .publications import sync_publications_from_feeds

__all__ = [
    "init_db",
    "create_combined_view",
    "migrate_db",
    "migrate_add_items_url_hash",
    "vacuum_db",
    "log_maintenance_run",
    "cleanup_empty_articles",
    "cleanup_filtered_titles",
    "sync_articles_from_items",
    "fetch_latest_journal_works",
    "rematch_publication_dois",
    "remove_feed_articles",
    "sync_publications_from_feeds",
]

# Convenience alias: historically callers may expect a `remove_` prefix.
remove_filtered_titles = cleanup_filtered_titles
