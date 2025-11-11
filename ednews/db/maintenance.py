"""Compatibility re-exports for maintenance helpers.

This module now forwards to smaller, focused modules:
- maintenance_log: log_maintenance_run
- maintenance_sync: sync_publications_from_feeds, sync_articles_from_items
- maintenance_cleanup: cleanup_empty_articles, cleanup_filtered_titles
- maintenance_journal: fetch_latest_journal_works
- maintenance_vacuum: vacuum_db
- maintenance_rematch: rematch_publication_dois
- maintenance_remove: remove_feed_articles
"""

from .maintenance_log import log_maintenance_run
from .maintenance_sync import (
    sync_publications_from_feeds,
    sync_articles_from_items,
)
from .maintenance_cleanup import (
    cleanup_empty_articles,
    cleanup_filtered_titles,
)
from .maintenance_journal import fetch_latest_journal_works
from .maintenance_vacuum import vacuum_db
from .maintenance_rematch import rematch_publication_dois
from .maintenance_remove import remove_feed_articles

__all__ = [
    "log_maintenance_run",
    "sync_publications_from_feeds",
    "sync_articles_from_items",
    "cleanup_empty_articles",
    "cleanup_filtered_titles",
    "fetch_latest_journal_works",
    "vacuum_db",
    "rematch_publication_dois",
    "remove_feed_articles",
]
