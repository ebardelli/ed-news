"""ednews.db package facade.

Thin re-exports of connection, schema, CRUD helpers, and maintenance utilities
split across focused modules. Maintains backward compatibility for
`from ednews import db` and `from ednews.db import manage_db`.
"""
import logging
import types
import sys

logger = logging.getLogger("ednews.db")

# Core schema helpers
from .schema import init_db, create_combined_view

# Connection helper
from .conn import get_connection

# Articles API
from .articles import (
    upsert_article,
    ensure_article_row,
    article_exists,
    get_article_metadata,
    get_article_by_title,
    enrich_articles_from_crossref,
    get_missing_crossref_dois,
    update_article_crossref,
)

# Publications API
from .publications import (
    upsert_publication,
    sync_publications_from_feeds,
)

# Headlines API
from .headlines import (
    upsert_news_item,
    save_headlines,
    save_news_items,
)

# Maintenance utilities
from .maintenance_sync import sync_articles_from_items
from .maintenance_journal import fetch_latest_journal_works
from .maintenance_vacuum import vacuum_db
from .maintenance_log import log_maintenance_run
from .maintenance_cleanup import cleanup_empty_articles, cleanup_filtered_titles
from .maintenance_rematch import rematch_publication_dois
from .maintenance_remove import remove_feed_articles

# Migrations
from .migrations import migrate_db, migrate_add_items_url_hash

# Synthesize ednews.db.manage_db module for backward compatibility
try:
    mod_name = __name__ + ".manage_db"
    if mod_name not in sys.modules:
        manage_mod = types.ModuleType(mod_name)
        manage_mod.init_db = init_db
        manage_mod.create_combined_view = create_combined_view
        manage_mod.sync_publications_from_feeds = sync_publications_from_feeds
        manage_mod.sync_articles_from_items = sync_articles_from_items
        manage_mod.fetch_latest_journal_works = fetch_latest_journal_works
        manage_mod.migrate_db = migrate_db
        manage_mod.migrate_add_items_url_hash = migrate_add_items_url_hash
        manage_mod.vacuum_db = vacuum_db
        manage_mod.log_maintenance_run = log_maintenance_run
        manage_mod.cleanup_empty_articles = cleanup_empty_articles
        manage_mod.cleanup_filtered_titles = cleanup_filtered_titles
        manage_mod.rematch_publication_dois = rematch_publication_dois
        manage_mod.remove_feed_articles = remove_feed_articles
        sys.modules[mod_name] = manage_mod
except Exception:
    logger.exception("Failed to synthesize ednews.db.manage_db module")

__all__ = [
    # schema/conn
    "get_connection",
    "init_db",
    "create_combined_view",
    # migrations
    "migrate_db",
    "migrate_add_items_url_hash",
    # maintenance
    "sync_publications_from_feeds",
    "sync_articles_from_items",
    "fetch_latest_journal_works",
    "vacuum_db",
    "log_maintenance_run",
    "cleanup_empty_articles",
    "cleanup_filtered_titles",
    "rematch_publication_dois",
    "remove_feed_articles",
    # articles
    "upsert_article",
    "ensure_article_row",
    "article_exists",
    "get_article_metadata",
    "get_article_by_title",
    "enrich_articles_from_crossref",
    "get_missing_crossref_dois",
    "update_article_crossref",
    # publications
    "upsert_publication",
    # headlines
    "upsert_news_item",
    "save_headlines",
    "save_news_items",
]
