# Architecture

This document describes the high-level architecture and design of ed-news.

## Overview

ed-news is a static site generator and feed builder for curated education news. It aggregates content from multiple sources (RSS feeds, news sites, academic journals) and produces a unified website with RSS feeds.

## System Components

### 1. Data Sources

The system pulls data from two main types of sources:

**Article Feeds** (configured in `research.json`)
- Academic journal RSS/Atom feeds
- Working paper repositories
- Configured with metadata: title, feed URL, publication ID, ISSN, and processor

**News Headlines** (configured in `news.json`)
- News site RSS feeds
- HTML-based news sites (via custom processors)
- Local news sources and aggregators

### 2. Core Modules

```
ednews/
├── cli/                    # CLI subcommands (one module per command)
│   ├── __init__.py         # Argument parser wiring and run() entrypoint
│   ├── fetch.py            # cmd_fetch
│   ├── build.py            # cmd_build
│   ├── embed.py            # cmd_embed
│   ├── db_init.py          # cmd_db_init
│   ├── issn_lookup.py      # cmd_issn_lookup
│   ├── headlines.py        # cmd_headlines
│   ├── manage_db.py        # cmd_manage_db_* handlers
│   ├── postprocess.py      # cmd_postprocess
│   ├── serve.py            # cmd_serve
│   └── common.py           # Shared CLI utilities
├── feeds.py                # Feed parsing and normalization
├── news.py                 # News headline aggregation
├── build.py                # Static site generation and rendering
├── embeddings.py           # Vector embeddings for similarity
├── crossref.py             # Crossref API integration for metadata
├── config.py               # Configuration constants and paths
├── http.py                 # HTTP utilities with retry logic
├── text.py                 # Text encoding and repair utilities
├── db/                     # Database layer
│   ├── __init__.py         # DB API facade
│   ├── schema.py           # Schema initialization and views
│   ├── articles.py         # Article CRUD
│   ├── headlines.py        # Headline CRUD
│   ├── publications.py     # Publication metadata
│   ├── migrations.py       # Schema migrations
│   ├── conn.py             # Connection management
│   ├── utils.py            # DB utility functions
│   ├── maintenance.py      # Maintenance orchestration
│   ├── manage_db.py        # manage-db command implementations
│   ├── maintenance_cleanup.py
│   ├── maintenance_encoding.py
│   ├── maintenance_journal.py
│   ├── maintenance_log.py
│   ├── maintenance_rematch.py
│   ├── maintenance_remove.py
│   ├── maintenance_sync.py
│   └── maintenance_vacuum.py
└── processors/             # Feed/site-specific processors
    ├── rss.py              # Canonical RSS preprocessor
    ├── crossref.py         # Crossref postprocessor
    ├── sciencedirect.py    # ScienceDirect enrichment
    ├── edworkingpapers.py  # EdWorkingPapers processor
    ├── fcmat.py            # FCMAT site scraper
    └── pressdemocrat.py    # Press Democrat filter
```

### 3. Data Flow

```
┌─────────────────┐
│ Configuration   │
│ (research.json, │
│  news.json)     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│ Feed Fetching   │─────▶│ Preprocessor │
│ (feeds.py,      │      │ (processors) │
│  news.py)       │      └──────┬───────┘
└─────────┬───────┘             │
          │                     │
          ▼                     ▼
    ┌─────────────────────────────┐
    │   SQLite Database           │
    │   (items, articles,         │
    │    headlines, publications) │
    └──────────┬──────────────────┘
               │
               ▼
    ┌──────────────────┐
    │  Postprocessor   │
    │  (Crossref, etc) │
    └──────────┬───────┘
               │
               ▼
    ┌──────────────────┐
    │   Embeddings     │
    │   (optional)     │
    └──────────┬───────┘
               │
               ▼
    ┌──────────────────┐
    │  Build/Render    │
    │  (templates)     │
    └──────────┬───────┘
               │
               ▼
    ┌──────────────────┐
    │  Static Output   │
    │  (index.html,    │
    │   RSS feeds)     │
    └──────────────────┘
```

### 4. Processing Pipeline

The system uses a **preprocessor/postprocessor** architecture:

**Preprocessors**
- Run during feed fetching
- Parse and normalize feed entries
- Examples: RSS parser, HTML scrapers
- Signature: `(session, url, publication_id=None, issn=None) -> list[dict]`

**Postprocessors**
- Run after entries are saved to the database
- Enrich articles with additional metadata (DOIs, abstracts, etc.)
- Examples: Crossref enrichment, ScienceDirect metadata
- Signature (DB-level): `(conn, feed_key, entries, session=None, publication_id=None, issn=None) -> int`
- Signature (in-memory): `(entries, session=None, publication_id=None, issn=None) -> list[dict] | None`

### 5. Database Layer

The database layer (`ednews.db`) provides:
- Schema initialization and migrations
- Article/headline CRUD operations
- Publication metadata management
- Maintenance utilities (vacuum, cleanup, sync)

All database operations use SQLite with optional `sqlite-vec` for embeddings.

## Key Design Decisions

### Feed Configuration Format

Feeds are configured in JSON with a flexible processor field:

```json
{
  "feeds": {
    "feed-key": {
      "title": "Feed Title",
      "feed": "https://example.org/feed.xml",
      "publication_id": "10.1234",
      "issn": "1234-5678",
      "processor": {"pre": "rss", "post": "crossref"}
    }
  }
}
```

The `processor` field supports:
- String: `"rss"` (preprocessor only)
- Dict: `{"pre": "rss", "post": "crossref"}` (both)
- List: `["rss", "crossref"]` (legacy format)

### Deduplication Strategy

Items are deduplicated at multiple levels:

1. **URL hash**: Items table uses `url_hash` (SHA-256 of normalized link)
2. **GUID uniqueness**: Combination of `(guid, link, title, published)`
3. **DOI uniqueness**: Articles table uses DOI as unique key
4. **Headline uniqueness**: Headlines table uses `(link, title)` as unique key

### Embeddings (Optional)

Vector embeddings enable semantic similarity search:
- Uses `nomic-embed-text-v1.5` model via the nomic SDK
- Stored in `sqlite-vec` virtual tables
- Generated on-demand via `embed` CLI command
- Powers "similar headlines" feature in the UI

### Static Site Generation

The build process:
1. Reads articles and headlines from SQLite
2. Applies date-based limits with same-date expansion
3. Filters empty/placeholder items
4. Renders Jinja2 templates
5. Copies static assets (CSS, JS)
6. Generates multiple RSS feeds (combined, articles-only, headlines-only)

## Extension Points

### Adding a New Preprocessor

1. Create a processor function in `ednews/processors/`
2. Export it from `ednews/processors/__init__.py`
3. Use it in feed config: `"processor": {"pre": "your-processor"}`

### Adding a New Postprocessor

1. Create a postprocessor function with appropriate signature
2. Export it from `ednews/processors/__init__.py`
3. Use it in feed config: `"processor": {"post": "your-postprocessor"}`

### Adding a New CLI Command

1. Create `ednews/cli/mycommand.py` with a `cmd_mycommand(args)` function
2. Import and wire it into the `run()` parser in `ednews/cli/__init__.py`
3. Add tests in `tests/test_cli_*.py`

## Performance Considerations

- **Concurrent Fetching**: Feed fetching uses ThreadPoolExecutor (8 workers)
- **Batch Processing**: Embeddings are generated in configurable batches (default: 64)
- **Caching**: Crossref DOI lookups are cached via `@lru_cache`
- **Connection Pooling**: HTTP requests use `requests.Session()` for connection reuse
- **Retry Logic**: Crossref requests have configurable retry/backoff parameters

## Security

- HTTP requests use configurable timeouts to prevent hangs
- User-Agent header identifies the client
- No secrets should be committed to the repository
- Database is local SQLite (no network exposure by default)

## Testing Strategy

The project uses pytest with:
- Unit tests for individual functions
- Integration tests for Crossref API (opt-in via `RUN_CROSSREF_INTEGRATION=1`)
- Fixtures for test data in `tests/fixtures/`
- Monkeypatching for external dependencies

## Deployment

The built static site can be deployed to:
- Static hosting (GitHub Pages, Netlify, etc.)
- S3 + CloudFront
- Any web server (served via `main.py serve` for development)

The Python package can be installed via:
- `uv sync` (development)
- `pip install -e .` (editable install)
- Package distribution via setuptools
