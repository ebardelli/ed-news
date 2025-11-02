# Database Schema

This document describes the SQLite database schema used by ed-news.

## Overview

ed-news uses a local SQLite database (`ednews.db`) to store fetched articles, headlines, and metadata. The schema is initialized via `uv run python main.py db-init` and managed through the `ednews.db` module.

## Tables

### `items`

Stores feed items (articles) from RSS/Atom feeds.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PRIMARY KEY | Auto-incrementing item ID |
| `doi` | TEXT | Digital Object Identifier (when available) |
| `feed_id` | TEXT | Feed key from configuration (e.g., "aerj") |
| `guid` | TEXT | Feed entry GUID/ID |
| `title` | TEXT | Article title |
| `link` | TEXT | Article URL |
| `url_hash` | TEXT | SHA-256 hash of normalized URL (for deduplication) |
| `published` | TEXT | Publication date (ISO format) |
| `summary` | TEXT | Article summary/description |
| `fetched_at` | TEXT | Timestamp when item was fetched |

**Unique Constraints:**
- `UNIQUE(url_hash)` - Prevents duplicate URLs
- `UNIQUE(guid, link, title, published)` - Prevents duplicate entries

### `articles`

Stores enriched article metadata (typically from Crossref).

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PRIMARY KEY | Auto-incrementing article ID |
| `doi` | TEXT | Digital Object Identifier (unique key) |
| `title` | TEXT | Article title |
| `authors` | TEXT | Author list (comma-separated or formatted) |
| `abstract` | TEXT | Article abstract |
| `crossref_xml` | TEXT | Raw Crossref XML response (if fetched) |
| `feed_id` | TEXT | Feed key from configuration |
| `publication_id` | TEXT | Publication DOI prefix (e.g., "10.3102") |
| `issn` | TEXT | Publication ISSN |
| `published` | TEXT | Publication date |
| `fetched_at` | TEXT | Timestamp when article was fetched |

**Unique Constraints:**
- `UNIQUE(doi)` - Each DOI appears only once

### `publications`

Stores publication metadata for journals and sources.

| Column | Type | Description |
|--------|------|-------------|
| `feed_id` | TEXT | Feed key from configuration |
| `publication_id` | TEXT NOT NULL | Publication DOI prefix |
| `feed_title` | TEXT | Human-readable publication name |
| `issn` | TEXT NOT NULL | Publication ISSN |

**Primary Key:**
- `PRIMARY KEY (publication_id, issn)` - Composite key

This table is synchronized from feed configurations via the `sync-publications` maintenance command.

### `headlines`

Stores news headlines from news sites and RSS feeds.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PRIMARY KEY | Auto-incrementing headline ID |
| `source` | TEXT | Source key from news.json (e.g., "fcmat") |
| `title` | TEXT | Headline title |
| `text` | TEXT | Headline text/summary |
| `link` | TEXT | Article URL |
| `first_seen` | TEXT | Timestamp when headline was first seen |
| `published` | TEXT | Publication date (when available) |

**Unique Constraints:**
- `UNIQUE(link, title)` - Prevents duplicate headlines

**Indexes:**
- `idx_headlines_source_first_seen` on `(source, first_seen)` - For efficient queries

### `maintenance_runs`

Logs database maintenance operations.

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER PRIMARY KEY | Auto-incrementing run ID |
| `command` | TEXT NOT NULL | Maintenance command executed |
| `status` | TEXT | Status (success, error, etc.) |
| `started` | TEXT | Start timestamp |
| `finished` | TEXT | End timestamp |
| `duration` | REAL | Duration in seconds |
| `details` | TEXT | Additional details (JSON or text) |

## Views

### `combined_articles`

Joins articles with publication metadata for display.

```sql
CREATE VIEW combined_articles AS
SELECT
    articles.doi AS doi,
    COALESCE(articles.title, '') AS title,
    ('https://doi.org/' || articles.doi) AS link,
    COALESCE(publications.feed_title, feeds.feed_title, '') AS feed_title,
    COALESCE(articles.abstract, '') AS content,
    COALESCE(articles.published, articles.fetched_at) AS published,
    COALESCE(articles.authors, '') AS authors
FROM articles
    LEFT JOIN publications on publications.feed_id = articles.feed_id
    LEFT JOIN publications as feeds on feeds.feed_id = articles.feed_id
WHERE articles.doi IS NOT NULL
```

## Virtual Tables (Optional)

When `sqlite-vec` is available, the following virtual tables store embeddings:

### `articles_vec`

Stores article embeddings for similarity search.

| Column | Type | Description |
|--------|------|-------------|
| `rowid` | INTEGER | Maps to `articles.id` |
| `embedding` | BLOB | Float32 vector (768 dimensions) |

### `headlines_vec`

Stores headline embeddings for similarity search.

| Column | Type | Description |
|--------|------|-------------|
| `rowid` | INTEGER | Maps to `headlines.id` |
| `embedding` | BLOB | Float32 vector (768 dimensions) |

Embeddings are generated via the `embed` CLI command and use the `nomic-embed-text-v1.5` model.

## Common Queries

### Fetch Recent Articles

```sql
SELECT doi, title, published, feed_title
FROM combined_articles
ORDER BY published DESC
LIMIT 20
```

### Find Articles by Publication

```sql
SELECT * FROM articles
WHERE publication_id = '10.3102'
ORDER BY published DESC
```

### Get Headlines by Source

```sql
SELECT title, link, published
FROM headlines
WHERE source = 'fcmat'
ORDER BY first_seen DESC
LIMIT 10
```

### Find Similar Articles (with embeddings)

```sql
SELECT
    a.id,
    a.title,
    distance
FROM articles a
JOIN (
    SELECT
        rowid,
        distance
    FROM articles_vec
    WHERE embedding MATCH ?
    ORDER BY distance
    LIMIT 5
) v ON a.id = v.rowid
```

## Maintenance Operations

The `manage-db` CLI command provides several maintenance operations:

### `migrate`
Runs schema migrations (e.g., adding `url_hash` column).

```bash
uv run python main.py manage-db migrate
```

### `vacuum`
Compacts the database and frees unused space.

```bash
uv run python main.py manage-db vacuum
```

### `cleanup-empty-articles`
Removes articles with no title or abstract older than N days.

```bash
uv run python main.py manage-db cleanup-empty-articles --older-than-days 90
```

### `sync-publications`
Synchronizes the `publications` table with feed configurations.

```bash
uv run python main.py manage-db sync-publications
```

### `run-all`
Runs all maintenance operations in sequence.

```bash
uv run python main.py manage-db run-all
```

## Data Lifecycle

### Article Lifecycle

1. **Fetch**: RSS feed is fetched and parsed
2. **Preprocess**: Preprocessor normalizes entries
3. **Save**: Items are inserted into `items` table (with deduplication)
4. **Postprocess**: Postprocessor enriches articles (e.g., Crossref lookup)
5. **Enrich**: Articles are inserted into `articles` table with metadata
6. **Embed** (optional): Embeddings are generated and stored in `articles_vec`
7. **Build**: Articles are read and rendered into static site
8. **Cleanup**: Old/empty articles are removed by maintenance scripts

### Headline Lifecycle

1. **Fetch**: News site is scraped or RSS feed is parsed
2. **Normalize**: Headlines are normalized to common format
3. **Save**: Headlines are inserted into `headlines` table (with deduplication)
4. **Embed** (optional): Embeddings are generated and stored in `headlines_vec`
5. **Build**: Headlines are read and rendered into static site

## Migration History

- **Initial schema**: Tables for `items`, `articles`, `publications`, `headlines`
- **url_hash migration**: Added `url_hash` column to `items` for better deduplication
- **embeddings**: Added virtual tables for vector storage (optional, requires `sqlite-vec`)

## Best Practices

- **Backup**: Regularly backup `ednews.db` before migrations or major operations
- **Vacuum**: Run vacuum periodically to reclaim space after deletions
- **Deduplication**: The schema enforces uniqueness constraints; handle conflicts gracefully
- **Indexing**: Indexes exist on commonly-queried columns; add more if needed
- **NULL handling**: Use `COALESCE` in queries to handle NULL values
- **Date formats**: Store dates in ISO 8601 format for consistency

## Troubleshooting

### "Database is locked" errors
- Close other connections before running migrations
- Use `conn.commit()` after writes to release locks

### "UNIQUE constraint failed" errors
- Expected when inserting duplicate items
- Feed processors should handle gracefully (log and continue)

### Missing embeddings
- Ensure `sqlite-vec` is installed: `pip install sqlite-vec`
- Run `uv run python main.py embed` to generate embeddings
- Check that `sqlite3` supports loadable extensions

### Schema out of sync
- Run `uv run python main.py db-init` to create missing tables
- Run `uv run python main.py manage-db migrate` for schema updates
