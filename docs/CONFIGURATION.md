# Configuration Guide

This document describes how to configure feeds and news sources for ed-news.

## Configuration Files

ed-news uses two JSON configuration files:

- **`research.json`** - Academic feeds and journal sources
- **`news.json`** - News sites and headline sources

## research.json Format

The `research.json` file configures academic journal feeds and working paper repositories.

### Basic Structure

```json
{
  "title": "Recent Publications in Education",
  "feeds": {
    "feed-key": {
      "title": "Feed Title",
      "link": "https://journal-website.org",
      "feed": "https://journal-website.org/feed.xml",
      "publication_id": "10.1234",
      "issn": "1234-5678",
      "processor": {"pre": "rss", "post": "crossref"}
    }
  }
}
```

### Field Descriptions

| Field | Required | Description |
|-------|----------|-------------|
| `title` | Yes | Top-level title for the feed collection |
| `feeds` | Yes | Object containing feed configurations |

### Feed Configuration

Each feed in the `feeds` object has the following fields:

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `title` | Yes | string | Human-readable feed title |
| `link` | Yes | string | URL to the journal/source homepage |
| `feed` | Yes | string | URL to the RSS/Atom feed |
| `publication_id` | No | string | DOI prefix (e.g., "10.3102") |
| `issn` | No | string | ISSN for the publication |
| `processor` | No | string/object/array | Processor configuration (see below) |

### Processor Configuration

The `processor` field controls how feeds are fetched and enriched. It supports multiple formats:

#### String Format (Preprocessor only)

```json
"processor": "rss"
```

This runs only the specified preprocessor (e.g., `rss_preprocessor`).

#### Object Format (Pre and Post)

```json
"processor": {
  "pre": "rss",
  "post": "crossref"
}
```

- `pre`: Preprocessor name (runs during fetch)
- `post`: Postprocessor name (runs after save)

#### Array Format (Legacy)

```json
"processor": ["rss", "crossref"]
```

Equivalent to `{"pre": "rss", "post": "crossref"}`.

### Available Preprocessors

| Preprocessor | Description |
|--------------|-------------|
| `rss` | Canonical RSS/Atom feed parser (default) |
| `edworkingpapers` | EdWorkingPapers.org scraper |
| `sciencedirect` | ScienceDirect feed parser |

### Available Postprocessors

| Postprocessor | Description |
|---------------|-------------|
| `crossref` | Enrich articles with Crossref metadata (DOI, abstract, authors) |
| `sciencedirect` | Enrich ScienceDirect articles with additional metadata |
| `edworkingpapers` | Enrich EdWorkingPapers with DOI and metadata |

### Complete Example

```json
{
  "title": "Recent Publications in Education",
  "feeds": {
    "aerj": {
      "title": "American Educational Research Journal",
      "link": "https://journals.sagepub.com/home/aer",
      "feed": "https://journals.sagepub.com/action/showFeed?ui=0&mi=ehikzz&ai=2b4&jc=aera&type=etoc&feed=rss",
      "publication_id": "10.3102",
      "issn": "0002-8312",
      "processor": {"pre": "rss", "post": "crossref"}
    },
    "er": {
      "title": "Educational Researcher",
      "link": "https://journals.sagepub.com/home/edr",
      "feed": "https://journals.sagepub.com/action/showFeed?ui=0&mi=ehikzz&ai=2b4&jc=edra&type=etoc&feed=rss",
      "publication_id": "10.3102",
      "issn": "0013-189X",
      "processor": {"pre": "rss", "post": "crossref"}
    },
    "edworkingpapers": {
      "title": "EdWorkingPapers",
      "link": "https://edworkingpapers.com",
      "feed": "https://edworkingpapers.com/feed",
      "processor": {
        "pre": "edworkingpapers",
        "post": "edworkingpapers"
      }
    }
  }
}
```

## news.json Format

The `news.json` file configures news sites and headline sources.

### Basic Structure

```json
{
  "title": "Recent News in Education",
  "feeds": {
    "source-key": {
      "title": "Source Title",
      "link": "https://news-site.org",
      "feed": "https://news-site.org/feed.xml",
      "processor": {"pre": "rss"}
    }
  }
}
```

### Field Descriptions

| Field | Required | Description |
|-------|----------|-------------|
| `title` | Yes | Top-level title for the news collection |
| `feeds` | Yes | Object containing news source configurations |

### News Source Configuration

Each source in the `feeds` object has the following fields:

| Field | Required | Type | Description |
|-------|----------|------|-------------|
| `title` | Yes | string | Human-readable source title |
| `link` | Yes | string | URL to the news site homepage |
| `feed` | No | string | URL to RSS/Atom feed (if available) |
| `processor` | No | string/object | Processor configuration |

**Note:** Either `feed` or `processor` must be provided. If both are provided, the processor takes precedence.

### Available News Preprocessors

| Preprocessor | Description |
|--------------|-------------|
| `fcmat` | Scrapes FCMAT news headlines from HTML |
| `pd-education` | Parses Press Democrat education feed with AP filtering |
| `calmatters-education` | Parses CalMatters education feed |
| `rss` | Generic RSS/Atom parser |

### Complete Example

```json
{
  "title": "Recent News in Education",
  "feeds": {
    "fcmat": {
      "title": "FCMAT Headlines",
      "link": "https://www.fcmat.org/news-headlines",
      "feed": "",
      "processor": {"pre": "fcmat"}
    },
    "pd-education": {
      "title": "Press Democrat Education News",
      "feed": "https://www.pressdemocrat.com/news/education/feed/",
      "processor": {"pre": "pd-education"}
    },
    "calmatters-education": {
      "title": "CalMatters Education News",
      "feed": "https://calmatters.org/category/education/feed/",
      "processor": {"pre": "rss"}
    }
  }
}
```

## Processor Development

### Creating a Preprocessor

Preprocessors run during feed fetching and return normalized entries.

**Signature:**
```python
def my_preprocessor(session: requests.Session, url: str,
                   publication_id: str | None = None,
                   issn: str | None = None) -> list[dict]:
    """Fetch and parse feed/site.
    
    Returns:
        List of dicts with keys: guid, title, link, published, summary
    """
```

**Example:**
```python
# ednews/processors/myprocessor.py
import requests

def my_preprocessor(session, url, publication_id=None, issn=None):
    resp = session.get(url)
    resp.raise_for_status()
    # Parse response and return entries
    return [
        {
            "guid": "unique-id-1",
            "title": "Article Title",
            "link": "https://example.org/article",
            "published": "2024-11-01T12:00:00Z",
            "summary": "Article summary..."
        }
    ]
```

**Export:**
```python
# ednews/processors/__init__.py
from .myprocessor import my_preprocessor
__all__ = [..., 'my_preprocessor']
```

**Usage:**
```json
{
  "processor": {"pre": "my"}
}
```

### Creating a Postprocessor (DB-level)

DB-level postprocessors run after entries are saved and can enrich articles.

**Signature:**
```python
def my_postprocessor_db(conn: sqlite3.Connection, feed_key: str,
                       entries: list[dict],
                       session: requests.Session | None = None,
                       publication_id: str | None = None,
                       issn: str | None = None) -> int:
    """Enrich articles in the database.
    
    Returns:
        Number of articles enriched.
    """
```

**Example:**
```python
# ednews/processors/mypostprocessor.py
import sqlite3

def my_postprocessor_db(conn, feed_key, entries, session=None,
                       publication_id=None, issn=None):
    enriched = 0
    for entry in entries:
        # Fetch additional metadata
        # Update article in database
        enriched += 1
    return enriched
```

**Usage:**
```json
{
  "processor": {"pre": "rss", "post": "my"}
}
```

### Creating a Postprocessor (In-memory)

In-memory postprocessors modify entries before they're saved.

**Signature:**
```python
def my_postprocessor(entries: list[dict],
                    session: requests.Session | None = None,
                    publication_id: str | None = None,
                    issn: str | None = None) -> list[dict] | None:
    """Transform entries before saving.
    
    Returns:
        Modified entries list or None to use original.
    """
```

## Best Practices

### Feed Configuration

1. **Use descriptive keys**: Feed keys appear in URLs and logs
2. **Provide ISSNs**: Enables Crossref ISSN lookup
3. **Include publication IDs**: Helps with DOI matching
4. **Test feeds**: Verify feed URLs are accessible and valid
5. **Use processors**: Enable postprocessors for metadata enrichment

### Processor Selection

1. **Default to `rss`**: For standard RSS/Atom feeds
2. **Add `crossref` postprocessor**: For academic journals with DOIs
3. **Custom processors**: For sites without feeds or with special parsing needs
4. **Chain processors**: Use both pre and post processors when needed

### Performance

1. **Limit feeds**: Too many feeds increase fetch time
2. **Use timeouts**: Configure appropriate timeouts in `config.py`
3. **Monitor rate limits**: Respect API rate limits (especially Crossref)
4. **Cache responses**: Preprocessors should cache when appropriate

### Maintenance

1. **Check feed URLs**: Feeds can change or break
2. **Update ISSNs**: Keep publication metadata current
3. **Remove dead feeds**: Clean up inactive sources
4. **Test after changes**: Run `fetch` and `build` to verify

## Troubleshooting

### Feed Not Fetching

**Problem:** Feed doesn't appear in database after fetch.

**Solutions:**
- Verify feed URL is accessible
- Check processor name is correct
- Look for errors in logs (`-v` flag)
- Ensure feed returns valid RSS/Atom

### Processor Not Found

**Problem:** Error: "Unknown processor: xyz"

**Solutions:**
- Check processor is exported in `ednews/processors/__init__.py`
- Verify processor name matches exactly (case-sensitive)
- Ensure processor function exists

### Crossref Enrichment Failing

**Problem:** Articles not getting DOIs or abstracts.

**Solutions:**
- Verify `publication_id` matches journal DOI prefix
- Check Crossref rate limits aren't exceeded
- Ensure article titles are accurate
- Try increasing timeout in `config.py`

### Duplicate Articles

**Problem:** Same article appears multiple times.

**Solutions:**
- Check URL normalization is working
- Verify GUIDs are unique
- Ensure `url_hash` migration has run
- Check for duplicate feed configurations

## Advanced Configuration

### Environment-Specific Configs

Use different config files for different environments:

```bash
# Development
export RESEARCH_JSON=research.dev.json

# Production
export RESEARCH_JSON=research.prod.json
```

### Custom Timeouts

Override timeout settings in `ednews/config.py`:

```python
CROSSREF_TIMEOUT = 60  # Increase for slow connections
CROSSREF_RETRIES = 5   # More retries for flaky networks
```

### Filtering Content

Add title filters to `ednews/config.py`:

```python
TITLE_FILTERS = [
    "editorial board",
    "front matter",
    "table of contents",
]
```

Articles matching these titles (case-insensitive) will be skipped.

## Configuration Validation

To validate your configuration:

```bash
# Test feed fetching
uv run python main.py fetch -v

# Check database
sqlite3 ednews.db "SELECT COUNT(*) FROM items;"

# Verify build
uv run python main.py build --out-dir test-build
```

## Migration from Old Formats

If migrating from `planet.ini` or other formats:

1. Convert feeds to JSON format
2. Add `processor` field to each feed
3. Run `db-init` to create new schema
4. Run `fetch` to populate database
5. Verify with `build`

See `ARCHITECTURE.md` for more on the processor system.
