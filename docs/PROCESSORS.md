# Preprocessors and Postprocessors

## Overview

ed-news uses a flexible processor architecture that separates feed fetching from metadata enrichment:

- **Preprocessors**: Fetch or parse raw feeds/pages and return a list of entry dicts.
- **Postprocessors**: Run after entries are saved, used to enrich articles or attach DOIs.

## Processor Types

### Preprocessors

Preprocessors run during the fetch phase and transform raw data into normalized entries.

**Function Signature:**
```python
def name_preprocessor(session: requests.Session, url: str,
                     publication_id: str | None = None,
                     issn: str | None = None) -> list[dict]:
    """Fetch and parse feed/site.
    
    Returns:
        List of entry dicts with keys: guid, title, link, published, summary
    """
```

### Postprocessors (DB-level)

DB-level postprocessors run after entries are saved and can enrich database records.

**Function Signature:**
```python
def name_postprocessor_db(conn: sqlite3.Connection, feed_key: str,
                         entries: list[dict],
                         session: requests.Session | None = None,
                         publication_id: str | None = None,
                         issn: str | None = None) -> int:
    """Enrich articles in database.
    
    Returns:
        Number of articles enriched.
    """
```

### Postprocessors (In-memory)

In-memory postprocessors modify entries before they're saved to the database.

**Function Signature:**
```python
def name_postprocessor(entries: list[dict],
                      session: requests.Session | None = None,
                      publication_id: str | None = None,
                      issn: str | None = None) -> list[dict] | None:
    """Transform entries before saving.
    
    Returns:
        Modified entries list or None to use original.
    """
```

## Naming Conventions

- `<name>_preprocessor` — Preprocessor function
- `<name>_postprocessor_db` — DB-level postprocessor
- `<name>_postprocessor` — In-memory postprocessor

## Available Processors

### Preprocessors

| Name | Module | Description |
|------|--------|-------------|
| `rss` | `ednews.processors.rss` | Canonical RSS/Atom feed parser |
| `edworkingpapers` | `ednews.processors.edworkingpapers` | EdWorkingPapers.org scraper |
| `sciencedirect` | `ednews.processors.sciencedirect` | ScienceDirect feed parser |
| `fcmat` | `ednews.processors.fcmat` | FCMAT news headline scraper |
| `pd-education` | `ednews.processors.pressdemocrat` | Press Democrat education feed (with AP filtering) |

### Postprocessors

| Name | Module | Type | Description |
|------|--------|------|-------------|
| `crossref` | `ednews.processors.crossref` | DB-level | Enrich articles with Crossref metadata |
| `sciencedirect` | `ednews.processors.sciencedirect` | DB-level | Enrich ScienceDirect articles |
| `edworkingpapers` | `ednews.processors.edworkingpapers` | DB-level | Enrich EdWorkingPapers with metadata |

## Compatibility

Existing `*_feed_processor` functions are treated as preprocessors for compatibility. The CLI will try `<name>_preprocessor` then fall back to `<name>_feed_processor`.

## RSS Preprocessor (Canonical)

The project provides a canonical RSS/Atom preprocessor named `rss_preprocessor` available from `ednews.processors`. It is a thin wrapper around `ednews.feeds.fetch_feed` and returns the `entries` list.

### Usage in Configuration

```json
{
  "feeds": {
    "example": {
      "title": "Example Feed",
      "feed": "https://example.org/feed.xml",
      "processor": "rss"
    }
  }
}
```

Or with both pre and post processors:

```json
{
  "feeds": {
    "example": {
      "title": "Example Feed",
      "feed": "https://example.org/feed.xml",
      "publication_id": "10.1234",
      "issn": "1234-5678",
      "processor": {
        "pre": "rss",
        "post": "crossref"
      }
    }
  }
}
```

### CLI Behavior

When a feed's `processor` is set to `rss`, the CLI calls `ednews.processors.rss_preprocessor(session, url, publication_id, issn)` during the fetch preprocessor phase. If a feed has no configured preprocessor, the CLI falls back to `rss_preprocessor` as the canonical default.

## Configuration Examples

### Basic RSS Feed

```json
{
  "aerj": {
    "title": "American Educational Research Journal",
    "link": "https://journals.sagepub.com/home/aer",
    "feed": "https://journals.sagepub.com/action/showFeed?type=etoc&feed=rss",
    "processor": "rss"
  }
}
```

### RSS Feed with Crossref Enrichment

```json
{
  "aerj": {
    "title": "American Educational Research Journal",
    "link": "https://journals.sagepub.com/home/aer",
    "feed": "https://journals.sagepub.com/action/showFeed?type=etoc&feed=rss",
    "publication_id": "10.3102",
    "issn": "0002-8312",
    "processor": {
      "pre": "rss",
      "post": "crossref"
    }
  }
}
```

### Custom HTML Scraper

```json
{
  "fcmat": {
    "title": "FCMAT Headlines",
    "link": "https://www.fcmat.org/news-headlines",
    "feed": "",
    "processor": {
      "pre": "fcmat"
    }
  }
}
```

### EdWorkingPapers (Custom Pre and Post)

```json
{
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
```

## Creating Custom Processors

### Example Preprocessor

```python
# ednews/processors/myprocessor.py
import requests
from bs4 import BeautifulSoup

def my_preprocessor(session, url, publication_id=None, issn=None):
    """Custom preprocessor for MySource."""
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    
    soup = BeautifulSoup(resp.content, 'html.parser')
    entries = []
    
    for article in soup.select('.article'):
        entries.append({
            'guid': article.get('data-id'),
            'title': article.select_one('.title').text,
            'link': article.select_one('a')['href'],
            'published': article.get('data-date', ''),
            'summary': article.select_one('.summary').text
        })
    
    return entries
```

Export from `ednews/processors/__init__.py`:

```python
from .myprocessor import my_preprocessor
__all__ = [..., 'my_preprocessor']
```

### Example Postprocessor (DB-level)

```python
# ednews/processors/mypostprocessor.py
import sqlite3
from ednews.db import upsert_article
from ednews import crossref

def my_postprocessor_db(conn, feed_key, entries, session=None,
                       publication_id=None, issn=None):
    """Enrich articles with custom metadata."""
    enriched = 0
    
    for entry in entries:
        title = entry.get('title')
        if not title:
            continue
        
        # Fetch metadata from external source
        doi = crossref.query_crossref_doi_by_title(title, publication_id)
        if doi:
            metadata = crossref.fetch_crossref_metadata(doi)
            if metadata:
                article = {
                    'doi': doi,
                    'title': title,
                    'authors': metadata.get('authors'),
                    'abstract': metadata.get('abstract'),
                    'published': metadata.get('published'),
                    'feed_id': feed_key,
                    'publication_id': publication_id,
                    'issn': issn
                }
                upsert_article(conn, article)
                enriched += 1
    
    return enriched
```

## Migration Guide

### From Legacy `*_feed_processor`

1. Rename function to `<name>_preprocessor`:

```python
# Old
def mysite_feed_processor(session, url):
    ...

# New
def mysite_preprocessor(session, url, publication_id=None, issn=None):
    ...
```

2. Update exports in `__init__.py`:

```python
from .mysite import mysite_preprocessor
__all__ = [..., 'mysite_preprocessor']
```

3. Update configuration:

```json
{
  "processor": {"pre": "mysite"}
}
```

### Adding DB-level Enrichment

1. Implement `<name>_postprocessor_db`:

```python
def mysite_postprocessor_db(conn, feed_key, entries, session=None,
                            publication_id=None, issn=None):
    # Enrich articles in database
    return enriched_count
```

2. Configure both pre and post:

```json
{
  "processor": {
    "pre": "mysite",
    "post": "mysite"
  }
}
```

## Testing Processors

### Monkeypatching Example

Because processors are exported from `ednews.processors`, tests can monkeypatch them easily to avoid network calls.

**Example pytest snippet:**

```python
import ednews.processors as proc_mod

def test_fetch_with_mock_processor(db_conn, monkeypatch):
    def fake_rss_pre(session, url, publication_id=None, issn=None):
        return [
            {
                "title": "Test Article",
                "link": "http://example.org/test",
                "summary": "Test summary",
                "guid": "test-guid",
                "published": "2024-11-01"
            }
        ]
    
    monkeypatch.setattr(proc_mod, "rss_preprocessor", fake_rss_pre, raising=False)
    
    # Test fetch logic
    from ednews import feeds
    feed_data = feeds.fetch_feed(
        session=requests.Session(),
        key="test",
        feed_title="Test Feed",
        url="http://example.org/feed"
    )
    
    assert len(feed_data["entries"]) == 1
    assert feed_data["entries"][0]["title"] == "Test Article"
```

This pattern mirrors other processor monkeypatch patterns used across the test suite.

### Unit Testing Preprocessors

```python
# tests/test_processors_my.py
from ednews.processors import my_preprocessor
import requests

def test_my_preprocessor(monkeypatch):
    # Mock HTTP response
    class MockResponse:
        content = b'<html>...</html>'
        def raise_for_status(self):
            pass
    
    def mock_get(url, **kwargs):
        return MockResponse()
    
    session = requests.Session()
    monkeypatch.setattr(session, 'get', mock_get)
    
    entries = my_preprocessor(session, "http://example.org")
    assert len(entries) > 0
    assert 'title' in entries[0]
```

### Integration Testing Postprocessors

```python
# tests/test_processors_my_postprocessor.py
import sqlite3
from ednews.db import init_db
from ednews.processors import my_postprocessor_db

def test_my_postprocessor_db():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    
    entries = [
        {
            "title": "Test Article",
            "link": "http://example.org/test"
        }
    ]
    
    count = my_postprocessor_db(
        conn=conn,
        feed_key="test",
        entries=entries,
        publication_id="10.1234"
    )
    
    assert count > 0
    
    # Verify article was saved
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM articles")
    assert cur.fetchone()[0] > 0
```

## Best Practices

1. **Preprocessors should be idempotent**: Multiple calls with same input should produce same output
2. **Handle errors gracefully**: Return empty list on failure rather than raising
3. **Use session for HTTP**: Accept and use the `session` parameter for connection pooling
4. **Normalize data**: Return consistent dict structure with standard keys
5. **Log appropriately**: Use `logging.getLogger(__name__)` for debug/info messages
6. **Test with mocks**: Use monkeypatching to avoid network calls in tests
7. **Document signatures**: Include docstrings with parameter and return descriptions
8. **Export from `__init__.py`**: Make processors discoverable by the CLI

## Troubleshooting

### Processor Not Found

**Problem:** Error message "Unknown processor: xyz"

**Solutions:**
- Verify processor is exported in `ednews/processors/__init__.py`
- Check processor name matches exactly (case-sensitive)
- Ensure module is importable (no syntax errors)

### Processor Returns No Entries

**Problem:** Feed fetch completes but no entries are saved.

**Solutions:**
- Check preprocessor is returning list of dicts
- Verify each dict has required keys: `guid`, `title`, `link`
- Check for exceptions in preprocessor (use `-v` flag)
- Ensure URL is accessible and returns expected content

### Postprocessor Not Running

**Problem:** Postprocessor doesn't enrich articles.

**Solutions:**
- Verify postprocessor is configured in feed config: `"post": "name"`
- Check postprocessor signature matches expected format
- Ensure entries are being saved before postprocessor runs
- Look for exceptions in postprocessor (use `-v` flag)

## See Also

- [Configuration Guide](CONFIGURATION.md) - Feed configuration format
- [API Reference](API.md) - Python API documentation
- [Development Guide](DEVELOPMENT.md) - Contributing processors
