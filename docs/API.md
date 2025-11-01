# API Reference

This document describes the public Python API for the `ednews` package.

## Package Structure

```python
import ednews
from ednews import config, db, feeds, build, embeddings, crossref, processors
```

## `ednews.config`

Configuration constants and paths.

### Constants

```python
ROOT: Path                          # Project root directory
RESEARCH_JSON: Path                 # Path to research.json
DB_PATH: Path                       # Path to ednews.db
USER_AGENT: str                     # HTTP User-Agent header
DEFAULT_MODEL: str                  # Default embedding model
CROSSREF_TIMEOUT: int               # Crossref API timeout (seconds)
CROSSREF_CONNECT_TIMEOUT: int       # Crossref connect timeout (seconds)
CROSSREF_RETRIES: int               # Number of retries for Crossref
CROSSREF_BACKOFF: float             # Backoff factor for retries
CROSSREF_STATUS_FORCELIST: list     # HTTP status codes to retry
ARTICLES_DEFAULT_LIMIT: int         # Default article limit for build
HEADLINES_DEFAULT_LIMIT: int        # Default headline limit for build
ARTICLES_MAX_SAME_DATE_EXTRA: int   # Max extra articles for same date
DEFAULT_MISSING_DATE: str           # Default date for items without date
FEED_SITE_LINK: str                 # RSS feed site link
FEED_TITLE_COMBINED: str            # Combined feed title
FEED_TITLE_ARTICLES: str            # Articles-only feed title
FEED_TITLE_HEADLINES: str           # Headlines-only feed title
TITLE_FILTERS: list                 # Titles to exclude from processing
```

## `ednews.db`

Database operations and management.

### Connection Management

```python
def get_connection(path: str | None = None) -> sqlite3.Connection:
    """Return a SQLite connection.
    
    Args:
        path: Optional database file path. If None, returns in-memory connection.
    
    Returns:
        SQLite connection object.
    """
```

### Schema Initialization

```python
def init_db(conn: sqlite3.Connection) -> None:
    """Initialize database schema and create tables/views.
    
    Args:
        conn: SQLite connection object.
    """
```

```python
def create_combined_view(conn: sqlite3.Connection) -> None:
    """Create combined_articles view.
    
    Args:
        conn: SQLite connection object.
    """
```

### Article Operations

```python
def upsert_article(conn: sqlite3.Connection, article: dict) -> bool:
    """Insert or update an article in the database.
    
    Args:
        conn: SQLite connection object.
        article: Dict with keys: doi, title, authors, abstract, feed_id,
                publication_id, issn, published, fetched_at, crossref_xml
    
    Returns:
        True if inserted/updated successfully.
    """
```

```python
def article_exists(conn: sqlite3.Connection, doi: str) -> bool:
    """Check if an article with the given DOI exists.
    
    Args:
        conn: SQLite connection object.
        doi: Digital Object Identifier to check.
    
    Returns:
        True if article exists in database.
    """
```

```python
def get_article_by_title(conn: sqlite3.Connection, title: str) -> dict | None:
    """Fetch article by title.
    
    Args:
        conn: SQLite connection object.
        title: Article title to search for.
    
    Returns:
        Article dict or None if not found.
    """
```

### Item Operations

```python
def save_items(conn: sqlite3.Connection, feed_id: str, items: list[dict]) -> int:
    """Save feed items to database.
    
    Args:
        conn: SQLite connection object.
        feed_id: Feed identifier.
        items: List of item dicts with keys: guid, title, link, published, summary
    
    Returns:
        Number of items saved.
    """
```

### Headline Operations

```python
def save_news_items(conn: sqlite3.Connection, source: str, items: list[dict]) -> int:
    """Save news headlines to database.
    
    Args:
        conn: SQLite connection object.
        source: Source identifier.
        items: List of headline dicts with keys: title, text, link, published
    
    Returns:
        Number of headlines saved.
    """
```

### Maintenance

```python
def sync_publications_from_feeds(conn: sqlite3.Connection, feeds_list: list) -> int:
    """Synchronize publications table with feed configurations.
    
    Args:
        conn: SQLite connection object.
        feeds_list: List of feed tuples from load_feeds()
    
    Returns:
        Number of publications synchronized.
    """
```

```python
def vacuum_db(conn: sqlite3.Connection) -> None:
    """Vacuum database to reclaim space.
    
    Args:
        conn: SQLite connection object.
    """
```

```python
def cleanup_empty_articles(conn: sqlite3.Connection, older_than_days: int = 90) -> int:
    """Remove articles with no title or abstract older than N days.
    
    Args:
        conn: SQLite connection object.
        older_than_days: Age threshold in days.
    
    Returns:
        Number of articles deleted.
    """
```

```python
def migrate_db(conn: sqlite3.Connection) -> None:
    """Run all available database migrations.
    
    Args:
        conn: SQLite connection object.
    """
```

## `ednews.feeds`

Feed loading, fetching, and normalization.

### Feed Loading

```python
def load_feeds() -> list[tuple]:
    """Load feeds from research.json.
    
    Returns:
        List of tuples: (key, title, url, publication_id, issn, processor)
    """
```

### Feed Fetching

```python
def fetch_feed(session: requests.Session, key: str, feed_title: str, url: str,
               publication_doi: str | None = None, issn: str | None = None,
               timeout: int = 20) -> dict:
    """Fetch and parse a single feed URL.
    
    Args:
        session: Requests session for connection pooling.
        key: Feed identifier.
        feed_title: Human-readable feed title.
        url: Feed URL.
        publication_doi: Optional publication DOI prefix.
        issn: Optional ISSN.
        timeout: HTTP timeout in seconds.
    
    Returns:
        Dict with keys: key, title, url, entries (list), error (if failed)
    """
```

### Content Checking

```python
def entry_has_content(entry: dict) -> bool:
    """Check if a feedparser entry has usable content.
    
    Args:
        entry: Feed entry dict.
    
    Returns:
        True if entry has title, link, summary, or content.
    """
```

### Metadata Extraction

```python
def extract_doi_from_entry(entry: dict) -> str | None:
    """Extract DOI from feed entry.
    
    Args:
        entry: Feed entry dict.
    
    Returns:
        DOI string or None.
    """
```

```python
def extract_abstract_from_entry(entry: dict) -> str | None:
    """Extract abstract from feed entry.
    
    Args:
        entry: Feed entry dict.
    
    Returns:
        Abstract text or None.
    """
```

```python
def extract_authors_from_entry(entry: dict) -> str | None:
    """Extract author list from feed entry.
    
    Args:
        entry: Feed entry dict.
    
    Returns:
        Comma-separated author names or None.
    """
```

## `ednews.crossref`

Crossref API integration.

### DOI Lookup

```python
def query_crossref_doi_by_title(title: str, 
                                preferred_publication_id: str | None = None,
                                timeout: int = 8) -> str | None:
    """Lookup DOI by article title via Crossref API.
    
    Args:
        title: Article title.
        preferred_publication_id: Optional DOI prefix to prefer.
        timeout: HTTP timeout in seconds.
    
    Returns:
        DOI string or None if not found.
    
    Note:
        Results are cached via @lru_cache.
    """
```

### Metadata Fetching

```python
def fetch_crossref_metadata(doi: str, timeout: int = 10,
                           conn: sqlite3.Connection | None = None) -> dict | None:
    """Fetch Crossref metadata for a DOI.
    
    Args:
        doi: Digital Object Identifier.
        timeout: HTTP timeout in seconds.
        conn: Optional SQLite connection (reserved for future use).
    
    Returns:
        Dict with keys: authors, abstract, raw, published, or None if failed.
    """
```

### Date Normalization

```python
def normalize_crossref_date(date_parts: list | None) -> str:
    """Convert Crossref date-parts array to ISO date string.
    
    Args:
        date_parts: Array like [[2024, 11, 1]] from Crossref JSON.
    
    Returns:
        ISO date string (YYYY-MM-DD) or empty string.
    """
```

## `ednews.embeddings`

Vector embeddings for similarity search.

### Table Creation

```python
def create_articles_vec(conn: sqlite3.Connection, dim: int = 768) -> None:
    """Create articles_vec virtual table.
    
    Args:
        conn: SQLite connection object.
        dim: Embedding dimension (default: 768).
    """
```

```python
def create_headlines_vec(conn: sqlite3.Connection, dim: int = 768) -> None:
    """Create headlines_vec virtual table.
    
    Args:
        conn: SQLite connection object.
        dim: Embedding dimension (default: 768).
    """
```

### Embedding Generation

```python
def upsert_embeddings(conn: sqlite3.Connection, table_name: str,
                     items: list[tuple[int, str]], model: str | None = None,
                     batch_size: int = 64) -> int:
    """Generate embeddings and upsert into vec table.
    
    Args:
        conn: SQLite connection object.
        table_name: Name of vec virtual table.
        items: List of (id, text) tuples.
        model: Model name (default: nomic-embed-text-v1.5).
        batch_size: Batch size for embedding generation.
    
    Returns:
        Number of embeddings written.
    """
```

### Similarity Search

```python
def query_similar(conn: sqlite3.Connection, table_name: str,
                 query_text: str, limit: int = 5,
                 model: str | None = None) -> list[tuple[int, float]]:
    """Find similar items using cosine similarity.
    
    Args:
        conn: SQLite connection object.
        table_name: Name of vec virtual table.
        query_text: Text to find similar items for.
        limit: Number of results to return.
        model: Model name (default: nomic-embed-text-v1.5).
    
    Returns:
        List of (rowid, distance) tuples, ordered by similarity.
    """
```

## `ednews.build`

Static site generation.

### Main Build Function

```python
def build_site(out_dir: str | Path = "build",
              articles_limit: int = 20,
              headlines_limit: int = 20,
              conn: sqlite3.Connection | None = None) -> None:
    """Build static site from database.
    
    Args:
        out_dir: Output directory for built site.
        articles_limit: Number of articles to include.
        headlines_limit: Number of headlines to include.
        conn: SQLite connection (uses DB_PATH if None).
    """
```

### Content Checking

```python
def item_has_content(item: dict) -> bool:
    """Check if item has usable content.
    
    Args:
        item: Item dict with title/link/content keys.
    
    Returns:
        True if item has any content.
    """
```

### RSS Generation

```python
def generate_rss_feed(items: list[dict], feed_title: str,
                     feed_link: str, template_name: str) -> str:
    """Generate RSS feed XML from items.
    
    Args:
        items: List of article/headline dicts.
        feed_title: Feed title.
        feed_link: Feed site link.
        template_name: Jinja2 template filename.
    
    Returns:
        RSS XML as string.
    """
```

## `ednews.news`

News headline aggregation.

### Configuration

```python
def load_config(path: Path | str | None = None) -> dict:
    """Load news configuration from JSON.
    
    Args:
        path: Optional path to news.json (default: news.json in cwd).
    
    Returns:
        Parsed configuration dict.
    """
```

### Fetching

```python
def fetch_site(session: requests.Session, site_cfg: dict) -> list[dict]:
    """Fetch headlines from a single news site.
    
    Args:
        session: Requests session.
        site_cfg: Site configuration dict with keys: title, link, feed, processor.
    
    Returns:
        List of headline dicts with keys: title, link, summary, published.
    """
```

```python
def fetch_all(session: requests.Session | None = None,
             cfg_path: str | Path | None = None,
             conn: sqlite3.Connection | None = None) -> dict[str, list[dict]]:
    """Fetch all configured news sites.
    
    Args:
        session: Optional requests session.
        cfg_path: Optional path to news.json.
        conn: Optional SQLite connection for persisting headlines.
    
    Returns:
        Dict mapping source key to list of headline dicts.
    """
```

## `ednews.processors`

Feed and site processors.

### RSS Preprocessor

```python
def rss_preprocessor(session: requests.Session, url: str,
                    publication_id: str | None = None,
                    issn: str | None = None) -> list[dict]:
    """Canonical RSS/Atom preprocessor.
    
    Args:
        session: Requests session.
        url: Feed URL.
        publication_id: Optional publication DOI prefix.
        issn: Optional ISSN.
    
    Returns:
        List of entry dicts.
    """
```

### Crossref Postprocessor

```python
def crossref_postprocessor_db(conn: sqlite3.Connection, feed_key: str,
                              entries: list[dict],
                              session: requests.Session | None = None,
                              publication_id: str | None = None,
                              issn: str | None = None) -> int:
    """Enrich articles with Crossref metadata (DB-level postprocessor).
    
    Args:
        conn: SQLite connection object.
        feed_key: Feed identifier.
        entries: List of entry dicts.
        session: Optional requests session.
        publication_id: Optional publication DOI prefix.
        issn: Optional ISSN.
    
    Returns:
        Number of articles enriched.
    """
```

## Usage Examples

### Fetching and Saving Feeds

```python
import sqlite3
import requests
from ednews import config, feeds, db

# Load feed configurations
feeds_list = feeds.load_feeds()

# Connect to database
conn = sqlite3.connect(str(config.DB_PATH))
db.init_db(conn)

# Fetch a feed
session = requests.Session()
feed_data = feeds.fetch_feed(
    session=session,
    key="aerj",
    feed_title="AERJ",
    url="https://journals.sagepub.com/...",
    publication_doi="10.3102",
    issn="0002-8312"
)

# Save items
db.save_items(conn, "aerj", feed_data["entries"])
conn.close()
```

### Generating Embeddings

```python
import sqlite3
from ednews import config, embeddings, db

conn = sqlite3.connect(str(config.DB_PATH))

# Create vec table
embeddings.create_articles_vec(conn)

# Fetch articles
cur = conn.cursor()
cur.execute("SELECT id, title || ' ' || COALESCE(abstract, '') FROM articles")
items = cur.fetchall()

# Generate embeddings
count = embeddings.upsert_embeddings(
    conn=conn,
    table_name="articles_vec",
    items=items,
    batch_size=32
)
print(f"Generated {count} embeddings")
conn.close()
```

### Building the Site

```python
from ednews import build

build.build_site(
    out_dir="build",
    articles_limit=30,
    headlines_limit=20
)
```

### Custom Preprocessor

```python
import requests

def my_preprocessor(session: requests.Session, url: str,
                   publication_id: str | None = None,
                   issn: str | None = None) -> list[dict]:
    """Custom feed preprocessor."""
    resp = session.get(url)
    # Parse and return entries
    return [
        {
            "guid": "...",
            "title": "...",
            "link": "...",
            "published": "...",
            "summary": "..."
        }
    ]

# Export from ednews/processors/__init__.py
# Use in config: "processor": {"pre": "my"}
```

## CLI vs. Python API

Most functionality is available via both CLI and Python API:

| CLI Command | Python API |
|-------------|------------|
| `fetch` | `feeds.load_feeds()`, `feeds.fetch_feed()`, `db.save_items()` |
| `build` | `build.build_site()` |
| `embed` | `embeddings.upsert_embeddings()` |
| `db-init` | `db.init_db()` |
| `manage-db vacuum` | `db.vacuum_db()` |
| `headlines` | `news.fetch_all()` |

The CLI (`ednews.cli`) is a thin wrapper around the Python API.
