# Development Guide

This guide will help you set up a development environment and contribute to ed-news.

## Prerequisites

- **Python 3.13+** (required by pyproject.toml)
- **uv** (recommended) or pip
- **Git** for version control
- **SQLite 3** with extension support (for embeddings)

## Initial Setup

### 1. Clone the Repository

```bash
git clone https://github.com/ebardelli/ed-news.git
cd ed-news
```

### 2. Install Dependencies

#### Using uv (Recommended)

```bash
# Install uv if not already installed
curl -LsSf https://astral.sh/uv/install.sh | sh

# Sync dependencies
uv sync
```

#### Using pip

```bash
# Create virtual environment
python3.13 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install in editable mode
pip install -e .
```

### 3. Initialize the Database

```bash
uv run python main.py db-init
```

### 4. Fetch Sample Data

```bash
# Fetch a few feeds to test
uv run python main.py fetch -v
```

### 5. Build the Site

```bash
uv run python main.py build --out-dir build
```

### 6. Run Tests

```bash
uv run pytest -q
```

## Project Structure

```
ed-news/
├── main.py                 # CLI entrypoint
├── ednews/                 # Main package
│   ├── cli.py             # CLI implementation
│   ├── feeds.py           # Feed fetching
│   ├── news.py            # News aggregation
│   ├── build.py           # Site generation
│   ├── embeddings.py      # Vector embeddings
│   ├── crossref.py        # Crossref integration
│   ├── config.py          # Configuration
│   ├── http.py            # HTTP utilities
│   ├── db/                # Database layer
│   │   ├── __init__.py    # DB API
│   │   ├── schema.py      # Schema definition
│   │   ├── maintenance.py # Maintenance operations
│   │   ├── migrations.py  # Schema migrations
│   │   └── utils.py       # DB utilities
│   ├── processors/        # Feed processors
│   │   ├── __init__.py    # Exports
│   │   ├── rss.py         # RSS preprocessor
│   │   ├── crossref.py    # Crossref postprocessor
│   │   ├── sciencedirect.py
│   │   ├── edworkingpapers.py
│   │   ├── fcmat.py
│   │   └── pressdemocrat.py
│   ├── templates/         # Jinja2 templates
│   └── static/            # CSS/JS assets
├── tests/                 # Test suite
│   ├── conftest.py        # Pytest configuration
│   ├── fixtures/          # Test data
│   └── test_*.py          # Test modules
├── docs/                  # Documentation
├── research.json          # Article feed config
├── news.json              # News feed config
├── pyproject.toml         # Project metadata
└── README.md              # Quick reference
```

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/my-feature
```

### 2. Make Changes

Edit files in `ednews/` or add new modules as needed.

### 3. Write Tests

Add tests in `tests/test_*.py`:

```python
# tests/test_my_feature.py
import pytest
from ednews import my_module

def test_my_function():
    result = my_module.my_function("input")
    assert result == "expected"
```

### 4. Run Tests

```bash
# Run all tests
uv run pytest

# Run specific test file
uv run pytest tests/test_my_feature.py

# Run with verbose output
uv run pytest -v

# Run with coverage
uv run pytest --cov=ednews
```

### 5. Test Locally

```bash
# Test fetch
uv run python main.py fetch -v

# Test build
uv run python main.py build --out-dir test-build

# Serve locally
uv run python main.py serve --directory test-build
```

### 6. Commit Changes

```bash
git add .
git commit -m "Add my feature"
git push origin feature/my-feature
```

### 7. Create Pull Request

Open a PR on GitHub for review.

## Testing

### Running Tests

```bash
# All tests
uv run pytest

# Specific test
uv run pytest tests/test_feeds.py::test_load_feeds

# With output
uv run pytest -v -s

# Stop on first failure
uv run pytest -x
```

### Test Structure

Tests use pytest with fixtures defined in `conftest.py`:

```python
# tests/conftest.py
import pytest
import sqlite3

@pytest.fixture
def db_conn():
    """Provide in-memory database for tests."""
    conn = sqlite3.connect(":memory:")
    from ednews.db import init_db
    init_db(conn)
    yield conn
    conn.close()
```

### Writing Tests

**Unit Test Example:**

```python
# tests/test_feeds.py
from ednews import feeds

def test_entry_has_content():
    entry = {"title": "Test", "link": "https://example.org"}
    assert feeds.entry_has_content(entry)
    
    empty_entry = {}
    assert not feeds.entry_has_content(empty_entry)
```

**Integration Test Example:**

```python
# tests/test_db.py
def test_upsert_article(db_conn):
    from ednews.db import upsert_article, article_exists
    
    article = {
        "doi": "10.1234/test",
        "title": "Test Article",
        "authors": "Author Name",
        "abstract": "Abstract text",
    }
    
    upsert_article(db_conn, article)
    assert article_exists(db_conn, "10.1234/test")
```

**Monkeypatching Example:**

```python
# tests/test_cli_fetch.py
import ednews.processors as proc_mod

def test_fetch_with_mock_processor(db_conn, monkeypatch):
    def fake_rss(session, url, publication_id=None, issn=None):
        return [{"title": "Test", "link": "https://example.org"}]
    
    monkeypatch.setattr(proc_mod, "rss_preprocessor", fake_rss)
    # Test fetch logic
```

### Test Coverage

Aim for high coverage of new code:

```bash
uv run pytest --cov=ednews --cov-report=html
open htmlcov/index.html  # View coverage report
```

## Adding Features

### Adding a Preprocessor

1. Create processor file:

```python
# ednews/processors/myprocessor.py
import requests

def my_preprocessor(session, url, publication_id=None, issn=None):
    """Fetch and parse my custom feed."""
    resp = session.get(url, timeout=20)
    resp.raise_for_status()
    
    # Parse response
    entries = []
    # ... parsing logic ...
    
    return entries
```

2. Export from `__init__.py`:

```python
# ednews/processors/__init__.py
from .myprocessor import my_preprocessor
__all__ = [..., 'my_preprocessor']
```

3. Add tests:

```python
# tests/test_processors_my.py
from ednews.processors import my_preprocessor

def test_my_preprocessor(monkeypatch):
    # Mock HTTP requests
    # Test parsing logic
    pass
```

4. Use in config:

```json
{
  "processor": {"pre": "my"}
}
```

### Adding a CLI Command

1. Add handler function:

```python
# ednews/cli.py
def cmd_mycommand(args):
    """Handle my-command CLI invocation."""
    logger.info("Running my command with args: %s", args)
    # Implementation
```

2. Register in argument parser:

```python
# ednews/cli.py, in run() function
subparsers = parser.add_subparsers()

my_parser = subparsers.add_parser(
    "my-command",
    help="Description of my command"
)
my_parser.add_argument("--option", help="Option description")
my_parser.set_defaults(func=cmd_mycommand)
```

3. Add tests:

```python
# tests/test_cli_my.py
import sys
from ednews.cli import run

def test_my_command(monkeypatch, capsys):
    monkeypatch.setattr(sys, 'argv', ['ednews', 'my-command', '--option', 'value'])
    run()
    captured = capsys.readouterr()
    assert "expected output" in captured.out
```

### Adding a Database Migration

1. Create migration function:

```python
# ednews/db/migrations.py
def migrate_add_my_column(conn):
    """Add my_column to items table."""
    import logging
    logger = logging.getLogger("ednews.db.migrations")
    
    cur = conn.cursor()
    try:
        cur.execute("ALTER TABLE items ADD COLUMN my_column TEXT")
        conn.commit()
        logger.info("Added my_column to items table")
    except Exception as e:
        logger.debug("my_column already exists or error: %s", e)
```

2. Call from `migrate_db`:

```python
# ednews/db/migrations.py
def migrate_db(conn):
    """Run all migrations."""
    migrate_add_items_url_hash(conn)
    migrate_add_my_column(conn)  # Add your migration
```

3. Test migration:

```python
# tests/test_migrate_my_column.py
def test_migrate_my_column():
    import sqlite3
    from ednews.db.migrations import migrate_add_my_column
    
    conn = sqlite3.connect(":memory:")
    # Create old schema
    conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
    
    # Run migration
    migrate_add_my_column(conn)
    
    # Verify column exists
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(items)")
    columns = [row[1] for row in cur.fetchall()]
    assert "my_column" in columns
```

## Code Style

### Python Conventions

- Follow PEP 8 style guide
- Use Google-style docstrings
- Use type hints where helpful
- Keep functions focused and small
- Prefer explicit over implicit

### Docstring Example

```python
def fetch_feed(session, key, feed_title, url, publication_doi=None, 
               issn=None, timeout=20):
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
        Dict with keys: key, title, url, entries (list), error (if failed).
    
    Raises:
        requests.RequestException: If HTTP request fails.
    """
```

### Import Order

1. Standard library
2. Third-party packages
3. Local modules

```python
import logging
from pathlib import Path

import requests
import feedparser

from ednews import config, db
```

## Debugging

### Enable Debug Logging

```bash
uv run python main.py fetch -v  # -v for verbose/debug logs
```

### Use Python Debugger

```python
# Add breakpoint in code
import pdb; pdb.set_trace()

# Or use modern breakpoint()
breakpoint()
```

### Inspect Database

```bash
sqlite3 ednews.db

# List tables
.tables

# Query items
SELECT * FROM items LIMIT 10;

# Check schema
.schema items
```

### Test Specific Processor

```python
# Create test script
import requests
from ednews.processors import my_preprocessor

session = requests.Session()
entries = my_preprocessor(session, "https://example.org/feed")
print(f"Got {len(entries)} entries")
for e in entries:
    print(e['title'])
```

## Performance Tips

### Profiling

```bash
# Profile fetch command
python -m cProfile -o fetch.prof main.py fetch
python -m pstats fetch.prof
# Then: sort cumtime, stats 20
```

### Optimization Guidelines

- Use connection pooling (`requests.Session`)
- Batch database operations
- Cache expensive computations (`@lru_cache`)
- Use ThreadPoolExecutor for I/O-bound tasks
- Avoid N+1 queries in database operations

## Common Issues

### Python 3.13 Required

**Problem:** `pip install` fails with Python version error.

**Solution:** Install Python 3.13 or update `pyproject.toml` to lower requirement.

### SQLite Extension Load Error

**Problem:** `sqlite_vec` fails to load.

**Solution:** Ensure SQLite is compiled with extension support:

```bash
python -c "import sqlite3; print(sqlite3.sqlite_version)"
# Should be 3.8.0+
```

### Import Errors

**Problem:** `ModuleNotFoundError` when importing from `ednews`.

**Solution:** Install in editable mode:

```bash
pip install -e .
```

### Test Failures

**Problem:** Tests fail with database errors.

**Solution:** Ensure `db-init` has run or use in-memory DB in tests:

```python
conn = sqlite3.connect(":memory:")
```

## Contributing Guidelines

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`
3. **Write tests** for new functionality
4. **Ensure all tests pass** before submitting
5. **Update documentation** if changing APIs
6. **Keep commits focused** and write clear messages
7. **Open a pull request** with description of changes

### Commit Message Format

```
Add feature to do X

- Implement X functionality
- Add tests for X
- Update docs for X

Fixes #123
```

## Release Process

1. Update version in `pyproject.toml`
2. Update `__version__` in `ednews/__init__.py`
3. Run full test suite
4. Create git tag: `git tag v2025.11.01`
5. Push tag: `git push origin v2025.11.01`
6. Build package: `python -m build`
7. Upload to PyPI (if applicable)

## Resources

- **GitHub Repository**: https://github.com/ebardelli/ed-news
- **Issue Tracker**: https://github.com/ebardelli/ed-news/issues
- **Documentation**: See `docs/` directory
- **Python Docs**: https://docs.python.org/3/
- **pytest Docs**: https://docs.pytest.org/
- **Jinja2 Docs**: https://jinja.palletsprojects.com/

## Getting Help

- Open an issue on GitHub
- Check existing documentation in `docs/`
- Review test files for usage examples
- Check commit history for similar changes

## License

See `pyproject.toml` for license information.
