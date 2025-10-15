# Copilot Instructions for ed-news

## Overview
This repository is a static site generator and feed builder written in Python. It processes metadata and feeds to produce an `index.html` and an RSS feed. The project is organized into a package (`ednews`) and top-level scripts for building and publishing the site.

## Key Components

### Top-Level Scripts
- `main.py`: CLI entry point with subcommands for common workflows. Available commands include:
	- `fetch` — fetch configured feeds and save entries to the database
	- `build` — render the static site into a directory (same core logic as `build.py`)
	- `embed` — generate local embeddings and insert into the DB
	- `enrich-crossref` — query Crossref to enrich articles missing Crossref XML metadata
	- `issn-lookup` — fetch recent works for journals by ISSN and insert into the DB 

### `ednews` Package
- `build.py`: Core build logic, including template rendering and embedding article similarity.
- `feeds.py`: Handles feed loading, fetching, and normalization.
- `db.py`: Database helpers for SQLite, including schema initialization.
- `config.py`: Centralized configuration (e.g., paths, constants).
- `embeddings.py`: Utilities for vector/embedding calculations.

### Templates and Static Assets
- `templates/`: Jinja2 templates for `index.html` and RSS.
- `static/`: CSS and JS files included in the build.

### Tests
- Located in `tests/`. Includes unit tests for modules like `build`, `feeds`, and `db`.

## Developer Workflows

### Building the Site
Run the build script to generate the static site:
```bash
uv run python build.py
```

### Fetching Feeds
Use the `fetch` subcommand to download and process feeds:
```bash
uv run python main.py fetch
```

Other `main.py` examples (also documented in the project `README.md`):
```bash
# Render the static site
uv run python main.py build --out-dir build

# Generate embeddings and store them in the DB
uv run python main.py embed --model <model-name> --batch-size 64

# Enrich missing Crossref metadata
uv run python main.py enrich-crossref --batch-size 20 --delay 0.1

# Lookup recent works by ISSN
uv run python main.py issn-lookup --per-journal 30 --timeout 10 --delay 0.05
```

Tip: the `main.py` CLI supports a global `-v/--verbose` flag to enable debug logging for commands. Example:

```bash
# Run fetch with verbose output
uv run python main.py -v fetch
```

### Running Tests
Tests are located in the `tests/` directory. Run them with:
```bash
uv run pytest
```

## Project-Specific Conventions
- **Database**: SQLite is used for storing feed and article data. Schema is initialized in `ednews/db.py`.
- **Embeddings**: Article similarity is calculated using `sqlite-vec` and stored in the database.
- **Feeds**: Feeds are defined in `planet.json` or `planet.ini`. JSON is preferred.
- **Logging**: Modules use `logging` with namespaced loggers (e.g., `ednews.build`).

## External Dependencies
- `sqlite-vec`: Used for vector similarity calculations.
- `jinja2`: For template rendering.
- `feedparser`: For parsing RSS/Atom feeds.
- `requests`: For HTTP requests.

## Examples
- Adding a new feed: Update `planet.json` with feed details.
- Debugging a build issue: Check logs in `ednews/build.py`.

## Key Files
- `main.py`: CLI entry point.
- `ednews/build.py`: Core build logic.
- `ednews/feeds.py`: Feed handling.
- `ednews/db.py`: Database helpers.
- `templates/index.html.jinja2`: HTML template.
- `tests/test_build.py`: Tests for build logic.
 - `tests/test_build.py`: Tests for build logic.
