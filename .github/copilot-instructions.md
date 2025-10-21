# Copilot Instructions for ed-news

## Overview
ed-news is a compact static site generator and feed builder written in Python. It builds an `index.html` and RSS feeds from configured article feeds and locally-configured news sites. The public API surface is primarily the `ednews` package and the `main.py` CLI entrypoint used in development and CI.

## Key Components

### Top-Level CLI
- `main.py`: CLI entrypoint which delegates to `ednews.cli`. Subcommands include:
  - `fetch` — fetch configured article feeds and news headlines and persist them to the DB (defaults to both articles and headlines if no flags provided)
  - `build` — render the static site into an output directory
  - `embed` — generate local embeddings and insert them into the DB
  - `enrich-crossref` — query Crossref to enrich articles missing Crossref metadata
  - `issn-lookup` — fetch recent works for journals by ISSN and insert into the DB
  - `headlines` — fetch configured news sites and either persist headlines to the DB or write JSON
  - `db-init` — create tables and views (run once)
  - `manage-db` — maintenance subcommands (migrate, vacuum, cleanup-empty-articles, sync-publications, run-all)
  - `serve` — serve the generated `build` directory locally

### `ednews` Package
- `build.py`: Core build and template rendering logic.
- `feeds.py`: Feed loading, fetching, normalization, and save helpers.
- `db/`: Database helpers and maintenance code (`manage_db.py`, `init_db`, enrichment helpers).
- `config.py`: Centralized configuration (paths, constants) used across the package.
- `embeddings.py`: Embeddings creation and vector storage helpers.
- `news.py` and `processors/`: News-site scraping and site-specific processors (e.g., Press Democrat, FCMat).

### Templates and Static Assets
- `templates/`: Jinja2 templates used to render the site and RSS feeds.
- `static/`: CSS and JS files copied into the generated `build/` output.

### Tests
- Unit and integration tests live in `tests/`. They cover feeds, Crossref parsing, DB behavior, embedding generation, and CLI handlers.

## Developer Workflows

### Running the CLI
Use the `main.py` wrapper. Examples (use `uv run` if you prefer `uv` for environment isolation):

```bash
# Fetch everything (articles + headlines)
uv run python main.py fetch

# Only fetch article feeds
uv run python main.py fetch --articles

# Only fetch headlines
uv run python main.py fetch --headlines

# Build site
uv run python main.py build --out-dir build

# Generate embeddings
uv run python main.py embed --batch-size 64

# Enrich articles from Crossref
uv run python main.py enrich-crossref --batch-size 20 --delay 0.1

 # ISSN lookup
 # --from-date/--until-date accept: YYYY, YYYY-MM, YYYY-MM-DD, or datetimes like YYYY-MM-DDTHH:MM
 # Datetimes without timezone are interpreted as UTC.
 uv run python main.py issn-lookup --per-journal 30

# DB initialization
uv run python main.py db-init

# Database maintenance
uv run python main.py manage-db cleanup-empty-articles --older-than-days 90

# Serve built site
uv run python main.py serve --directory build
```

Tip: `-v/--verbose` enables debug logging for CLI commands.

### Running Tests

Run tests with pytest:

```bash
uv run pytest
```

Set `RUN_CROSSREF_INTEGRATION=1` in the environment to enable Crossref integration tests when needed.

## Project Conventions
- Database: SQLite is used for persistence. The schema and views are created by `ednews.db.init_db` and exposed via the `db-init` CLI command.
- Feeds: Article feeds and headline sources are defined in top-level JSON files (`planet.json` / `news.json` / `research.json`) or in code under `ednews/processors/` for site-specific scraping.
- Embeddings: Local embedding generation uses `ednews.embeddings` and stores vectors in the DB.
- Logging: Modules use standard `logging` with module-level loggers (for example `ednews.cli`).

### Docstrings
- Prefer Google-style docstrings for new/changed functions and modules.

## External Dependencies
- `sqlite-vec` (optional) for vector similarity.
- `jinja2` for templates.
- `feedparser` for parsing RSS/Atom feeds.
- `requests` for HTTP interactions.

## Notes for Copilot usage
- Tests are a reliable guide to the public behavior of this package — prefer changes that keep tests green.
- The `ednews.cli` module is the main surface for CLI behavior; update tests when adding/renaming subcommands.
- The DB helpers moved into `ednews.db` during a recent refactor; prefer the new import paths (`ednews.db.manage_db`).

## Key files
- `main.py` — CLI entrypoint delegating to `ednews.cli`
- `ednews/cli.py` — CLI implementation and handlers
- `ednews/build.py` — static site rendering logic
- `ednews/feeds.py` — feed fetching and saving
- `ednews/db/manage_db.py` — DB maintenance utilities
- `ednews/embeddings.py` — embeddings utilities
