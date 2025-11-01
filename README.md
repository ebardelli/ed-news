# ed-news

A compact static site generator and feed builder for curated education news.

This repository builds an `index.html` and RSS feeds from configured article feeds and locally-maintained news-site processors. The Python package lives in `ednews/` and the developer-facing CLI is `main.py`.

## Quick layout

- `main.py` — CLI entrypoint that delegates to `ednews.cli`
- `ednews/` — package with build, feeds, embeddings, db, and processors modules
- `ednews.db` — local SQLite database used for development (binary)
- `templates/`, `static/` — site templates and assets
- `news.json`, `research.json` — local site/feed configuration
- `tests/` — unit/integration tests and fixtures

## Quick start

1. Create a development environment (uses `uv`):

```bash
uv sync
```

2. Initialize the SQLite database (one-time):

```bash
uv run python main.py db-init
```

3. Fetch feeds and build the site:

```bash
# fetch articles + headlines (default)
uv run python main.py fetch

# build static site into ./build
uv run python main.py build --out-dir build

# serve locally
uv run python main.py serve --directory build
```

## CLI reference (selected)

- fetch — fetch article feeds and/or news headlines (flags: `--articles`, `--headlines`)
- build — render templates into a static `build/` directory (`--out-dir`)
- embed — generate local embeddings and persist vectors to the DB (`--model`, `--batch-size`)
- issn-lookup — lookup recent works by ISSN and insert into DB
- headlines — fetch configured news sites and persist or write JSON (`--out`, `--no-persist`)
- db-init — create schema and views
- manage-db — subcommands for maintenance (migrate, vacuum, cleanup-empty-articles, sync-publications)

## Database notes

- The project uses SQLite. Schema and maintenance helpers are in `ednews/db/`.
- The `manage_db` helpers were moved into `ednews.db.manage_db`; update external imports if you relied on `ednews.manage_db`.

## CrossRef ISSN Lookup

```sh
uv run ednews issn-lookup \
  --date-filter-type created \
  --from-date '2024-01-01T00' \
  --per-journal 100
```

Date formats accepted for `--from-date` and `--until-date`:

- Year only: `2025`
- Year and month: `2025-04`
- Full date: `2025-04-01`
- Datetime (no timezone): `2025-04-01T12:30` (interpreted as UTC and converted to `2025-04-01T12:30:00+00:00`)
- Datetime with timezone/Z: `2025-04-01T12:30:00Z` or `2025-04-01T12:30:00+02:00`

Partial date fragments (year, year-month, year-month-day) are preserved as-is and passed to Crossref. Datetimes without a timezone are treated as UTC.

## Testing

Run the test suite with pytest:

```bash
uv run pytest -q
```

Some tests exercise Crossref integration — set `RUN_CROSSREF_INTEGRATION=1` to enable them.

## Notable changes (recent)

- Split article feeds and news headlines; `fetch` can target either or both.
- Empty/placeholder feed items are filtered before persisting and during build.
- Embeddings support for articles and headlines; see `ednews.embeddings` and the `embed` CLI subcommand.
- Additional site processors live under `ednews/processors/` (e.g., Press Democrat, FCMat).

## Contributing

Contributions welcome. Run tests locally and include fixtures/mocks for external services to keep CI fast.

## License

See `pyproject.toml` / `PKG-INFO` for project metadata.
