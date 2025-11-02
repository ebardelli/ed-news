# ed-news

ed-news is a compact static site generator and feed builder for curated education news.

This repository contains the `ednews` Python package and a small CLI entrypoint (`main.py`) used to fetch feeds, build the static site, generate embeddings, and run lightweight DB maintenance for development.

For full documentation (architecture, configuration, development, API, and database schema) see the `docs/` directory.

## Quick Start

```bash
uv run python main.py db-init
uv run python main.py fetch
uv run python main.py build --out-dir build
uv run python main.py serve --directory build
```
