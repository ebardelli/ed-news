# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Documentation

The `docs/` directory is the authoritative source of documentation. Read these before making changes:

| Topic | File |
|---|---|
| Architecture & data flow | `docs/ARCHITECTURE.md` |
| Feed & news configuration | `docs/CONFIGURATION.md` |
| Database schema & queries | `docs/DATABASE.md` |
| Processor development | `docs/PROCESSORS.md` |
| Dev setup, testing, CLI usage | `docs/DEVELOPMENT.md` |
| Python API reference | `docs/API.md` |

**When making changes**, update the relevant file(s) in `docs/` to reflect those changes — schema changes → `DATABASE.md`, new processors → `PROCESSORS.md`, new CLI commands → `DEVELOPMENT.md`, architecture shifts → `ARCHITECTURE.md`.

## Quick Commands

```bash
uv sync                                          # install dependencies
uv run ednews db-init                            # initialize schema (one-time)
uv run ednews fetch                              # fetch all feeds
uv run ednews build --out-dir build              # build static site
uv run ednews manage-db run-all --older-than-days 7  # maintenance
uv run pytest -q                                 # run tests
uv run pytest tests/test_foo.py::test_bar -v     # run single test
```

The CLI is also accessible as `uv run python main.py <command>`. Justfile shortcuts: `just fetch`, `just build`, `just serve`, `just db`.

## Key Design Notes

- **Two-phase processing**: preprocessors run during fetch (return normalized entry dicts); postprocessors run after DB save (enrich DB records). See `docs/PROCESSORS.md`.
- **Deduplication** happens at three levels: URL hash, `(guid, link, title, published)` tuple, and DOI uniqueness.
- **Feed fetching** uses `ThreadPoolExecutor` with 8 workers.
- **Crossref retries** are configured in `ednews/config.py` (`CROSSREF_*` constants).
- **Embeddings** are optional; stored in sqlite-vec virtual tables via `uv run ednews embed`.
