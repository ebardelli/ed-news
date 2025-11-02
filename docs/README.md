# ed-news Documentation

Welcome to the ed-news documentation! This directory contains comprehensive guides for users, developers, and contributors.

## Documentation Overview

### For Users

- **[README](../README.md)** - Quick start guide and CLI reference
- **[Configuration Guide](CONFIGURATION.md)** - How to configure feeds and news sources

### For Developers

- **[Development Guide](DEVELOPMENT.md)** - Setting up your development environment and contributing
- **[Architecture](ARCHITECTURE.md)** - System design and component overview
- **[API Reference](API.md)** - Complete Python API documentation
- **[Processors Guide](PROCESSORS.md)** - Creating custom preprocessors and postprocessors

### Reference

- **[Database Schema](DATABASE.md)** - SQLite schema, tables, views, and queries

## Quick Navigation

### Getting Started

1. Start with the main [README](../README.md) for installation and quick start
2. Read [DEVELOPMENT.md](DEVELOPMENT.md) to set up your development environment
3. Explore [CONFIGURATION.md](CONFIGURATION.md) to understand feed configuration

### Understanding the System

1. Read [ARCHITECTURE.md](ARCHITECTURE.md) for system design and data flow
2. Check [DATABASE.md](DATABASE.md) for schema and data models
3. Review [PROCESSORS.md](PROCESSORS.md) to understand the processor architecture

### Working with the API

1. Browse [API.md](API.md) for Python API reference
2. Check [PROCESSORS.md](PROCESSORS.md) for processor development
3. See [DEVELOPMENT.md](DEVELOPMENT.md) for testing and debugging

## Key Concepts

### Preprocessors and Postprocessors

ed-news uses a two-phase processing pipeline:

- **Preprocessors** fetch and parse raw feeds/sites
- **Postprocessors** enrich articles with additional metadata

Learn more in [PROCESSORS.md](PROCESSORS.md).

### Feed Configuration

Feeds are configured in JSON files (`research.json`, `news.json`) with flexible processor options. Learn more in [CONFIGURATION.md](CONFIGURATION.md).

### Database

SQLite stores all articles, headlines, and metadata with support for:
- Deduplication via URL hashing
- Optional vector embeddings for similarity search
- Maintenance operations via CLI

Learn more in [DATABASE.md](DATABASE.md).

### Static Site Generation

The build process reads from SQLite and generates:
- `index.html` with articles and headlines
- Multiple RSS feeds (combined, articles-only, headlines-only)
- Static assets (CSS, JS)

Learn more in [ARCHITECTURE.md](ARCHITECTURE.md).

## Common Tasks

### Adding a New Feed

1. Edit `research.json` or `news.json`
2. Add feed configuration with appropriate processor
3. Run `uv run python main.py fetch`
4. Verify with `uv run python main.py build`

See [CONFIGURATION.md](CONFIGURATION.md) for details.

### Creating a Custom Processor

1. Create processor module in `ednews/processors/`
2. Export from `ednews/processors/__init__.py`
3. Add tests in `tests/test_processors_*.py`
4. Use in feed configuration

See [PROCESSORS.md](PROCESSORS.md) for details.

### Debugging Issues

1. Enable verbose logging: `uv run python main.py fetch -v`
2. Check database: `sqlite3 ednews.db`
3. Review logs for errors
4. Test processors individually

See [DEVELOPMENT.md](DEVELOPMENT.md) for debugging tips.

## Contributing

We welcome contributions! Please:

1. Read [DEVELOPMENT.md](DEVELOPMENT.md) for setup
2. Follow the coding conventions
3. Add tests for new features
4. Update documentation as needed
5. Submit a pull request

## Getting Help

- **Issues**: Open an issue on GitHub
- **Documentation**: Check the relevant guide in this directory
- **Examples**: Review test files in `tests/`
- **Code**: Browse source in `ednews/`

## Documentation Maintenance

When updating code, please also update:

- API signatures → [API.md](API.md)
- Database schema → [DATABASE.md](DATABASE.md)
- Configuration format → [CONFIGURATION.md](CONFIGURATION.md)
- Architecture diagrams → [ARCHITECTURE.md](ARCHITECTURE.md)
- Processor examples → [PROCESSORS.md](PROCESSORS.md)

## License

See the main repository for license information.
