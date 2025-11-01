Preprocessors and Postprocessors
=================================

Overview
--------
This project now splits "processors" into two roles:

- Preprocessors: fetch or parse raw feeds/pages and return a list of entry dicts.
- Postprocessors: run after entries are saved, used to enrich articles or attach DOIs.

Naming conventions
------------------
- `<name>_preprocessor` — preprocessor-style callable, signature: `(session, url, publication_id=None, issn=None) -> list[dict]`
- `<name>_postprocessor_db` — DB-level postprocessor: `(conn, feed_key, entries, session=None, publication_id=None, issn=None) -> int`
- `<name>_postprocessor` — in-memory postprocessor: `(entries, session=None, publication_id=None, issn=None) -> list[dict] | None`

Compatibility
-------------
Existing `*_feed_processor` functions are treated as preprocessors for compatibility. The CLI will try `<name>_preprocessor` then fall back to `<name>_feed_processor`.

Migration
---------
1. Implement or alias a `<name>_preprocessor` for any feed that needs custom parsing.
2. Implement a `<name>_postprocessor_db` for DB-level enrichment (e.g., sciencedirect enrichment).
3. Add unit tests ensuring the pre/post processors are invoked in the fetch flow.

RSS preprocessor (canonical)
----------------------------
The project provides a canonical RSS/Atom preprocessor named `rss_preprocessor`
available from `ednews.processors`. It is a thin wrapper around
`ednews.feeds.fetch_feed` and returns the `entries` list so it can be used
as a preprocessor in feed configs.

Example `research.json` entry using the RSS preprocessor:

```json
{
	"feeds": {
		"example": {
			"title": "Example feed",
			"feed": "https://example.org/feed.xml",
			"processor": "rss"
		}
	}
}
```

CLI behavior
------------
When a feed's `processor` is set to `rss`, the CLI will call
`ednews.processors.rss_preprocessor(session, url, publication_id, issn)` during
the fetch preprocessor phase. If a feed has no configured preprocessor but no
preprocessor produced entries, the CLI will also prefer `rss_preprocessor` as
the canonical fallback.

Testing / monkeypatch example
-----------------------------
Because `rss_preprocessor` is exported from `ednews.processors`, tests can
monkeypatch it easily to avoid network calls. Example pytest snippet:

```python
import ednews.processors as proc_mod

def fake_rss_pre(session, url, publication_id=None, issn=None):
		return [{"title": "T", "link": "http://example/t", "summary": "s"}]

monkeypatch.setattr(proc_mod, "rss_preprocessor", fake_rss_pre, raising=False)
```

This mirrors other processor monkeypatch patterns used across the test suite.
