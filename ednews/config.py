"""Configuration constants and project paths for ed-news.

Defines root paths for planet files, the default SQLite DB path, user-agent
string for HTTP requests, and the default embedding model name.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLANET_JSON = ROOT / "planet.json"
PLANET_INI = ROOT / "planet.ini"
DB_PATH = ROOT / "ednews.db"

# Embedding config
USER_AGENT = "ed-news-fetcher/1.0"
DEFAULT_MODEL = "nomic-embed-text-v1.5"

# Crossref / HTTP fetch settings
# Read timeout (seconds) for Crossref API responses. Can be overridden by passing
# an explicit `timeout` to the db.fetch_latest_journal_works function or via
# environment variables in runtime deployments.
CROSSREF_TIMEOUT = 30
# Connect timeout (seconds) to avoid long TCP connect hangs
CROSSREF_CONNECT_TIMEOUT = 5
# Number of retries for transient errors (network/5xx/429)
CROSSREF_RETRIES = 3
# Backoff factor used by urllib3.util.retry.Retry (sleep = backoff_factor * (2 ** (retry - 1)))
CROSSREF_BACKOFF = 0.3
# Status codes that should trigger a retry
CROSSREF_STATUS_FORCELIST = [429, 500, 502, 503, 504]

# Default number of articles to include on the site build. When more than this
# number of articles share the same DATE(published) as the Nth article, all
# articles for that date are included.
ARTICLES_DEFAULT_LIMIT = 20
