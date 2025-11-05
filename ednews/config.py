"""Configuration constants and project paths for ed-news.

Defines root paths for planet files, the default SQLite DB path, user-agent
string for HTTP requests, and the default embedding model name.
"""

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RESEARCH_JSON = ROOT / "research.json"
# Primary JSON config file for the project
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

# Default number of headlienes to include on the site build. When more than this
# number of headlines share the same DATE(published) as the Nth headline, all
# headlines for that date are included.
HEADLINES_DEFAULT_LIMIT = 20

# Maximum number of extra articles to include when expanding the selection to
# include all articles that share the Nth article's DATE(published). This
# prevents pathological cases where a single date contains hundreds or
# thousands of articles and the build would attempt to include them all.
# The effective maximum returned articles will be: limit + ARTICLES_MAX_SAME_DATE_EXTRA
ARTICLES_MAX_SAME_DATE_EXTRA = 20

# Default date to use when an item has no published/first_seen value.
# Stored as an ISO date string (YYYY-MM-DD).
DEFAULT_MISSING_DATE = "2020-01-01"

# RSS feed metadata defaults
FEED_SITE_LINK = "https://ed-news.ebardelli.com/"
FEED_TITLE_COMBINED = "Latest Education News"
FEED_TITLE_ARTICLES = "Latest Education Articles"
FEED_TITLE_HEADLINES = "Latest Education Headlines"

# Titles to exclude from processing/saving. Comparison is done on the
# trimmed, lowercased title string. Add additional blacklist titles here.
TITLE_FILTERS = [
	"editorial board",
	"editorial announcement",
    "reviewer acknowledgements",
]
