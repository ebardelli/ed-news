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

# Default number of articles to include on the site build. When more than this
# number of articles share the same DATE(published) as the Nth article, all
# articles for that date are included.
ARTICLES_DEFAULT_LIMIT = 20
