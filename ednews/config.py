from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PLANET_JSON = ROOT / "planet.json"
PLANET_INI = ROOT / "planet.ini"
DB_PATH = ROOT / "ednews.db"

USER_AGENT = "ed-news-fetcher/1.0"
DEFAULT_MODEL = "nomic-embed-text-v1.5"
