import os
import shutil
from pathlib import Path
from configparser import ConfigParser
from jinja2 import Environment, FileSystemLoader
import sqlite3
from datetime import datetime
from email.utils import parsedate_to_datetime
import logging

BUILD_DIR = Path("build")
TEMPLATES_DIR = Path("templates")
STATIC_DIR = Path("static")
PLANET_FILE = Path("planet.ini")
DB_FILE = Path("ednews.db")

logger = logging.getLogger("ednews.build")


def read_planet(planet_path: Path):
    # prepend a fake section so ConfigParser can read top-level `title = ...`
    raw = planet_path.read_text(encoding="utf-8")
    patched = "[global]\n" + raw
    cfg = ConfigParser()
    cfg.read_string(patched)
    site_title = cfg.get("global", "title", fallback="Latest Research Articles in Education")
    feeds = []
    for section in cfg.sections():
        if section == "global":
            continue
        item = {
            "id": section,
            "title": cfg.get(section, "title", fallback=section),
            "link": cfg.get(section, "link", fallback=""),
            "feed": cfg.get(section, "feed", fallback=""),
        }
        feeds.append(item)
    return {"title": site_title, "feeds": feeds}


def render_templates(context, out_dir: Path):
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    # render Jinja2 templates that end with .jinja2
    for tmpl_name in os.listdir(TEMPLATES_DIR):
        if tmpl_name.endswith(".jinja2"):
            tpl = env.get_template(tmpl_name)
            out_name = tmpl_name.replace(".jinja2", "")
            out_path = out_dir / out_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(tpl.render(context), encoding="utf-8")
            logger.info("wrote %s", out_path)


def copy_static(out_dir: Path):
    dest = out_dir / "static"
    if dest.exists():
        shutil.rmtree(dest)
    if STATIC_DIR.exists():
        shutil.copytree(STATIC_DIR, dest)
        logger.info("copied static -> %s", dest)


def build(out_dir: Path = BUILD_DIR):
    logger.info("building static site into %s", out_dir)
    ctx = read_planet(PLANET_FILE) if PLANET_FILE.exists() else {"title": "Latest Research Articles in Education", "feeds": []}
    # add a build timestamp for templates
    # Prefer using the IANA Pacific timezone so the output shows PST/PDT as appropriate.
    try:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("America/Los_Angeles")
            ctx["build_time"] = datetime.now(tz).strftime("%a, %d %b %Y %H:%M %Z")
        except Exception:
            # Fall back to local timezone-aware time
            ctx["build_time"] = datetime.now().astimezone().strftime("%a, %d %b %Y %H:%M %Z")
    except Exception:
        # Final fallback: naive local time
        ctx["build_time"] = datetime.now().strftime("%a, %d %b %Y %H:%M")
    # load latest articles from SQLite DB if present
    if DB_FILE.exists():
        try:
            ctx["articles"] = read_articles(DB_FILE, limit=20)
            logger.info("loaded %d articles from %s", len(ctx["articles"]), DB_FILE)
        except Exception as e:
            logger.warning("failed to load articles from DB: %s", e)
            ctx["articles"] = []
    else:
        ctx["articles"] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    render_templates(ctx, out_dir)
    copy_static(out_dir)
    logger.info("done")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    build()


def read_articles(db_path: Path, limit: int = 30):
    """Read up to `limit` most-recent articles using the `combined_articles` view.

    This function assumes the view `combined_articles` already exists and is
    maintained elsewhere. If the view isn't present or the query fails, the
    function logs a warning and returns an empty list.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    try:
        cur.execute(
            "SELECT title, link, feed_title, content, published, authors FROM combined_articles ORDER BY published DESC LIMIT ?",
            (limit,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    except Exception as e:
        logger.warning("combined_articles view missing or query failed: %s", e)
        conn.close()
        return []

    conn.close()

    def format_short_date(value):
        if not value:
            return None
        if isinstance(value, datetime):
            return value.strftime("%a, %d %b %Y")
        s = str(value)
        try:
            dt = parsedate_to_datetime(s)
            return dt.strftime("%a, %d %b %Y")
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime("%a, %d %b %Y")
        except Exception:
            pass
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%a, %d %b %Y")
            except Exception:
                continue
        import re

        m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                return dt.strftime("%a, %d %b %Y")
            except Exception:
                pass
        return s

    out = []
    for r in rows:
        pub_raw = r.get('published')
        published_short = format_short_date(pub_raw)
        out.append({
            'title': r.get('title'),
            'link': r.get('link'),
            'feed_title': r.get('feed_title'),
            'content': r.get('content'),
            'published': published_short,
            'raw': r,
        })
    return out


if __name__ == "__main__":
    build()