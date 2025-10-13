import os
import shutil
from pathlib import Path
from configparser import ConfigParser
from jinja2 import Environment, FileSystemLoader
import sqlite3
from datetime import datetime
from email.utils import parsedate_to_datetime

BUILD_DIR = Path("build")
TEMPLATES_DIR = Path("templates")
STATIC_DIR = Path("static")
PLANET_FILE = Path("planet.ini")
DB_FILE = Path("ednews.db")


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
            print("wrote", out_path)


def copy_static(out_dir: Path):
    dest = out_dir / "static"
    if dest.exists():
        shutil.rmtree(dest)
    if STATIC_DIR.exists():
        shutil.copytree(STATIC_DIR, dest)
        print("copied static ->", dest)


def build(out_dir: Path = BUILD_DIR):
    print("building static site into", out_dir)
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
            print(f"loaded {len(ctx['articles'])} articles from {DB_FILE}")
        except Exception as e:
            print("warning: failed to load articles from DB:", e)
            ctx["articles"] = []
    else:
        ctx["articles"] = []
    out_dir.mkdir(parents=True, exist_ok=True)
    render_templates(ctx, out_dir)
    copy_static(out_dir)
    print("done")


def read_articles(db_path: Path, limit: int = 30):
    """Read up to `limit` most-recent articles from the sqlite database.

    This function is intentionally conservative: it will try to find a sensible
    table (common names like `articles`, `posts`, `items`) and pick likely
    columns for title, link, content and published timestamp. If it can't
    detect those, it falls back to selecting a few columns and ordering by
    rowid.
    Returns a list of dicts with keys: title, link, content, published, raw
    where `raw` contains the original row mapping.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    if not tables:
        conn.close()
        return []

    preferred = ["articles", "article", "posts", "items", "entries", "news"]
    table = None
    for p in preferred:
        if p in tables:
            table = p
            break

    # if no preferred name found, look for a table with a date-like column
    if not table:
        for t in tables:
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = [r[1] for r in cur.fetchall()]
            lower = [c.lower() for c in cols]
            for candidate in ("published", "pub_date", "pubdate", "date", "created", "timestamp"):
                if candidate in lower:
                    table = t
                    break
            if table:
                break

    # final fallback to first table
    if not table:
        table = tables[0]

    cur.execute(f"PRAGMA table_info('{table}')")
    cols_info = cur.fetchall()
    cols = [r[1] for r in cols_info]

    # heuristics for fields
    lower_cols = {c.lower(): c for c in cols}
    title_field = lower_cols.get('title') or lower_cols.get('name') or lower_cols.get('headline')
    feed_field = lower_cols.get('feed_title') or lower_cols.get('feed') or lower_cols.get('journal')
    link_field = lower_cols.get('link') or lower_cols.get('url') or lower_cols.get('permalink')
    content_field = lower_cols.get('content') or lower_cols.get('body') or lower_cols.get('description') or lower_cols.get('text') or lower_cols.get('summary')
    pub_field = (lower_cols.get('published') or lower_cols.get('pub_date') or lower_cols.get('pubdate') or lower_cols.get('date') or lower_cols.get('created') or lower_cols.get('timestamp'))

    # build select list (avoid duplicates). include feed_field so feed_title is retrieved
    select_order = []
    for f in (title_field, feed_field, link_field, content_field, pub_field):
        if f and f not in select_order:
            select_order.append(f)
    if not select_order:
        # pick first up to 4 columns
        select_order = cols[: min(4, len(cols))]

    select_clause = ", ".join([f'"{c}"' for c in select_order])
    order_clause = f'ORDER BY "{pub_field}" DESC' if pub_field else 'ORDER BY rowid DESC'
    q = f'SELECT {select_clause} FROM "{table}" {order_clause} LIMIT ?'
    cur.execute(q, (limit,))
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    def format_short_date(value):
        if not value:
            return None
        # If it's already a datetime, format directly
        if isinstance(value, datetime):
            return value.strftime("%a, %d %b %Y")
        s = str(value)
        # Try RFC-2822 / RFC-822 parsing
        try:
            dt = parsedate_to_datetime(s)
            return dt.strftime("%a, %d %b %Y")
        except Exception:
            pass
        # Try ISO format
        try:
            dt = datetime.fromisoformat(s)
            return dt.strftime("%a, %d %b %Y")
        except Exception:
            pass
        # Try a few common strptime patterns
        for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%a, %d %b %Y")
            except Exception:
                continue
        # Try to extract a YYYY-MM-DD substring
        import re

        m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                return dt.strftime("%a, %d %b %Y")
            except Exception:
                pass
        # Fall back to original string if nothing parsed
        return s

    out = []
    for r in rows:
        pub_raw = r.get(pub_field) if pub_field else None
        published_short = format_short_date(pub_raw)
        out.append({
            'title': r.get(title_field) if title_field else None,
            'link': r.get(link_field) if link_field else None,
            'feed_title': r.get(feed_field) if feed_field else None,
            'content': r.get(content_field) if content_field else None,
            # replace published with the short format for templates
            'published': published_short,
            'raw': r,
        })
    return out


if __name__ == "__main__":
    build()