"""Build utilities for ed-news.

This module contains helpers to render the static site from templates,
copy static assets and the SQLite database, and read feed/planet
configuration used to assemble the site.
"""

import os
import shutil
from pathlib import Path
from configparser import ConfigParser
from jinja2 import Environment, FileSystemLoader
import sqlite3
import sqlite_vec
from datetime import datetime
from email.utils import parsedate_to_datetime
import logging
from . import config
import duckdb

MODEL_NAME = config.DEFAULT_MODEL

BUILD_DIR = Path("build")
TEMPLATES_DIR = Path("templates")
STATIC_DIR = Path("static")
PLANET_FILE = config.PLANET_INI
DB_FILE = config.DB_PATH

logger = logging.getLogger("ednews.build")


def get_similar_articles_by_doi(conn, doi, top_n=5, model=MODEL_NAME, store_if_missing: bool = True):
    """Return a list of similar articles for the given DOI using stored embeddings.

    The function looks up the article by DOI, retrieves its embedding from the
    ``articles_vec`` virtual table and performs a cosine-distance nearest-
    neighbors query using sqlite-vec.

    Args:
        conn (sqlite3.Connection): Open SQLite connection with sqlite-vec loaded.
        doi (str): DOI of the target article to find similarities for.
        top_n (int): Maximum number of similar articles to return.
        model (str): Embedding model name (unused here, kept for API compatibility).
        store_if_missing (bool): Whether to attempt to store a generated embedding
            if missing (not implemented here; present for compatibility).

    Returns:
        list: A list of dicts with keys: 'doi', 'title', 'abstract', 'distance'.
    """
    conn.enable_load_extension(True)
    sqlite_vec.load(conn)

    cur = conn.cursor()

    cur.execute("SELECT id, title, abstract FROM articles WHERE doi = ? LIMIT 1", (doi,))
    row = cur.fetchone()
    if not row:
        logger.debug("No article found with DOI: %s", doi)
        return []

    article_id, title, abstract = row
    title = title or ""
    abstract = abstract or ""
    combined = title.strip()
    if abstract.strip():
        combined = combined + "\n\n" + abstract.strip() if combined else abstract.strip()

    cur.execute("SELECT embedding FROM articles_vec WHERE rowid = ?", (article_id,))
    res = cur.fetchone()
    if res and res[0]:
        target_blob = res[0]
    else:
        logger.debug("No embedding found for DOI %s (id=%s)", doi, article_id)
        return []

    q = '''
    SELECT A.doi, A.title, A.abstract, vec_distance_cosine(V.embedding, ?) AS distance
    FROM articles AS A, articles_vec AS V
    WHERE A.id = V.rowid AND A.id != ?
    ORDER BY distance ASC
    LIMIT ?
    '''

    results = cur.execute(q, (target_blob, article_id, top_n)).fetchall()

    out_list = []
    for doi_r, title_r, abstract_r, distance in results:
        out_list.append({
            "doi": doi_r,
            "title": title_r,
            "abstract": abstract_r,
            "distance": float(distance) if distance is not None else None,
        })

    return out_list


def read_planet(planet_path: Path):
    """Read a planet.ini-style file and return site metadata and feeds.

    The function wraps the planet file with a ``[global]`` section so that
    :class:`configparser.ConfigParser` can parse top-level key/value pairs.

    Args:
        planet_path (pathlib.Path): Path to the planet.ini file to read.

    Returns:
        dict: A mapping with keys ``title`` and ``feeds``. ``feeds`` is a list
            of feed descriptor dicts with keys ``id``, ``title``, ``link`` and
            ``feed``.
    """
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
    """Render Jinja2 templates from the templates directory into ``out_dir``.

    Args:
        context (dict): Template context mapping used when rendering templates.
        out_dir (pathlib.Path): Output directory where rendered templates will
            be written.
    """
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    for tmpl_name in os.listdir(TEMPLATES_DIR):
        if tmpl_name.endswith(".jinja2"):
            tpl = env.get_template(tmpl_name)
            out_name = tmpl_name.replace(".jinja2", "")
            out_path = out_dir / out_name
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(tpl.render(context), encoding="utf-8")
            logger.info("wrote %s", out_path)


def copy_static(out_dir: Path):
    """Copy the project's ``static/`` directory into the build output.

    If the destination exists it will be removed before copying.

    Args:
        out_dir (pathlib.Path): Destination directory for static assets.
    """
    dest = out_dir / "static"
    if dest.exists():
        shutil.rmtree(dest)
    if STATIC_DIR.exists():
        shutil.copytree(STATIC_DIR, dest)
        logger.info("copied static -> %s", dest)


def build(out_dir: Path = BUILD_DIR):
    """High-level build function to render the static site.

    The function collects site metadata, optionally loads recent articles
    from the configured DB, computes similar-article suggestions, renders
    templates, copies static assets and the DB into the output directory.

    Args:
        out_dir (pathlib.Path): Destination directory for the built static site.
    """
    logger.info("building static site into %s", out_dir)
    ctx = read_planet(PLANET_FILE) if PLANET_FILE.exists() else {"title": "Latest Research Articles in Education", "feeds": []}
    try:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("America/Los_Angeles")
            ctx["build_time"] = datetime.now(tz).strftime("%a, %d %b %Y %H:%M %Z")
        except Exception:
            ctx["build_time"] = datetime.now().astimezone().strftime("%a, %d %b %Y %H:%M %Z")
    except Exception:
        ctx["build_time"] = datetime.now().strftime("%a, %d %b %Y %H:%M")

    if DB_FILE.exists():
        try:
            # use `publications=5` to get the latest 5 publications (all articles
            # from each publication's most recent date)
            ctx["articles"] = read_articles(DB_FILE, publications=5)
            logger.info("loaded %d articles from %s", len(ctx["articles"]), DB_FILE)
            if get_similar_articles_by_doi and ctx.get("articles"):
                try:
                    conn = sqlite3.connect(str(DB_FILE))
                    for art in ctx["articles"]:
                        doi = art.get("doi")
                        raw = art.get("raw") or {}
                        doi_source = None
                        if doi:
                            doi_source = 'top-level'
                        elif isinstance(raw, dict) and raw.get('doi'):
                            doi = raw.get('doi')
                            doi_source = 'raw'
                        else:
                            link = art.get("link")
                            if isinstance(link, str) and link.startswith("https://doi.org/"):
                                doi = link[len("https://doi.org/"):] 
                                doi_source = 'link'

                        if not doi:
                            art["similar_articles"] = []
                            continue

                        try:
                            sims = get_similar_articles_by_doi(conn, doi, top_n=5, store_if_missing=False)
                            art["similar_articles"] = sims or []
                        except Exception as e:
                            logger.exception("Error computing similar articles for DOI=%s: %s", doi, e)
                            art["similar_articles"] = []
                finally:
                    try:
                        conn.close()
                    except Exception:
                        pass
        except Exception as e:
            logger.warning("failed to load articles from DB: %s", e)
            ctx["articles"] = []
    else:
        ctx["articles"] = []

    if "articles" in ctx:
        grouped_articles = {}
        for article in ctx["articles"]:
            publication = article.get("feed_title", "Unknown Publication")
            if publication not in grouped_articles:
                grouped_articles[publication] = []
            grouped_articles[publication].append(article)
        ctx["grouped_articles"] = grouped_articles
    else:
        ctx["grouped_articles"] = {}

    out_dir.mkdir(parents=True, exist_ok=True)
    render_templates(ctx, out_dir)
    copy_static(out_dir)
    # export selected DB tables to parquet files under build/db/
    try:
        export_db_parquet(out_dir)
    except Exception:
        logger.exception("export_db_parquet failed")
    logger.info("done")


def read_articles(db_path: Path, limit: int = 30, days: int | None = None, publications: int | None = None):
    """Read recent articles from the ``combined_articles`` view in the DB.

    The function supports several retrieval modes:
    * If ``publications`` is provided, returns all articles from the latest
      N publications (one date per publication).
    * Else if ``days`` is provided, returns articles whose ``DATE(published)``
      is in the latest ``days`` distinct dates.
    * Otherwise returns the most recent ``limit`` articles.

    Args:
        db_path (pathlib.Path): Path to the SQLite database file.
        limit (int): Fallback limit for the number of articles to return.
        days (int | None): Optional number of distinct recent dates to include.
        publications (int | None): Optional number of latest publications to include.

    Returns:
        list[dict]: A list of article dictionaries containing ``title``, ``doi``,
            ``link``, ``feed_title``, ``content``, ``published`` (short date
            string), and ``raw`` (original DB row).
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = []

    # publications takes precedence over days when provided. If publications is set,
    # return all articles from each of the latest `publications` feeds where the
    # article's date equals that feed's most recent date.
    if publications is not None:
        try:
            # Load rows with non-null published and feed_title
            cur.execute("SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE published IS NOT NULL AND feed_title IS NOT NULL")
            all_rows = [dict(r) for r in cur.fetchall()]

            if all_rows:
                latest_per_feed = {}
                for r in all_rows:
                    ft = r.get('feed_title')
                    pub = r.get('published') or ''
                    if ft not in latest_per_feed or (pub and pub > latest_per_feed[ft]):
                        latest_per_feed[ft] = pub

                sorted_feeds = sorted(latest_per_feed.items(), key=lambda kv: kv[1], reverse=True)
                top_feeds = [ft for ft, _ in sorted_feeds[:publications]]

                feeds_latest_date = {ft: (latest_per_feed[ft][:10] if latest_per_feed[ft] else None) for ft in top_feeds}

                filtered = []
                for r in all_rows:
                    ft = r.get('feed_title')
                    if ft in feeds_latest_date and feeds_latest_date[ft]:
                        if (r.get('published') or '')[:10] == feeds_latest_date[ft]:
                            filtered.append(r)

                rows = sorted(filtered, key=lambda r: r.get('published') or '', reverse=True)
        except Exception:
            rows = []

    # If publications not used, and days is provided, use the previous behavior of
    # selecting articles whose DATE(published) is in the latest `days` distinct dates.
    elif days is not None:
        try:
            cur.execute(
                """
                SELECT doi, title, link, feed_title, content, published, authors
                FROM combined_articles
                WHERE DATE(published) IN (
                    SELECT DISTINCT DATE(published) AS d
                    FROM combined_articles
                    WHERE published IS NOT NULL
                    ORDER BY d DESC
                    LIMIT ?
                )
                ORDER BY published DESC
                LIMIT 50
                """,
                (days,),
            )
            rows = [dict(r) for r in cur.fetchall()]
        except Exception:
            rows = []

    # Fallback: select most recent `limit` articles
    if not rows:
        try:
            cur.execute(
                "SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles ORDER BY published DESC LIMIT ?",
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
    parsed_rows = []
    for r in rows:
        pub_raw = r.get('published')
        date_key = None
        if pub_raw is None:
            date_key = None
        else:
            try:
                if isinstance(pub_raw, datetime):
                    date_key = pub_raw.date().isoformat()
                else:
                    s = str(pub_raw)
                    try:
                        dt = parsedate_to_datetime(s)
                        date_key = dt.date().isoformat()
                    except Exception:
                        try:
                            dt = datetime.fromisoformat(s)
                            date_key = dt.date().isoformat()
                        except Exception:
                            for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                                try:
                                    dt = datetime.strptime(s, fmt)
                                    date_key = dt.date().isoformat()
                                    break
                                except Exception:
                                    continue
                            if date_key is None:
                                import re

                                m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
                                if m:
                                    try:
                                        dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                                        date_key = dt.date().isoformat()
                                    except Exception:
                                        date_key = None
            except Exception:
                date_key = None

        parsed_rows.append((date_key, r))

    if days is None:
        selected = parsed_rows
    else:
        distinct_dates = []
        for dk, _ in parsed_rows:
            # If publications is provided, select articles from the latest N publications
            # where "latest" is the most recent published date per publication, and
            # include only articles from that publication that were published on that date.
            if publications is not None:
                try:
                    # Python-driven approach for robustness across SQLite versions.
                    # 1) Load all rows where published and feed_title are not null.
                    cur.execute("SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE published IS NOT NULL AND feed_title IS NOT NULL")
                    all_rows = [dict(r) for r in cur.fetchall()]

                    if not all_rows:
                        rows = []
                    else:
                        # 2) Compute latest published (string compare of timestamp) per feed_title
                        latest_per_feed = {}
                        for r in all_rows:
                            ft = r.get('feed_title')
                            pub = r.get('published') or ''
                            # use raw string compare on ISO-like timestamps; ensure longer is greater
                            if ft not in latest_per_feed or (pub and pub > latest_per_feed[ft]):
                                latest_per_feed[ft] = pub

                        # 3) Sort feeds by latest published desc and take top-N
                        sorted_feeds = sorted(latest_per_feed.items(), key=lambda kv: kv[1], reverse=True)
                        top_feeds = [ft for ft, _ in sorted_feeds[:publications]]

                        # 4) For each top feed, compute YYYY-MM-DD latest date and filter rows
                        feeds_latest_date = {ft: (latest_per_feed[ft][:10] if latest_per_feed[ft] else None) for ft in top_feeds}

                        filtered = []
                        for r in all_rows:
                            ft = r.get('feed_title')
                            if ft in feeds_latest_date and feeds_latest_date[ft]:
                                if (r.get('published') or '')[:10] == feeds_latest_date[ft]:
                                    filtered.append(r)

                        # order by published desc
                        rows = sorted(filtered, key=lambda r: r.get('published') or '', reverse=True)
                except Exception:
                    rows = []
            elif days is not None:
                continue
            if dk not in distinct_dates:
                distinct_dates.append(dk)
            if len(distinct_dates) >= days:
                break
        allowed = set(distinct_dates)
        selected = [pr for pr in parsed_rows if pr[0] in allowed]

    out = []
    for date_key, r in selected:
        pub_raw = r.get('published')
        published_short = format_short_date(pub_raw)
        out.append({
            'title': r.get('title'),
            'doi': r.get('doi'),
            'link': r.get('link'),
            'feed_title': r.get('feed_title'),
            'content': r.get('content'),
            'published': published_short,
            'raw': r,
        })
    return out


def export_db_parquet(out_dir: Path, tables: list | None = None):
    """Export selected tables from the configured SQLite DB into Parquet files

    Uses DuckDB's sqlite extension to read directly from the SQLite
    database file and writes each table to ``out_dir/db/{table}.parquet``.

    Args:
        out_dir (pathlib.Path): Base output directory (typically the build dir).
        tables (list[str] | None): List of table names to export. If omitted,
            exports the default set: articles, items, publications, articles_vec.
    """
    if tables is None:
        tables = ["articles", "items", "publications"]

    if not DB_FILE.exists():
        logger.debug("DB file %s does not exist; skipping parquet export", DB_FILE)
        return

    db_out = out_dir / "db"
    db_out.mkdir(parents=True, exist_ok=True)

    con = None
    try:
        con = duckdb.connect(database=":memory:")
        # Try to load sqlite extension which provides sqlite_scan; if not
        # available the COPY from sqlite_scan may fail and we'll log a warning.
        try:
            con.execute("INSTALL sqlite")
            con.execute("LOAD sqlite")
        except Exception:
            # ignore; extension may already be present
            pass

        for table in tables:
            dest = db_out / f"{table}.parquet"
            try:
                # Use sqlite_scan to read a table directly from the SQLite file
                sql = (
                    "COPY (SELECT * FROM sqlite_scan('" + str(DB_FILE) + "', '" + table + "')) "
                    "TO '" + str(dest) + "' (FORMAT PARQUET)"
                )
                con.execute(sql)
                logger.info("exported table %s -> %s", table, dest)
            except Exception as e:
                logger.warning("failed to export table %s: %s", table, e)
    except Exception as e:
        logger.warning("duckdb export failed: %s", e)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
