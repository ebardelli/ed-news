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

MODEL_NAME = config.DEFAULT_MODEL

BUILD_DIR = Path("build")
TEMPLATES_DIR = Path("templates")
STATIC_DIR = Path("static")
PLANET_FILE = config.PLANET_INI
DB_FILE = config.DB_PATH

logger = logging.getLogger("ednews.build")


def get_similar_articles_by_doi(conn, doi, top_n=5, model=MODEL_NAME, store_if_missing: bool = True):
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
    logger.info("done")


def read_articles(db_path: Path, limit: int = 30, days: int | None = None, publications: int | None = None):
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
