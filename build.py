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

MODEL_NAME = 'nomic-embed-text-v1.5'
def get_similar_articles_by_doi(conn, doi, top_n=5, model=MODEL_NAME, store_if_missing: bool = True):
    """
    Given an article DOI, return a list of up to `top_n` similar articles.

    Behavior:
    - Looks up the article `id` and content (title+abstract) by DOI.
    - If an embedding exists in `articles_vec` for that id, use it.
    - If missing and `store_if_missing` is True, generate the embedding locally,
      store it in `articles_vec`, and use it for the similarity search.

    Returns a list of dicts: [{"doi":..., "title":..., "abstract":..., "distance":...}, ...]
    """
    # Enable the sqlite-vec extension
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

    # Try to fetch existing embedding blob
    cur.execute("SELECT embedding FROM articles_vec WHERE rowid = ?", (article_id,))
    res = cur.fetchone()
    if res and res[0]:
        target_blob = res[0]
    else:
        # No embedding present for this article
        logger.debug("No embedding found for DOI %s (id=%s)", doi, article_id)
        return []

    # Query for similar articles using the target blob
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

    # Log a short summary
    logger.debug("Found %d similar articles for DOI %s (id=%s)", len(out_list), doi, article_id)
    for itm in out_list:
        logger.debug("  - %s (Distance: %.4f) Title: %s", itm['doi'], itm['distance'] if itm['distance'] is not None else 0.0, (itm['title'] or '')[:80])

    return out_list

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
            # get all articles published on the most recent 5 distinct days
            ctx["articles"] = read_articles(DB_FILE, days=5)
            logger.info("loaded %d articles from %s", len(ctx["articles"]), DB_FILE)
            # Attach similar articles for each article on the page if possible.
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
                            logger.debug("No DOI for article title=%s; will skip similarity. (link=%s)", art.get('title'), art.get('link'))
                            art["similar_articles"] = []
                            continue

                        logger.debug("Looking up similar articles for DOI=%s (source=%s, title=%s)", doi, doi_source, art.get('title'))
                        try:
                            sims = get_similar_articles_by_doi(conn, doi, top_n=5, store_if_missing=False)
                            if not sims:
                                logger.debug("No similar articles found for DOI=%s (maybe no embedding)", doi)
                                art["similar_articles"] = []
                            else:
                                logger.debug("Found %d similar articles for DOI=%s", len(sims), doi)
                                art["similar_articles"] = sims
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
    
    # Group articles by feed_title
    if "articles" in ctx:
        grouped_articles = {}
        for article in ctx["articles"]:
            publication = article.get("feed_title", "Unknown Publication")
            if publication not in grouped_articles:
                grouped_articles[publication] = []
            grouped_articles[publication].append(article)

        # Add grouped articles to context
        ctx["grouped_articles"] = grouped_articles
    else:
        ctx["grouped_articles"] = {}
    
    out_dir.mkdir(parents=True, exist_ok=True)
    render_templates(ctx, out_dir)
    copy_static(out_dir)
    logger.info("done")



def read_articles(db_path: Path, limit: int = 30, days: int | None = None):
    """Read articles from the `combined_articles` view.

    By default this returns up to `limit` most-recent articles. If `days` is
    provided, return all articles whose publish date (by calendar day) is in
    the most recent `days` distinct dates present in the view. This lets the
    caller request "all articles from the last N different publish days".

    The function will try to use SQLite DATE() functions to compute distinct
    publish dates. If that fails (for example, because published values are
    not parseable by SQLite's DATE()), it falls back to selecting the most
    recent `limit` rows and performing the date-distinct filtering in Python.
    """
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows = []
    # If days is provided, try to select articles where DATE(published) is in
    # the most recent `days` distinct dates. This uses a subquery to extract
    # the distinct dates then selects rows matching those dates.
    if days is not None:
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
            # Fall back to a simpler strategy below
            rows = []

    # If rows still empty (no days requested or SQL failed), fall back to the
    # original LIMIT-based query.
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
    # If `days` was requested but the SQL date selection failed and we fell
    # back to a LIMIT-based query above, implement the "last N distinct
    # publish dates" selection in Python here: group rows by calendar date
    # parsed from the `published` value, then include all rows whose date is
    # among the most recent `days` distinct dates.
    parsed_rows = []
    for r in rows:
        pub_raw = r.get('published')
        # try to parse to a date-only key for grouping (ISO YYYY-MM-DD)
        date_key = None
        if pub_raw is None:
            date_key = None
        else:
            # try several parsers in order
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
        # no special grouping requested; return rows up to `limit` as before
        selected = parsed_rows
    else:
        # collect distinct dates in order from newest to oldest
        distinct_dates = []
        for dk, _ in parsed_rows:
            if dk is None:
                continue
            if dk not in distinct_dates:
                distinct_dates.append(dk)
            if len(distinct_dates) >= days:
                break

        # include rows whose date_key is in the distinct_dates set
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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    build()