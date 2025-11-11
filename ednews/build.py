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
import json
import sqlite3
import sqlite_vec
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import logging
from . import config
import duckdb
import re

MODEL_NAME = config.DEFAULT_MODEL

# Build output directory
BUILD_DIR = Path("build")
# Package-relative template/static dirs: when packaged the templates and static
# assets live inside the `ednews` package. Use Path(__file__).parent to locate
# them; fallback to top-level paths for backward compatibility in dev checkouts.
PKG_DIR = Path(__file__).parent
TEMPLATES_DIR = (
    (PKG_DIR / "templates") if (PKG_DIR / "templates").exists() else Path("templates")
)
STATIC_DIR = (PKG_DIR / "static") if (PKG_DIR / "static").exists() else Path("static")
# Use the JSON research file
PLANET_FILE = config.RESEARCH_JSON
DB_FILE = config.DB_PATH

logger = logging.getLogger("ednews.build")


def item_has_content(item: dict) -> bool:
    """Return True if the given feed/article-like item has any usable content.

    We treat an item as having content if at least one of title, link or
    content/abstract is present and non-empty after stripping.
    """
    if not item or not isinstance(item, dict):
        return False
    title = (item.get("title") or "").strip()
    link = (item.get("link") or "").strip()
    # content may be stored under several keys depending on origin
    content = (item.get("content") or item.get("abstract") or "").strip()
    return bool(title or link or content)


def _make_rss_description(item: dict) -> str:
    """Return a compact HTML description string for RSS from an item dict.

    The function prefers abstract over content, includes an optional
    source line, and renders similar items as a bulleted HTML list.
    This is a module-level helper to ensure it's available anywhere in the
    build process.
    """
    # Build HTML blocks for the RSS description so parts render with clear
    # separation in RSS readers (HTML is placed inside the CDATA section).
    parts = []
    src = item.get("source")
    if src:
        parts.append(f"<p><strong>Source:</strong> {src}</p>")

    main = item.get("abstract") or item.get("content") or ""
    main = str(main).strip()
    if main:
        # Wrap the main text in a paragraph to ensure spacing/rendering
        parts.append(f"<p>{main}</p>")

    sims = item.get("similar_headlines") or []
    sims_li = []
    for s in sims:
        label = (s.get("title") or s.get("text") or "").strip()
        if not label:
            continue
        if s.get("link"):
            sims_li.append(f"<li><a href=\"{s.get('link')}\">{label}</a></li>")
        else:
            sims_li.append(f"<li>{label}</li>")
    if sims_li:
        # Put the 'Related' heading in its own paragraph and then the list
        parts.append(
            "<p><strong>Related:</strong></p>\n<ul>" + "\n".join(sims_li) + "</ul>"
        )

    # Join blocks with a single newline; each block contains its own HTML
    # and will render as separate paragraphs in RSS clients.
    return "\n".join(parts).strip()


def _strip_img_tags(html: str) -> str:
    """Return the input HTML with any <img ...> tags removed.

    This is a small, conservative sanitizer used at build time to ensure
    embedded images from upstream feeds aren't rendered into the static
    site. It intentionally only strips <img> tags.
    """
    if not html:
        return html or ""
    try:
        # Remove img tags (case-insensitive). Keep other markup intact.
        return re.sub(r"<img\b[^>]*>", "", str(html), flags=re.IGNORECASE)
    except Exception:
        return str(html)


def get_similar_articles_by_doi(
    conn, doi, top_n=5, model=MODEL_NAME, store_if_missing: bool = True
):
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

    cur.execute(
        "SELECT id, title, abstract FROM articles WHERE doi = ? LIMIT 1", (doi,)
    )
    row = cur.fetchone()
    if not row:
        logger.debug("No article found with DOI: %s", doi)
        return []

    article_id, title, abstract = row
    title = title or ""
    abstract = abstract or ""
    combined = title.strip()
    if abstract.strip():
        combined = (
            combined + "\n\n" + abstract.strip() if combined else abstract.strip()
        )

    cur.execute("SELECT embedding FROM articles_vec WHERE rowid = ?", (article_id,))
    res = cur.fetchone()
    if res and res[0]:
        target_blob = res[0]
    else:
        logger.debug("No embedding found for DOI %s (id=%s)", doi, article_id)
        return []

    q = """
    SELECT A.doi, A.title, A.abstract, vec_distance_cosine(V.embedding, ?) AS distance
    FROM articles AS A, articles_vec AS V
    WHERE A.id = V.rowid AND A.id != ?
    ORDER BY distance ASC
    LIMIT ?
    """

    results = cur.execute(q, (target_blob, article_id, top_n)).fetchall()

    out_list = []
    for doi_r, title_r, abstract_r, distance in results:
        out_list.append(
            {
                "doi": doi_r,
                "title": title_r,
                "abstract": abstract_r,
                "distance": float(distance) if distance is not None else None,
            }
        )

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
    site_title = cfg.get(
        "global", "title", fallback="Latest Research Articles in Education"
    )
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

    The function collects site metadata, loads recent articles from the
    configured DB (by default the most recent 20 articles, see
    :func:`read_articles`), computes similar-article suggestions, renders
    templates, copies static assets and the DB into the output directory.

    Important behaviour: the build always requests the most recent 20
    articles. When more than 20 articles share the same published DATE as the
    20th most recent article, all articles with that DATE are included so the
    site doesn't arbitrarily truncate a day's publications.

    Args:
        out_dir (pathlib.Path): Destination directory for the built static site.
    """
    logger.info("building static site into %s", out_dir)
    # Load site metadata from JSON or INI planet files.
    if PLANET_FILE.exists():
        if PLANET_FILE.suffix == ".json":
            try:
                data = json.loads(PLANET_FILE.read_text(encoding="utf-8"))
            except Exception:
                logger.exception("failed to parse JSON planet file: %s", PLANET_FILE)
                ctx = {"title": "Latest Research Articles in Education", "feeds": []}
            else:
                title = data.get("title", "Latest Research Articles in Education")
                feeds = []
                for key, info in (data.get("feeds", {}) or {}).items():
                    feeds.append(
                        {
                            "id": key,
                            "title": info.get("title", key),
                            "link": info.get("link", ""),
                            "feed": info.get("feed", ""),
                        }
                    )
                ctx = {"title": title, "feeds": feeds}
        else:
            ctx = read_planet(PLANET_FILE)
    else:
        ctx = {"title": "Latest Research Articles in Education", "feeds": []}
    try:
        try:
            from zoneinfo import ZoneInfo

            tz = ZoneInfo("America/Los_Angeles")
            ctx["build_time"] = datetime.now(tz).strftime("%a, %d %b %Y %H:%M %Z")
        except Exception:
            ctx["build_time"] = (
                datetime.now().astimezone().strftime("%a, %d %b %Y %H:%M %Z")
            )
    except Exception:
        ctx["build_time"] = datetime.now().strftime("%a, %d %b %Y %H:%M")

    if DB_FILE.exists():
        try:
            # Load the most recent articles according to the configured default.
            # If more than `ARTICLES_DEFAULT_LIMIT` articles share the same
            # published DATE as the Nth article, `read_articles` will include them
            # as well so the site doesn't arbitrarily truncate a day's publications.
            ctx["articles"] = read_articles(
                DB_FILE, limit=config.ARTICLES_DEFAULT_LIMIT
            )
            logger.info("loaded %d articles from %s", len(ctx["articles"]), DB_FILE)
            if get_similar_articles_by_doi and ctx.get("articles"):
                try:
                    conn = sqlite3.connect(str(DB_FILE))
                    for art in ctx["articles"]:
                        doi = art.get("doi")
                        raw = art.get("raw") or {}
                        doi_source = None
                        if doi:
                            doi_source = "top-level"
                        elif isinstance(raw, dict) and raw.get("doi"):
                            doi = raw.get("doi")
                            doi_source = "raw"
                        else:
                            link = art.get("link")
                            if isinstance(link, str) and link.startswith(
                                "https://doi.org/"
                            ):
                                doi = link[len("https://doi.org/") :]
                                doi_source = "link"

                        if not doi:
                            art["similar_articles"] = []
                            continue

                        try:
                            sims = get_similar_articles_by_doi(
                                conn, doi, top_n=5, store_if_missing=False
                            )
                            art["similar_articles"] = sims or []
                        except Exception as e:
                            logger.exception(
                                "Error computing similar articles for DOI=%s: %s",
                                doi,
                                e,
                            )
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

    # Load recent news headlines from the headlines table if present
    try:
        # Use the configured headlines default limit; fall back to 20 when not set.
        ctx["news_headlines"] = read_news_headlines(
            DB_FILE, limit=getattr(config, "HEADLINES_DEFAULT_LIMIT", 20)
        )
    except Exception:
        ctx["news_headlines"] = []

    # Compute related headlines using stored embeddings (headlines_vec).
    try:
        from . import embeddings as _emb

        # Ensure the headlines_vec virtual table exists (no-op if not supported)
        try:
            conn_tmp = sqlite3.connect(str(DB_FILE))
            _emb.create_headlines_vec(conn_tmp)
        finally:
            try:
                conn_tmp.close()
            except Exception:
                pass

        if ctx.get("news_headlines"):
            try:
                conn_sim = sqlite3.connect(str(DB_FILE))
                for nh in ctx["news_headlines"]:
                    nid = nh.get("id")
                    if not nid:
                        nh["similar_headlines"] = []
                        continue
                    try:
                        sims = _emb.find_similar_headlines_by_rowid(
                            conn_sim, nid, top_n=5
                        )
                        nh["similar_headlines"] = sims or []
                    except Exception:
                        logger.exception(
                            "Error computing similar headlines for id=%s", nid
                        )
                        nh["similar_headlines"] = []
            finally:
                try:
                    conn_sim.close()
                except Exception:
                    pass
    except Exception:
        # If embeddings backend not available, leave similar_headlines empty
        try:
            for nh in ctx.get("news_headlines") or []:
                nh["similar_headlines"] = []
        except Exception:
            pass

    # Log headline embedding/stats: total headlines, how many have similar suggestions,
    # and how many embeddings exist in the headlines_vec table (if available).
    try:
        total_headlines = len(ctx.get("news_headlines") or [])
        with_emb_suggestions = sum(
            1 for nh in (ctx.get("news_headlines") or []) if nh.get("similar_headlines")
        )
        emb_count = None
        try:
            conn_stat = sqlite3.connect(str(DB_FILE))
            cur_stat = conn_stat.cursor()
            cur_stat.execute("SELECT COUNT(rowid) FROM headlines_vec")
            row = cur_stat.fetchone()
            emb_count = row[0] if row and row[0] is not None else 0
        except Exception:
            emb_count = None
        finally:
            try:
                if conn_stat:
                    conn_stat.close()
            except Exception:
                pass
        logger.info(
            "headlines: total=%d with_suggestions=%d embeddings=%s",
            total_headlines,
            with_emb_suggestions,
            str(emb_count),
        )
    except Exception:
        logger.exception("Failed to compute headline build stats")

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
    # Expose feed links/titles to templates so index.html can render feed links
    try:
        ctx["feed_links"] = {
            "combined": {
                "title": getattr(config, "FEED_TITLE_COMBINED", "Ed News"),
                "href": "index.rss",
            },
            "headlines": {
                "title": getattr(config, "FEED_TITLE_HEADLINES", "Ed Headlines"),
                "href": "headlines.rss",
            },
            "articles": {
                "title": getattr(config, "FEED_TITLE_ARTICLES", "Ed Articles"),
                "href": "articles.rss",
            },
        }
    except Exception:
        ctx["feed_links"] = {}

    # Render standard templates (index.html, index.rss, etc.)
    render_templates(ctx, out_dir)

    # Additionally build separate RSS feeds:
    # - index.rss (combined): latest 40 items from articles + headlines
    # - articles.rss: latest HEADLINES_DEFAULT_LIMIT (articles limit is ARTICLES_DEFAULT_LIMIT)
    # - headlines.rss: latest HEADLINES_DEFAULT_LIMIT
    try:
        env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
        # Prefer dedicated templates for articles/headlines when available
        try:
            tpl_articles = env.get_template("articles.rss.jinja2")
        except Exception:
            tpl_articles = env.get_template("index.rss.jinja2")
        try:
            tpl_headlines = env.get_template("headlines.rss.jinja2")
        except Exception:
            tpl_headlines = env.get_template("index.rss.jinja2")
        # Use index template for combined feed
        tpl_index = env.get_template("index.rss.jinja2")

        # Use the module-level _make_rss_description helper to prepare descriptions

        # Prepare metadata for feeds: use configured titles/links if present
        site_link = getattr(config, "FEED_SITE_LINK", "https://ebardelli.com/ed-news/")
        feed_meta_combined = {
            "title": getattr(config, "FEED_TITLE_COMBINED", "Latest Education News"),
            "link": site_link,
            "description": getattr(
                config, "FEED_TITLE_COMBINED", "Latest Education News"
            ),
            "last_build_date": ctx.get("build_time"),
            "pub_date": ctx.get("build_time"),
        }
        feed_meta_articles = {
            "title": getattr(
                config, "FEED_TITLE_ARTICLES", "Latest Education Articles"
            ),
            "link": site_link,
            "description": getattr(
                config, "FEED_TITLE_ARTICLES", "Latest Education Articles"
            ),
            "last_build_date": ctx.get("build_time"),
            "pub_date": ctx.get("build_time"),
        }
        feed_meta_headlines = {
            "title": getattr(
                config, "FEED_TITLE_HEADLINES", "Latest Education Headlines"
            ),
            "link": site_link,
            "description": getattr(
                config, "FEED_TITLE_HEADLINES", "Latest Education Headlines"
            ),
            "last_build_date": ctx.get("build_time"),
            "pub_date": ctx.get("build_time"),
        }

        # Articles-only feed
        articles_limit = getattr(config, "ARTICLES_DEFAULT_LIMIT", 20)
        # Filter out entirely empty/meaningless items before rendering
        # Ensure article items include optional `source` and `similar_headlines` keys
        articles_items = []
        for a in ctx.get("articles") or []:
            if not item_has_content(a):
                continue
            it = dict(a)
            # Prefer an explicit source field if present, else use feed title
            src = (
                it.get("source")
                or it.get("feed_title")
                or (it.get("raw", {}) or {}).get("source")
            )
            if src:
                it["source"] = src
            # Convert similar_articles (from article embeddings) to a generic similar_headlines shape
            sims = it.get("similar_articles") or []
            sim_items = []
            for s in sims:
                # s may contain doi, title, abstract, distance
                sim_items.append(
                    {
                        "title": s.get("title"),
                        "text": s.get("abstract"),
                        "link": (
                            ("https://doi.org/" + s.get("doi"))
                            if s.get("doi")
                            else None
                        ),
                        "distance": s.get("distance"),
                    }
                )
            it["similar_headlines"] = sim_items
            # Prepare rss_description in Python so templates can render it directly
            try:
                it["rss_description"] = _make_rss_description(it)
            except Exception:
                it["rss_description"] = ""
            articles_items.append(it)
        articles_items = articles_items[:articles_limit]
        # Ensure each item has a hashed guid for de-duplication
        import hashlib

        def make_guid_for_article(a):
            # Prefer link, else title+published+content
            key = a.get("link") or (
                str(a.get("title") or "")
                + "|"
                + str(a.get("published") or "")
                + "|"
                + str(a.get("content") or "")
            )
            return hashlib.sha1(key.encode("utf-8")).hexdigest()

        for a in articles_items:
            if not a.get("guid"):
                a["guid"] = make_guid_for_article(a)

        articles_ctx = {**feed_meta_articles, "articles": articles_items}
        (out_dir / "articles.rss").write_text(
            tpl_articles.render(articles_ctx), encoding="utf-8"
        )
        logger.info("wrote %s", out_dir / "articles.rss")

        # Headlines-only feed: map headlines into article-shaped dicts
        headlines_limit = getattr(config, "HEADLINES_DEFAULT_LIMIT", 20)
        headlines_raw = ctx.get("news_headlines") or []

        # Convert headline rows to the expected keys used by the RSS template
        def headline_to_item(h):
            item = {
                "title": h.get("title") or "Untitled",
                "link": h.get("link") or "",
                "content": h.get("text") or "",
                "abstract": None,
                "published": h.get("published") or None,
                "source": h.get("source") or None,
                "similar_headlines": h.get("similar_headlines") or [],
            }
            try:
                item["rss_description"] = _make_rss_description(item)
            except Exception:
                item["rss_description"] = ""
            return item

        headlines_items = [headline_to_item(h) for h in headlines_raw]
        # Filter headlines for meaningful content and apply limit afterwards
        headlines_items = [h for h in headlines_items if item_has_content(h)][
            :headlines_limit
        ]

        def make_guid_for_headline(h):
            key = h.get("link") or (
                str(h.get("title") or "")
                + "|"
                + str(h.get("published") or "")
                + "|"
                + str(h.get("content") or "")
            )
            return hashlib.sha1(key.encode("utf-8")).hexdigest()

        for h in headlines_items:
            if not h.get("guid"):
                h["guid"] = make_guid_for_headline(h)

        headlines_ctx = {**feed_meta_headlines, "articles": headlines_items}
        (out_dir / "headlines.rss").write_text(
            tpl_headlines.render(headlines_ctx), encoding="utf-8"
        )
        logger.info("wrote %s", out_dir / "headlines.rss")

        # Combined feed: merge articles and headlines, parse/normalize published
        # datetimes for stable ordering and take up to combined_limit items.
        combined_limit = 40
        merged = []

        import re

        def parse_datetime_value(val):
            """Return a timezone-aware datetime for sorting. On failure return a very old datetime."""
            if not val:
                return datetime(1970, 1, 1, tzinfo=timezone.utc)
            if isinstance(val, datetime):
                dt = val
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            s = str(val)
            # Try email-style date parsing
            try:
                dt = parsedate_to_datetime(s)
                if dt is not None and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
            # Try ISO-like parsing
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ):
                try:
                    dt = datetime.strptime(s, fmt)
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    continue
            m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
            if m:
                try:
                    dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                    dt = dt.replace(tzinfo=timezone.utc)
                    return dt
                except Exception:
                    pass
            return datetime(1970, 1, 1, tzinfo=timezone.utc)

        # Normalize articles and headlines into items with a sortable 'published_dt'
        def make_rss_description(item: dict) -> str:
            """Return a compact HTML description string for RSS from an item dict.

            The function prefers abstract over content, includes an optional
            source line, and renders similar items as a bulleted HTML list.
            """
            parts = []
            src = item.get("source")
            if src:
                parts.append(f"<strong>Source:</strong> {src}")

            main = item.get("abstract") or item.get("content") or ""
            main = str(main).strip()
            if main:
                parts.append(main)

            sims = item.get("similar_headlines") or []
            sims_li = []
            for s in sims:
                label = (s.get("title") or s.get("text") or "").strip()
                if not label:
                    continue
                if s.get("link"):
                    sims_li.append(f"<li><a href=\"{s.get('link')}\">{label}</a></li>")
                else:
                    sims_li.append(f"<li>{label}</li>")
            if sims_li:
                parts.append(
                    "<strong>Related:</strong>\n<ul>" + "\n".join(sims_li) + "</ul>"
                )

            return "\n\n".join(parts).strip()

        def norm_article(a):
            raw_published = (
                a.get("raw", {}).get("published")
                if a.get("raw")
                else a.get("published")
            )
            pd = parse_datetime_value(raw_published)
            # Build a normalized item shape including source and similar_headlines
            item = {
                "title": a.get("title"),
                "link": a.get("link"),
                "content": a.get("content") or a.get("abstract") or "",
                "abstract": a.get("abstract"),
                "published": a.get("published"),
                "published_dt": pd,
                "source": a.get("source")
                or a.get("feed_title")
                or (a.get("raw") or {}).get("source"),
                # ensure similar_headlines is present and in the expected shape
                "similar_headlines": a.get("similar_headlines")
                or a.get("similar_articles")
                or [],
            }
            item["rss_description"] = make_rss_description(item)
            return item

        def norm_headline(h):
            raw_published = (
                h.get("raw", {}).get("published")
                if h.get("raw")
                else h.get("published")
            )
            pd = parse_datetime_value(raw_published)
            item = {
                "title": h.get("title"),
                "link": h.get("link"),
                "content": h.get("text") or "",
                "abstract": None,
                "published": h.get("published"),
                "published_dt": pd,
                "source": h.get("source") or None,
                "similar_headlines": h.get("similar_headlines") or [],
            }
            item["rss_description"] = make_rss_description(item)
            return item

        for a in ctx.get("articles") or []:
            na = norm_article(a)
            if item_has_content(na):
                merged.append(na)
        for h in ctx.get("news_headlines") or []:
            nh = norm_headline(h)
            if item_has_content(nh):
                merged.append(nh)

        # Sort by parsed published datetime descending
        try:
            merged.sort(
                key=lambda x: x.get("published_dt")
                or datetime(1970, 1, 1, tzinfo=timezone.utc),
                reverse=True,
            )
        except Exception:
            pass

        combined_items = merged[:combined_limit]
        # Add GUIDs to combined items if missing
        for it in combined_items:
            if not it.get("guid"):
                key = it.get("link") or (
                    str(it.get("title") or "")
                    + "|"
                    + str(it.get("published") or "")
                    + "|"
                    + str(it.get("content") or "")
                )
                it["guid"] = hashlib.sha1(str(key).encode("utf-8")).hexdigest()

        combined_ctx = {**feed_meta_combined, "articles": combined_items}
        (out_dir / "index.rss").write_text(
            tpl_index.render(combined_ctx), encoding="utf-8"
        )
        logger.info("wrote %s", out_dir / "index.rss")
    except Exception:
        logger.exception("Failed to render additional RSS feeds")
    copy_static(out_dir)
    # export selected DB tables to parquet files under build/db/
    try:
        export_db_parquet(out_dir)
    except Exception:
        logger.exception("export_db_parquet failed")
    logger.info("done")


def read_articles(
    db_path: Path,
    limit: int = config.ARTICLES_DEFAULT_LIMIT,
    days: int | None = None,
    publications: int | None = None,
):
    """Read recent articles from the ``combined_articles`` view in the DB.

    The function supports several retrieval modes:
    * If ``publications`` is provided, returns all articles from the latest
      N publications (one date per publication).
    * Else if ``days`` is provided, returns articles whose ``DATE(published)``
      is in the latest ``days`` distinct dates.
    * Otherwise returns the most recent ``limit`` articles. When using the
      fallback ``limit`` mode (i.e. ``publications`` and ``days`` are not
      provided), the function will expand the selection so that if the Nth
      most recent article (ordered by ``published`` desc) shares its
      DATE(published) with additional articles, those articles are also
      included. This prevents arbitrarily truncating all articles published
      on the same date as the Nth article.

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
            cur.execute(
                "SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE published IS NOT NULL AND feed_title IS NOT NULL"
            )
            all_rows = [dict(r) for r in cur.fetchall()]

            if all_rows:
                latest_per_feed = {}
                for r in all_rows:
                    ft = r.get("feed_title")
                    pub = r.get("published") or ""
                    if ft not in latest_per_feed or (pub and pub > latest_per_feed[ft]):
                        latest_per_feed[ft] = pub

                sorted_feeds = sorted(
                    latest_per_feed.items(), key=lambda kv: kv[1], reverse=True
                )
                top_feeds = [ft for ft, _ in sorted_feeds[:publications]]

                feeds_latest_date = {
                    ft: (latest_per_feed[ft][:10] if latest_per_feed[ft] else None)
                    for ft in top_feeds
                }

                filtered = []
                for r in all_rows:
                    ft = r.get("feed_title")
                    if ft in feeds_latest_date and feeds_latest_date[ft]:
                        if (r.get("published") or "")[:10] == feeds_latest_date[ft]:
                            filtered.append(r)

                rows = sorted(
                    filtered, key=lambda r: r.get("published") or "", reverse=True
                )
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
            # To avoid arbitrarily truncating a day's publications, compute
            # the DATE(published) of the Nth most recent article and include
            # all articles whose DATE(published) is on or after that date.
            # If this fails for any reason, fall back to a simple LIMIT query.
            cur.execute(
                "SELECT DATE(published) as d FROM combined_articles WHERE published IS NOT NULL ORDER BY published DESC LIMIT 1 OFFSET ?",
                (max(0, limit - 1),),
            )
            row = cur.fetchone()
            if row and row[0]:
                nth_date = row[0]
                # First fetch the top-N articles ordered by published DESC
                cur.execute(
                    "SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE published IS NOT NULL ORDER BY published DESC LIMIT ?",
                    (limit,),
                )
                top_rows = [dict(r) for r in cur.fetchall()]

                # Then fetch all articles that share the same DATE(published) as the Nth article
                cur.execute(
                    "SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE DATE(published) = ? ORDER BY published DESC",
                    (nth_date,),
                )
                same_date_rows = [dict(r) for r in cur.fetchall()]

                # Merge top_rows and same_date_rows while preserving order and avoiding duplicates
                seen = set()
                merged = []
                for r in top_rows:
                    key = (r.get("doi"), r.get("link"))
                    if key not in seen:
                        seen.add(key)
                        merged.append(r)
                for r in same_date_rows:
                    key = (r.get("doi"), r.get("link"))
                    if key not in seen:
                        seen.add(key)
                        merged.append(r)

                # Enforce a hard cap to avoid pathological expansions when many
                # articles share the same DATE(published). The configured cap is
                # limit + config.ARTICLES_MAX_SAME_DATE_EXTRA.
                max_allowed = limit + getattr(
                    config, "ARTICLES_MAX_SAME_DATE_EXTRA", 200
                )
                if len(merged) > max_allowed:
                    logger.warning(
                        "merged articles (%d) exceed max_allowed (%d); truncating to max_allowed",
                        len(merged),
                        max_allowed,
                    )
                    merged = merged[:max_allowed]

                rows = merged
            else:
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
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
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
        pub_raw = r.get("published")
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
                            for fmt in (
                                "%Y-%m-%dT%H:%M:%S.%f",
                                "%Y-%m-%dT%H:%M:%S",
                                "%Y-%m-%d %H:%M:%S",
                                "%Y-%m-%d",
                            ):
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

        # Default any missing/parse-failed dates to 2020-01-01 so undated articles
        # are treated as old and appear after newer items when selecting recent ones.
        if date_key is None:
            date_key = "2020-01-01"
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
                    cur.execute(
                        "SELECT doi, title, link, feed_title, content, published, authors FROM combined_articles WHERE published IS NOT NULL AND feed_title IS NOT NULL"
                    )
                    all_rows = [dict(r) for r in cur.fetchall()]

                    if not all_rows:
                        rows = []
                    else:
                        # 2) Compute latest published (string compare of timestamp) per feed_title
                        latest_per_feed = {}
                        for r in all_rows:
                            ft = r.get("feed_title")
                            pub = r.get("published") or ""
                            # use raw string compare on ISO-like timestamps; ensure longer is greater
                            if ft not in latest_per_feed or (
                                pub and pub > latest_per_feed[ft]
                            ):
                                latest_per_feed[ft] = pub

                        # 3) Sort feeds by latest published desc and take top-N
                        sorted_feeds = sorted(
                            latest_per_feed.items(), key=lambda kv: kv[1], reverse=True
                        )
                        top_feeds = [ft for ft, _ in sorted_feeds[:publications]]

                        # 4) For each top feed, compute YYYY-MM-DD latest date and filter rows
                        feeds_latest_date = {
                            ft: (
                                latest_per_feed[ft][:10]
                                if latest_per_feed[ft]
                                else None
                            )
                            for ft in top_feeds
                        }

                        filtered = []
                        for r in all_rows:
                            ft = r.get("feed_title")
                            if ft in feeds_latest_date and feeds_latest_date[ft]:
                                if (r.get("published") or "")[:10] == feeds_latest_date[
                                    ft
                                ]:
                                    filtered.append(r)

                        # order by published desc
                        rows = sorted(
                            filtered,
                            key=lambda r: r.get("published") or "",
                            reverse=True,
                        )
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
        pub_raw = r.get("published")
        published_short = format_short_date(pub_raw)
        out.append(
            {
                "title": r.get("title"),
                "doi": r.get("doi"),
                "link": r.get("link"),
                "feed_title": r.get("feed_title"),
                "content": _strip_img_tags(r.get("content")),
                "published": published_short,
                "raw": r,
            }
        )
    return out


def read_news_headlines(db_path: Path, limit: int = None):
    """Read the most recent news headlines from the `headlines` table.

    Returns a list of dicts with keys: title, link, text, published (short string).
    """
    if not db_path.exists():
        return []
    if limit is None:
        try:
            from . import config as _cfg

            limit = getattr(_cfg, "HEADLINES_DEFAULT_LIMIT", 8)
        except Exception:
            limit = 8
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    try:
        # fetch a reasonable number of recent rows; include `id` so callers can
        # reference the headlines rowid when looking up headline embeddings.
        cur.execute(
            "SELECT id, title, link, text, published, first_seen FROM headlines"
        )
        rows = [dict(r) for r in cur.fetchall()]
    except Exception:
        rows = []
    finally:
        try:
            conn.close()
        except Exception:
            pass

    def parse_dt(val):
        if not val:
            return None
        s = str(val).strip()
        # Try ISO first
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            pass
            # Try email utils parsing
            try:
                dt = parsedate_to_datetime(s)
                if dt is not None and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass
        # Try common ISO-ish formats
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(s, fmt)
                dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                continue
        # Try to extract a YYYY-MM-DD substring
        import re

        m = re.search(r"(\d{4}-\d{2}-\d{2})", s)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
                dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    # attach a sortable date key to each row; default missing dates to 2020-01-01
    enriched = []
    for r in rows:
        pub = r.get("published")
        first = r.get("first_seen")
        dt = parse_dt(pub) or parse_dt(first)
        if dt is None:
            # default to an old date when no published/first_seen available
            dt = datetime(2020, 1, 1, tzinfo=timezone.utc)
        enriched.append((dt, r))

    # sort by datetime (None values go last)
    enriched.sort(key=lambda x: (x[0] is None, x[0]), reverse=True)

    # If we have no rows, return empty
    if not enriched:
        return []

    # Determine the Nth date and include all headlines on or after that date
    if len(enriched) >= limit:
        nth_dt = enriched[limit - 1][0]
        try:
            nth_date = nth_dt.date().isoformat()
        except Exception:
            nth_date = None
    else:
        nth_date = None

    if nth_date:
        # include all rows whose DATE(published) is >= nth_date (preserving order)
        selected_rows = [
            r
            for dt, r in enriched
            if (dt is not None and dt.date().isoformat() >= nth_date)
        ]
    else:
        selected_rows = [r for _, r in enriched[:limit]]

    selected = selected_rows

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
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
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
    for r in selected:
        out.append(
            {
                "id": r.get("id"),
                "title": r.get("title"),
                "link": r.get("link"),
                "text": _strip_img_tags(r.get("text")),
                "published": format_short_date(
                    r.get("published") or r.get("first_seen")
                ),
            }
        )
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
        # include a headlines parquet export which sources from the news_items table
        tables = ["articles", "items", "publications", "headlines"]

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
                # Map logical 'headlines' dest to the headlines table in SQLite
                src_table = "headlines" if table == "headlines" else table
                # Use sqlite_scan to read a table directly from the SQLite file
                sql = (
                    "COPY (SELECT * FROM sqlite_scan('"
                    + str(DB_FILE)
                    + "', '"
                    + src_table
                    + "')) "
                    "TO '" + str(dest) + "' (FORMAT PARQUET)"
                )
                con.execute(sql)
                logger.info("exported table %s (src=%s) -> %s", table, src_table, dest)
            except Exception as e:
                logger.warning(
                    "failed to export table %s (src=%s): %s", table, src_table, e
                )
    except Exception as e:
        logger.warning("duckdb export failed: %s", e)
    finally:
        try:
            if con is not None:
                con.close()
        except Exception:
            pass
