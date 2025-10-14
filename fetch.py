#!/usr/bin/env python3
"""Fetch RSS/Atom feeds listed in planet.json and save items to ednews.db.

Usage: python fetch.py

This script:
- reads feeds from planet.json
- fetches them in parallel using requests
- parses with feedparser
- stores items with simple deduplication (by guid/link/title+published)
"""
import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
import re
import html
import xml.etree.ElementTree as ET
import logging
import time


ROOT = Path(__file__).resolve().parent
PLANET = ROOT / "planet.json"
DB_PATH = ROOT / "ednews.db"

logger = logging.getLogger("ednews.fetch")


def load_feeds():
    with PLANET.open("r", encoding="utf-8") as f:
        data = json.load(f)
    feeds = data.get("feeds", {})
    # normalize to list of (key, feedurl)
    results = []
    for key, info in feeds.items():
        url = info.get("feed")
        if url:
            # support an optional feed-level publication_id (e.g. journal prefix like 10.1016/j.learninstruc)
            pub_id = info.get("publication_id")
            results.append((key, info.get("title"), url, pub_id))
    logger.debug("loaded %d feeds from %s", len(results), PLANET)
    return results


def init_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            feed_key TEXT,
            feed_title TEXT,
            guid TEXT,
            title TEXT,
            link TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT,
            UNIQUE(guid, link, title, published)
        )
        """
    )
    # Articles table: will be populated from fetched feed data (by DOI when available).
    # doi is treated as the unique identifier for an article.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT,
            title TEXT,
            authors TEXT,
            abstract TEXT,
            crossref_xml TEXT,
            fetched_at TEXT,
            UNIQUE(doi)
        )
        """
    )
    conn.commit()
    logger.debug("initialized database and ensured tables exist")
    # ensure article_doi column exists for older DBs
    cur.execute("PRAGMA table_info('items')")
    cols = [r[1] for r in cur.fetchall()]
    try:
        cur.execute(
            """
            CREATE VIEW IF NOT EXISTS combined_articles AS
            SELECT
                COALESCE(items.doi, articles.doi) AS doi,
                COALESCE(articles.title, items.title) AS title,
                items.link AS link,
                items.feed_title AS feed_title,
                COALESCE(articles.abstract, items.summary) AS content,
                COALESCE(items.published, articles.fetched_at, items.fetched_at) AS published,
                articles.authors AS authors
            FROM items
                JOIN articles ON articles.doi = items.doi

            where items.doi is not null
            """
        )
        conn.commit()
        logger.debug("created combined_articles view")
    except Exception:
        logger.debug("could not create combined_articles view (db may be locked or readonly)")


def fetch_feed(session, key, feed_title, url, publication_doi=None, timeout=20):
    """Fetch a feed URL and return parsed feed entries.

    Returns: list of dicts with fields matching DB columns.
    """
    logger.info("fetching feed %s (%s)", key, url)
    try:
        resp = session.get(url, timeout=timeout, headers={"User-Agent": "ed-news-fetcher/1.0"})
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as e:
        logger.warning("failed to fetch feed %s (%s): %s", key, url, e)
        return {"key": key, "title": feed_title, "url": url, "error": str(e), "entries": []}

    entries = []
    for e in parsed.entries:
        guid = e.get("id") or e.get("guid") or e.get("link") or e.get("title")
        title = e.get("title", "")
        link = e.get("link", "")
        published = e.get("published") or e.get("updated") or ""
        summary = e.get("summary", "")
        # keep the original parsed entry for richer metadata extraction later
        entries.append({
            "guid": guid,
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "_entry": e,
            # attach feed-level publication_id to each entry so save_entries can use it
            "_feed_publication_id": publication_doi,
        })

    logger.debug("parsed %d entries from feed %s", len(entries), key)
    # include any feed-level publication DOI so downstream saving logic can use it
    return {"key": key, "title": feed_title, "url": url, "publication_id": publication_doi, "error": None, "entries": entries}


def title_suitable_for_crossref_lookup(title: str) -> bool:
    """Heuristics to avoid CrossRef lookups for very generic or short titles.

    Returns True when title looks specific enough (length and not in a small blacklist).
    """
    if not title:
        return False
    t = title.strip()
    # too short
    if len(t) < 10:
        return False
    # if the title is purely generic like "Editorial", "Editorial Board", "Correction", skip
    blacklist = {"editorial", "editorial board", "correction", "corrections", "erratum", "letter to the editor", "front matter"}
    if t.lower() in blacklist:
        return False
    # avoid titles that are 1-3 words common phrases
    words = [w for w in re.split(r"\s+", t) if w]
    if len(words) <= 3:
        # if very short but contains punctuation or parentheses with year or DOIs, allow
        if re.search(r"\d{4}", t) or re.search(r"10\.\d{4,9}/", t):
            return True
        return False
    return True


def save_entries(conn, feed_key, feed_title, entries):
    cur = conn.cursor()
    inserted = 0
    logger.debug("saving %d entries for feed %s (%s)", len(entries), feed_key, feed_title)
    for e in entries:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO items
                (feed_key, feed_title, guid, title, link, published, summary, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feed_key,
                    feed_title,
                    e.get("guid"),
                    e.get("title"),
                    e.get("link"),
                    e.get("published") or datetime.now(timezone.utc).isoformat(),
                    e.get("summary"),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            if cur.rowcount:
                inserted += 1
                # get the inserted item's rowid
                item_rowid = cur.lastrowid
            # Attempt to extract richer article metadata and upsert into `articles` table.
            entry_obj = e.get("_entry") or {}
            doi = extract_doi_from_entry(entry_obj) or extract_doi_from_entry(e)

            # If DOI not present in entry, fetch the feed-level publication_id (DOI prefix)
            # but do NOT assign it as the entry DOI. We only use it as a preferred
            # prefix when performing CrossRef title lookups.
            pub_pid = None
            try:
                pub_pid = e.get("_feed_publication_id")
            except Exception:
                pub_pid = None

            # If still no DOI, attempt CrossRef title lookup for ScienceDirect links
            if not doi:
                link_val = (e.get("link") or "")
                # Only run title-lookup for ScienceDirect links and when the title looks specific enough
                if ("www.sciencedirect.com" in link_val or "sciencedirect.com" in link_val):
                    lookup_title = e.get("title") or (entry_obj.get("title") if isinstance(entry_obj, dict) else None)
                    if lookup_title and title_suitable_for_crossref_lookup(lookup_title):
                        try:
                            found = query_crossref_doi_by_title(lookup_title, preferred_publication_id=pub_pid)
                            if found:
                                logger.info("ScienceDirect title lookup found DOI %s for title: %s", found, lookup_title)
                                doi = found
                        except Exception:
                            logger.debug("CrossRef title lookup failed for ScienceDirect title: %s", lookup_title)

            if doi:
                doi = normalize_doi(doi)
                title = e.get("title") or (entry_obj.get("title") if isinstance(entry_obj, dict) else None)
                authors = extract_authors_from_entry(entry_obj) or extract_authors_from_entry(e)
                abstract = extract_abstract_from_entry(entry_obj) or extract_abstract_from_entry(e)
                # create a minimal article row (no CrossRef lookup) so items can link to it by DOI
                ensured = ensure_article_row(conn, doi, title=title, authors=authors, abstract=abstract)
                # attach article doi to the items row if available
                try:
                    if ensured and item_rowid:
                        cur.execute("UPDATE items SET doi = ? WHERE id = ?", (doi, item_rowid))
                except Exception:
                    logger.debug("failed to attach doi %s to item %s", doi, item_rowid)
        except Exception:
            # Ignore single-row errors; continue with others
            continue
    conn.commit()
    logger.info("saved %d new items for feed %s", inserted, feed_key)
    return inserted


def normalize_doi(doi: str) -> str | None:
    if not doi:
        return None
    doi = doi.strip()
    # remove common prefixes like doi: or https://doi.org/
    doi = re.sub(r"^(doi:\s*|https?://(dx\.)?doi\.org/)", "", doi, flags=re.IGNORECASE)
    doi = doi.strip()
    # drop URL query params or fragments if present
    doi = re.split(r"[?#]", doi, maxsplit=1)[0]
    # remove trailing punctuation commonly appended in text
    doi = doi.rstrip(" .;,)/]")
    # strip surrounding quotes or angle brackets
    doi = doi.strip('"\'<>[]()')

    # try to extract a DOI-like substring (starts with 10.<digits>/<non-space>)
    m = re.search(r"(10\.\d{4,9}/\S+)", doi)
    if m:
        core = m.group(1)
        # again strip any trailing punctuation from the captured core
        core = core.rstrip(" .;,)/]")
        core = core.strip('"\'<>[]()')
        return core

    # final simple check
    if "/" not in doi:
        # If the incoming string doesn't look like a DOI, try a CrossRef title lookup
        try:
            found = query_crossref_doi_by_title(doi)
            if found:
                # attempt to extract DOI-like core from the returned value
                m2 = re.search(r"(10\.\d{4,9}/\S+)", found)
                if m2:
                    return m2.group(1).rstrip(" .;,)/]").strip('"\'<>[]()')
                return found
        except Exception:
            logger.debug("CrossRef title lookup failed or returned no DOI for: %s", doi)
        return None


def query_crossref_doi_by_title(title: str, preferred_publication_id: str | None = None, timeout: int = 8) -> str | None:
    """Query CrossRef for a DOI using the publication title.

    Uses the `https://api.crossref.org/works?query.title={title}` endpoint and
    returns the first matching DOI if available.
    """
    if not title:
        return None
    try:
        headers = {"User-Agent": "ed-news-fetcher/1.0", "Accept": "application/json"}
        # request multiple rows so we can prefer DOIs matching the feed's publication id
        params = {"query.title": title, "rows": 20}
        logger.debug("CrossRef title lookup for title: %s", title)
        resp = requests.get("https://api.crossref.org/works", params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("message", {}).get("items", []) or []
        if not items:
            return None
        # If a preferred publication id (DOI prefix) is provided, prefer the first
        # CrossRef result whose DOI starts with that prefix. Otherwise return the
        # first result.
        if preferred_publication_id:
            pref = preferred_publication_id.rstrip().lower()
            for it in items:
                d = (it.get("DOI") or "").lower()
                if d.startswith(pref):
                    logger.info("CrossRef title lookup: selected DOI %s matching preferred_publication_id %s for title: %s", d, pref, title)
                    return d
        # fallback to first item
        doi = items[0].get("DOI")
        if doi:
            logger.info("CrossRef title lookup: found DOI %s for title: %s", doi, title)
            return doi
    except Exception as e:
        logger.debug("CrossRef title lookup error for '%s': %s", title, e)
    return None


def extract_doi_from_entry(entry) -> str | None:
    """Heuristics to find a DOI in the feedparser entry.

    Checks common places: 'doi' key, links, summary/content, and id.
    """
    # 1. direct field
    for key in ("doi", "dc:identifier", "doi"):
        v = entry.get(key)
        if v:
            d = normalize_doi(str(v))
            if d:
                return d

    # 2. links (some feeds include doi: in link or point to doi.org)
    for l in entry.get("links", []) or []:
        href = l.get("href")
        if not href:
            continue
        m = re.search(r"doi\.org/(10\.\d{4,9}/[^\s'\"]+)", href, flags=re.IGNORECASE)
        if m:
            return normalize_doi(m.group(1))

    # 3. id/guid
    idv = entry.get("id") or entry.get("guid")
    if idv:
        m = re.search(r"(10\.\d{4,9}/[^\s'\"]+)", str(idv))
        if m:
            return normalize_doi(m.group(1))

    # 4. summary/content
    text_candidates = []
    if entry.get("summary"):
        text_candidates.append(entry.get("summary"))
    for content in entry.get("content", []) or []:
        text_candidates.append(content.get("value") or "")

    for txt in text_candidates:
        if not txt:
            continue
        # strip HTML
        t = re.sub(r"<[^>]+>", " ", txt)
        m = re.search(r"(10\.\d{4,9}/[^\s'\"<>]+)", t)
        if m:
            return normalize_doi(m.group(1))

    return None


def extract_authors_from_entry(entry) -> str | None:
    """Return a serialized authors string (comma-separated) if present.

    Looks at 'author', 'authors', and dc:creator fields.
    """
    # feedparser often provides 'authors' as a list of dicts with 'name'
    authors = []
    if entry.get("authors"):
        for a in entry.get("authors"):
            if isinstance(a, dict):
                n = a.get("name")
            else:
                n = a
            if n:
                authors.append(str(n).strip())

    if not authors and entry.get("author"):
        authors = [entry.get("author")]

    # dc:creator may appear as a key
    if not authors and entry.get("dc_creator"):
        authors = [entry.get("dc_creator")]

    if authors:
        # normalize: unescape HTML entities and join
        cleaned = [html.unescape(a) for a in authors if a]
        return ", ".join(cleaned)
    return None


def extract_abstract_from_entry(entry) -> str | None:
    # prefer 'summary' then first content element
    abstract = entry.get("summary") or None
    if not abstract:
        for c in entry.get("content", []) or []:
            v = c.get("value")
            if v:
                abstract = v
                break
    if abstract:
        # strip surrounding HTML tags but preserve simple markup
        txt = re.sub(r"<[^>]+>", " ", abstract)
        return html.unescape(txt).strip()
    return None


def upsert_article(conn, doi: str, title: str | None, authors: str | None, abstract: str | None):
    if not doi:
        return False
    cur = conn.cursor()
    # check for cached CrossRef XML in the articles table
    cur.execute(
        "SELECT crossref_xml, authors, abstract, fetched_at FROM articles WHERE doi = ? LIMIT 1",
        (doi,),
    )
    row = cur.fetchone()
    crossref_raw = None
    if row:
        cached_raw, cached_authors, cached_abstract, cached_fetched = row
        if cached_raw:
            crossref_raw = cached_raw
            logger.debug("CrossRef cache hit for %s (fetched_at=%s)", doi, cached_fetched)
            # prefer cached authors/abstract when incoming values are missing
            if not authors and cached_authors:
                authors = cached_authors
            if not abstract and cached_abstract:
                abstract = cached_abstract
        else:
            logger.debug("articles row exists for %s but no crossref_xml cached", doi)

    # If we still don't have crossref_raw, attempt a network fetch to enrich
    if not crossref_raw:
        try:
            logger.debug("attempting CrossRef enrichment for DOI %s", doi)
            cr = fetch_crossref_metadata(doi)
            if cr:
                found = []
                if cr.get("authors"):
                    authors = cr.get("authors")
                    found.append("authors")
                if cr.get("abstract"):
                    abstract = cr.get("abstract")
                    found.append("abstract")
                crossref_raw = cr.get("raw")
                logger.info("CrossRef enrichment for %s found: %s", doi, ",".join(found) or "none")
        except Exception:
            logger.warning("CrossRef enrichment attempt failed for %s", doi)
    now = datetime.now(timezone.utc).isoformat()
    try:
        # Use INSERT OR REPLACE to update existing records by DOI.
        cur.execute(
            """
            INSERT INTO articles (doi, title, authors, abstract, crossref_xml, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(doi) DO UPDATE SET
                title=excluded.title,
                authors=excluded.authors,
                abstract=excluded.abstract,
                crossref_xml=COALESCE(excluded.crossref_xml, articles.crossref_xml),
                fetched_at=excluded.fetched_at
            """,
            (doi, title, authors, abstract, crossref_raw, now),
        )
        conn.commit()
        # return the article id
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        aid = row[0] if row and row[0] else None
        logger.debug("upserted article doi=%s id=%s", doi, aid)
        return aid
    except Exception:
        # Fallback to INSERT OR REPLACE for older SQLite versions without excluded
        try:
            # Fallback: preserve existing crossref_xml by using COALESCE with a subquery
            cur.execute(
                """
                INSERT OR REPLACE INTO articles (id, doi, title, authors, abstract, crossref_xml, fetched_at)
                VALUES (
                    (SELECT id FROM articles WHERE doi = ?), ?, ?, ?, ?, COALESCE(?, (SELECT crossref_xml FROM articles WHERE doi = ?)), ?
                )
                """,
                (doi, doi, title, authors, abstract, crossref_raw, doi, now),
            )
            conn.commit()
            cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
            row = cur.fetchone()
            aid = row[0] if row and row[0] else None
            return aid
        except Exception:
            return False


def ensure_article_row(conn, doi: str, title: str | None = None, authors: str | None = None, abstract: str | None = None) -> int | None:
    """Ensure an article row exists for `doi`. Insert minimal data if missing and
    return the article id. Does NOT call CrossRef.
    """
    cur = conn.cursor()
    try:
        cur.execute("INSERT OR IGNORE INTO articles (doi, title, authors, abstract, fetched_at) VALUES (?, ?, ?, ?, ?)", (doi, title, authors, abstract, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None
    except Exception:
        return None


def enrich_articles_from_crossref(conn, batch_size: int = 20, delay: float = 0.1):
    """Fetch CrossRef metadata for articles that don't have crossref_xml yet.

    Processes articles in batches and sleeps `delay` seconds between requests to
    avoid hammering CrossRef. Skips articles that already have `crossref_xml`.
    """
    cur = conn.cursor()
    cur.execute("SELECT articles.doi FROM articles join items on items.doi = articles.doi WHERE crossref_xml IS NULL OR crossref_xml = '' ORDER BY COALESCE(items.published, items.fetched_at, articles.fetched_at) DESC LIMIT ?", (batch_size,))
    rows = cur.fetchall()
    updated = 0
    for r in rows:
        doi = r[0]
        if not doi:
            continue
        try:
            cr = fetch_crossref_metadata(doi)
            if not cr:
                continue
            # update article with crossref data
            authors = cr.get("authors")
            abstract = cr.get("abstract")
            raw = cr.get("raw")
            cur.execute(
                "UPDATE articles SET authors = COALESCE(?, authors), abstract = COALESCE(?, abstract), crossref_xml = ? WHERE doi = ?",
                (authors, abstract, raw, doi),
            )
            conn.commit()
            updated += 1
            logger.info("enriched article %s with CrossRef data", doi)
        except Exception as e:
            logger.warning("failed to enrich doi %s: %s", doi, e)
        time.sleep(delay)
    return updated


def fetch_crossref_metadata(doi: str, timeout: int = 10) -> dict | None:
    """Fetch CrossRef UNIXREF XML for a DOI and extract authors and abstract.

    Returns: dict with optional keys 'authors' (comma-separated string) and
    'abstract' (string), or None on failure.
    """
    if not doi:
        return None
    url = f"http://dx.crossref.org/{doi}"
    headers = {"Accept": "application/vnd.crossref.unixref+xml", "User-Agent": "ed-news-fetcher/1.0"}
    try:
        logger.info("CrossRef lookup for DOI %s -> %s", doi, url)
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        raw_xml = resp.content.decode('utf-8', errors='replace')
        logger.debug("CrossRef response for %s: %d bytes", doi, len(raw_xml))
        # parse XML
        root = ET.fromstring(raw_xml)
    except Exception:
        logger.warning("CrossRef lookup failed for %s: %s", doi, sys.exc_info()[1])
        return None

    def localname(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    # extract abstract
    abstract = None
    for elem in root.iter():
        if localname(elem.tag).lower() == "abstract":
            text = "".join(elem.itertext()).strip()
            if text:
                abstract = text
                break

    # extract authors: look for surname/given_name pairs under person_name or for person_name-like parents
    authors_list = []
    for parent in root.iter():
        tag = localname(parent.tag).lower()
        if tag in ("person_name", "contributor", "name", "author", "creator", "person"):
            given = None
            surname = None
            collab = None
            for child in parent:
                ctag = localname(child.tag).lower()
                text = (child.text or "").strip()
                if ctag in ("given_name", "given", "givenname") and text:
                    given = text
                elif ctag in ("surname", "family_name", "family") and text:
                    surname = text
                elif ctag in ("collab", "organization", "org", "institution") and text:
                    collab = text
            if surname or given:
                authors_list.append(" ".join([p for p in (given, surname) if p]))
            elif collab:
                authors_list.append(collab)
            elif parent.text and parent.text.strip():
                authors_list.append(parent.text.strip())

    # Fallback: look for <surname> elements and pair with nearest given_name sibling
    if not authors_list:
        for surname in root.iter():
            if localname(surname.tag).lower() == "surname":
                stext = (surname.text or "").strip()
                if not stext:
                    continue
                parent = surname.getparent() if hasattr(surname, 'getparent') else None
                # try to find given_name among siblings
                given = None
                if parent is not None:
                    for child in list(parent):
                        if localname(child.tag).lower() in ("given_name", "given", "givenname"):
                            given = (child.text or "").strip()
                            break
                if given:
                    authors_list.append(f"{given} {stext}")
                else:
                    authors_list.append(stext)

    authors = None
    if authors_list:
        # deduplicate while preserving order
        seen = set()
        dedup = []
        for a in authors_list:
            if a and a not in seen:
                dedup.append(a)
                seen.add(a)
        authors = ", ".join(dedup)

    out = {k: v for k, v in (("authors", authors), ("abstract", abstract)) if v}
    out["raw"] = raw_xml
    return out


def create_combined_view(conn: sqlite3.Connection):
    """Create a combined_articles view that unifies articles and items.

    The view provides: title, link, feed_title, content, published, authors
    where article abstracts populate content and item summaries populate content.
    """
    cur = conn.cursor()
    # Build a joined view that links items to their article (if any) and
    # exposes a single row per item with article metadata preferred when available.
    cur.execute(
        """
        CREATE VIEW IF NOT EXISTS combined_articles AS
        SELECT
            COALESCE(items.article_doi, articles.doi) AS doi,
            COALESCE(articles.title, items.title) AS title,
            items.link AS link,
            items.feed_title AS feed_title,
            COALESCE(articles.abstract, items.summary) AS content,
            items.fetched_at AS published,
            articles.authors AS authors
        FROM items
        LEFT JOIN articles ON articles.doi = items.article_doi
        """
    )
    conn.commit()


def main():
    feeds = load_feeds()
    if not feeds:
        logger.error("No feeds found in planet.json")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    session = requests.Session()

    with ThreadPoolExecutor(max_workers=10) as ex:
        # feeds now have optional publication_doi in their tuple
        futures = {}
        for item in feeds:
            # item is (key, title, url, publication_doi)
            if len(item) == 4:
                key, title, url, publication_doi = item
            else:
                key, title, url = item
                publication_doi = None
            fut = ex.submit(fetch_feed, session, key, title, url, publication_doi)
            futures[fut] = (key, title, url, publication_doi)
        for fut in as_completed(futures):
            meta = futures[fut]
            try:
                res = fut.result()
            except Exception as exc:
                logger.error("Error fetching %s: %s", meta[2], exc)
                continue
            if res.get("error"):
                logger.warning("Failed: %s -> %s", meta[2], res["error"])
                continue
            count = save_entries(conn, res["key"], res["title"], res["entries"])
            logger.info("%s: fetched %d entries, inserted %d", res["key"], len(res["entries"]), count)

    # ensure the combined_articles view exists (used by build.py)
    try:
        create_combined_view(conn)
        logger.info("ensured combined_articles view exists")
    except Exception as e:
        logger.warning("failed to create combined_articles view: %s", e)

    # Enrich articles from CrossRef in bulk for those missing crossref_xml
    try:
        updated = enrich_articles_from_crossref(conn, batch_size=50, delay=0.05)
        logger.info("enriched %d articles from CrossRef", updated)
    except Exception as e:
        logger.warning("bulk CrossRef enrichment failed: %s", e)

    conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    main()