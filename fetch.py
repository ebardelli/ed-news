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
import os
import argparse


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
            # new: optional explicit ISSN field on feeds; prefer this when present
            issn = info.get("issn")
            results.append((key, info.get("title"), url, pub_id, issn))
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
            publication_id TEXT,
            issn TEXT,
            published TEXT,
            fetched_at TEXT,
            UNIQUE(doi)
        )
        """
    )
    # Publications table: map a publication identifier (e.g. DOI prefix or ISSN)
    # to the feed key and feed title from planet.json. This helps lookups that
    # prefer a feed-level publication identifier when searching CrossRef.
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publications (
            publication_id TEXT NOT NULL,
            feed_id TEXT,
            feed_title TEXT,
            issn TEXT NOT NULL,
            PRIMARY KEY (publication_id, issn)
        )
        """
    )
    conn.commit()

    # Migrate an older single-column PK `publications` table to the new
    # composite primary key (publication_id, issn) while preserving data.
    try:
        cur.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='publications' LIMIT 1")
        row = cur.fetchone()
        if row:
            sql = row[0] or ''
            if 'PRIMARY KEY (publication_id, issn)' not in sql:
                logger.info('migrating publications table to composite primary key (publication_id, issn)')
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS publications_new (
                        publication_id TEXT NOT NULL,
                        feed_id TEXT,
                        feed_title TEXT,
                        issn TEXT NOT NULL,
                        PRIMARY KEY (publication_id, issn)
                    )
                    """
                )
                # copy rows, coalescing NULL issn to empty string
                try:
                    cur.execute(
                        "INSERT OR REPLACE INTO publications_new (publication_id, feed_id, feed_title, issn) SELECT publication_id, feed_id, feed_title, COALESCE(issn, '') FROM publications"
                    )
                    cur.execute("DROP TABLE IF EXISTS publications")
                    cur.execute("ALTER TABLE publications_new RENAME TO publications")
                    conn.commit()
                    logger.info('publications table migrated')
                except Exception:
                    logger.debug('failed to migrate publications table; leaving original in place')
    except Exception:
        logger.debug('publications migration check failed')

    logger.debug("initialized database and ensured tables exist")
    # If an old `journal_works` table exists, migrate any DOI'd rows into `articles`
    try:
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='journal_works' LIMIT 1")
        if cur.fetchone():
            logger.info("migrating data from journal_works into articles (doi-only rows)")
            # copy rows that have a DOI into articles, preserving publication_id/published/fetched_at
            cur.execute(
                "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, crossref_xml, publication_id, published, fetched_at) SELECT doi, title, authors, abstract, crossref_xml, publication_id, published, fetched_at FROM journal_works WHERE doi IS NOT NULL"
            )
            conn.commit()
            try:
                cur.execute("DROP TABLE IF EXISTS journal_works")
                conn.commit()
                logger.info("dropped legacy journal_works table after migration")
            except Exception:
                logger.debug("failed to drop legacy journal_works table; leaving it in place")
    except Exception:
        logger.debug("journal_works migration check failed")
    # No legacy combined view creation here; `create_combined_view` will be used later


def fetch_feed(session, key, feed_title, url, publication_doi=None, issn=None, timeout=20):
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
            "_feed_issn": issn,
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
            # If the entry has a link, prefer deduping by link first to avoid
            # inserting duplicate items when guid/title/published differ.
            link_val = (e.get("link") or "").strip()
            if link_val:
                try:
                    cur.execute("SELECT id, doi FROM items WHERE link = ? LIMIT 1", (link_val,))
                    existing = cur.fetchone()
                    if existing:
                        existing_id, existing_doi = existing[0], existing[1]
                        logger.debug("item with same link already exists (id=%s, link=%s, doi=%s)", existing_id, link_val, existing_doi)
                        # If the existing row lacks a DOI, attempt to extract one now
                        if not existing_doi:
                            try:
                                entry_obj = e.get("_entry") or {}
                                doi = extract_doi_from_entry(entry_obj) or extract_doi_from_entry(e)
                                if doi:
                                    doi = normalize_doi(doi)
                                    if doi:
                                        # ensure an article row exists and attach doi to the existing item
                                        try:
                                            feed_issn = e.get("_feed_issn") if isinstance(e, dict) else None
                                            feed_pub_id = e.get("_feed_publication_id") if isinstance(e, dict) else None
                                            ensured = ensure_article_row(conn, doi, title=e.get("title"), authors=None, abstract=None, publication_id=feed_pub_id, issn=feed_issn)
                                        except Exception:
                                            ensured = None
                                        try:
                                            cur.execute("UPDATE items SET doi = ? WHERE id = ?", (doi, existing_id))
                                            conn.commit()
                                            logger.info("attached DOI %s to existing item id=%s", doi, existing_id)
                                        except Exception:
                                            logger.debug("failed to attach doi %s to existing item id=%s", doi, existing_id)
                            except Exception:
                                logger.debug("failed to extract/attach DOI for existing item link=%s", link_val)
                        # Skip insertion since the URL is already present
                        continue
                except Exception:
                    # If the lookup fails for some reason, fall back to normal insert
                    logger.debug("link existence check failed for link=%s; continuing with insert", link_val)

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
            else:
                # Insert was ignored because an item with the same unique key
                # (guid/link/title/published) already exists. In that case we
                # should skip any expensive enrichment (CrossRef/title lookups)
                # to avoid re-fetching DOIs for feeds that don't include them.
                existing_link = e.get("link") or ""
                logger.debug("item already exists (skipping enrichment) for link: %s", existing_link)
                continue
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
                    feed_issn = None
                    try:
                        feed_issn = e.get("_feed_issn")
                    except Exception:
                        feed_issn = None
                    ensured = ensure_article_row(conn, doi, title=title, authors=authors, abstract=abstract, publication_id=pub_pid, issn=feed_issn)
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

    # 3b. NBER working paper URLs often look like /papers/w34339 or /papers/w34339#fromrss
    # If the feed provides a feed-level publication_id (e.g. 10.3386) use it; otherwise
    # fall back to known NBER prefix when the link hostname indicates nber.org.
    try:
        link_val = (entry.get("link") or "")
        if link_val:
            m_nber = re.search(r"/papers/(w\d+)", link_val)
            if m_nber:
                suffix = m_nber.group(1)
                # prefer feed-level publication id if present on the entry
                feed_pid = None
                try:
                    feed_pid = entry.get("_feed_publication_id")
                except Exception:
                    feed_pid = None
                if not feed_pid and "nber.org" in link_val:
                    feed_pid = "10.3386"
                if feed_pid:
                    return normalize_doi(f"{feed_pid}/{suffix}")
    except Exception:
        # non-fatal, continue with other heuristics
        pass

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


def upsert_article(conn, doi: str, title: str | None, authors: str | None, abstract: str | None, publication_id: str | None = None, issn: str | None = None):
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
            INSERT INTO articles (doi, title, authors, abstract, crossref_xml, publication_id, issn, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(doi) DO UPDATE SET
                title=excluded.title,
                authors=excluded.authors,
                abstract=excluded.abstract,
                crossref_xml=COALESCE(excluded.crossref_xml, articles.crossref_xml),
                publication_id=COALESCE(excluded.publication_id, articles.publication_id),
                issn=COALESCE(excluded.issn, articles.issn),
                fetched_at=excluded.fetched_at
            """,
            (doi, title, authors, abstract, crossref_raw, publication_id, issn, now),
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
                INSERT OR REPLACE INTO articles (id, doi, title, authors, abstract, crossref_xml, publication_id, issn, fetched_at)
                VALUES (
                    (SELECT id FROM articles WHERE doi = ?), ?, ?, ?, ?, COALESCE(?, (SELECT crossref_xml FROM articles WHERE doi = ?)), COALESCE((SELECT publication_id FROM articles WHERE doi = ?), ?), COALESCE((SELECT issn FROM articles WHERE doi = ?), ?), ?
                )
                """,
                (doi, doi, title, authors, abstract, crossref_raw, doi, publication_id, doi, issn, now),
            )
            conn.commit()
            cur.execute("SELECT id FROM articles WHERE doi = ? LIMIT 1", (doi,))
            row = cur.fetchone()
            aid = row[0] if row and row[0] else None
            return aid
        except Exception:
            return False


def ensure_article_row(conn, doi: str, title: str | None = None, authors: str | None = None, abstract: str | None = None, publication_id: str | None = None, issn: str | None = None) -> int | None:
    """Ensure an article row exists for `doi`. Insert minimal data if missing and
    return the article id. Does NOT call CrossRef.
    """
    cur = conn.cursor()
    if not doi:
        logger.debug("ensure_article_row called without doi; skipping")
        return None
    try:
        cur.execute(
            "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, publication_id, issn, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (doi, title, authors, abstract, publication_id, issn, datetime.now(timezone.utc).isoformat()),
        )
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
    # Build a view that includes both pulled feed `items` and journal-discovered works
    # joined with the canonical `articles` table for metadata.
    cur.execute(
        """
        CREATE VIEW IF NOT EXISTS combined_articles AS
        SELECT
            articles.doi AS doi,
            COALESCE(articles.title, '') AS title,
            ('https://doi.org/' || articles.doi) AS link,
            COALESCE(publications.feed_title, feeds.feed_title, '') AS feed_title,
            COALESCE(articles.abstract, '') AS content,
            COALESCE(articles.published, articles.fetched_at) AS published,
            COALESCE(articles.authors, '') AS authors
        FROM articles
        	LEFT JOIN publications on publications.issn = articles.issn
        	LEFT JOIN publications as feeds on feeds.publication_id = articles.publication_id
        WHERE articles.doi IS NOT NULL
        """
    )
    conn.commit()


def fetch_latest_journal_works(conn: sqlite3.Connection, feeds, per_journal: int = 30, timeout: int = 10, delay: float = 0.05):
    """Query CrossRef for the latest works for each feed that has an `issn`.
    Stores discovered DOIs directly into the `articles` table (only DOIs are
    accepted). Returns count of inserted article rows.
    """
    cur = conn.cursor()
    session = requests.Session()
    inserted = 0
    for item in feeds:
        # feeds may be tuples of (key, title, url, publication_id, issn)
        if len(item) == 5:
            key, title, url, publication_id, issn = item
        elif len(item) == 4:
            key, title, url, publication_id = item
            issn = None
        else:
            # unknown shape; skip
            continue

        # We prefer to look up works by the feed's journal title. If that's
        # missing, fall back to an explicit ISSN when present, then to
        # publication_id (DOI prefix) when present. If none of these are
        # available, skip the feed.
        if not (issn):
            continue
        logger.info("fetching latest works for feed=%s title=%r publication_id=%r issn=%r", key, title, publication_id, issn)
        try:
            headers = {"User-Agent": "ed-news-fetcher/1.0", "Accept": "application/json"}
            params = {"rows": per_journal, "sort": "published", "order": "desc"}

            mailto = os.environ.get("CROSSREF_MAILTO", "your_email@example.com")
            url = f"https://api.crossref.org/journals/{issn}/works"
            params = {"sort": "created", "order": "desc", "filter": "type:journal-article", "rows": min(per_journal, 100), "mailto": mailto}

            resp = session.get(url, params=params, headers=headers, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            items = data.get("message", {}).get("items", []) or []

            for it in items[:per_journal]:
                doi = (it.get("DOI") or "").strip()
                if not doi:
                    continue
                norm = normalize_doi(doi)
                if not norm:
                    continue
                published = None
                if it.get("created") and it.get("created").get("date-time"):
                    published = it.get("created").get("date-time")
                elif it.get("published-print") and it.get("published-print").get("date-parts"):
                    parts = it.get("published-print").get("date-parts")[0]
                    published = "-".join(str(p) for p in parts)
                # Insert into articles directly (only DOIs accepted)
                try:
                    # record which identifier we used for this feed: prefer issn when present
                    db_pub_id = issn

                    # extract minimal article metadata from CrossRef item
                    title_text = it.get("title") and (it.get("title")[0] if isinstance(it.get("title"), list) else it.get("title"))
                    authors_text = None
                    if it.get("author"):
                        authors_text = ", ".join([" ".join(filter(None, [a.get("given"), a.get("family")])) for a in it.get("author") if a])
                    # CrossRef items may contain abstract in 'abstract'
                    abstract_text = it.get("abstract") or None
                    # crossref_xml isn't available from the works endpoint; leave NULL for now
                    crossref_raw = None

                    # Only insert if we have a DOI (norm is truthy)
                    cur.execute(
                        "INSERT OR IGNORE INTO articles (doi, title, authors, abstract, crossref_xml, publication_id, issn, published, fetched_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (norm, title_text, authors_text, abstract_text, crossref_raw, db_pub_id, issn, published, datetime.now(timezone.utc).isoformat()),
                    )
                    if cur.rowcount:
                        inserted += 1
                except Exception as e:
                    logger.debug("failed to insert article doi=%s: %s", norm, e)
            conn.commit()
        except Exception as e:
            logger.warning("failed to fetch latest works for %s (%s): %s", key, publication_id, e)
        time.sleep(delay)
    logger.info("inserted %d new articles from CrossRef journal queries", inserted)
    return inserted


def main(run_feeds: bool = True, run_journal_works: bool = True):
    feeds = load_feeds()
    if not feeds:
        logger.error("No feeds found in planet.json")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Populate publications table from feeds that provide a publication_id.
    try:
        cur = conn.cursor()
        added = 0
        for item in feeds:
            # item may be (key, title, url, publication_doi, issn) or older 4-tuples
            if len(item) == 5:
                key, title, url, publication_doi, issn = item
            elif len(item) == 4:
                key, title, url, publication_doi = item
                issn = None
            else:
                continue
            # Some feeds may provide only an ISSN (no publication_id). Use the
            # publication_id when present; otherwise fall back to the ISSN so we
            # still record the feed in the publications table. We keep the raw
            # issn value in the `issn` column as well.
            pub_key = publication_doi or (issn if issn else None)
            if pub_key:
                try:
                    cur.execute(
                        "INSERT OR REPLACE INTO publications (publication_id, feed_id, feed_title, issn) VALUES (?, ?, ?, ?)",
                        (pub_key, key, title, issn),
                    )
                    added += 1
                except Exception:
                    logger.debug("failed to upsert publication row for feed %s (publication_id=%s)", key, pub_key)
        if added:
            conn.commit()
            logger.info("populated publications table with %d entries from planet.json", added)
    except Exception:
        logger.debug("failed to populate publications table from feeds")

    if run_journal_works:
        # Fetch latest journal works (from CrossRef) for feeds that provide a publication_id
        try:
            fetch_latest_journal_works(conn, feeds, per_journal=30, timeout=10, delay=0.05)
        except Exception as e:
            logger.warning("failed to fetch latest journal works: %s", e)

    if run_feeds:
        session = requests.Session()

        with ThreadPoolExecutor(max_workers=10) as ex:
            # feeds now have optional publication_doi in their tuple
            futures = {}
            for item in feeds:
                # item may be (key, title, url, publication_doi, issn) or older 4-tuples
                if len(item) == 5:
                    key, title, url, publication_doi, issn = item
                elif len(item) == 4:
                    key, title, url, publication_doi = item
                    issn = None
                else:
                    # unknown shape; skip
                    continue
                fut = ex.submit(fetch_feed, session, key, title, url, publication_doi)
                futures[fut] = (key, title, url, publication_doi, issn)
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
    parser = argparse.ArgumentParser(description="Fetch feeds and/or fetch latest journal works from CrossRef")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--feeds-only", action="store_true", help="only fetch RSS/Atom feeds listed in planet.json")
    group.add_argument("--journals-only", action="store_true", help="only fetch latest journal works from CrossRef for journals in planet.json")
    args = parser.parse_args()
    if args.feeds_only:
        main(run_feeds=True, run_journal_works=False)
    elif args.journals_only:
        main(run_feeds=False, run_journal_works=True)
    else:
        main(run_feeds=True, run_journal_works=True)