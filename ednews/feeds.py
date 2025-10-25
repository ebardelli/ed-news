"""Feed parsing and normalization utilities.

This module loads feed sources from planet files, fetches and parses feeds,
and provides helpers to extract DOIs, authors, and abstracts from feed
entries. It also contains logic to persist entries into the project's DB.
"""

import json
import logging
from pathlib import Path
from typing import List
import feedparser
import requests
from . import config
import re
import html
from datetime import datetime, timezone
from . import crossref
from . import db as eddb

logger = logging.getLogger("ednews.feeds")


def entry_has_content(entry: dict) -> bool:
    """Return True if a feedparser entry/dict has usable content.

    Consider title, link, summary/content as evidence of meaningful content.
    """
    if not entry or not isinstance(entry, dict):
        return False
    title = (entry.get("title") or "")
    link = (entry.get("link") or "")
    summary = (entry.get("summary") or "")
    # Some feeds put the body in `content` blocks
    content_blocks = []
    for c in entry.get("content", []) or []:
        content_blocks.append(c.get("value") or "")
    content = " ".join(content_blocks)
    # Also check abstract extractor as a fallback
    abstract = extract_abstract_from_entry(entry) or ""
    return bool(str(title).strip() or str(link).strip() or str(summary).strip() or str(content).strip() or str(abstract).strip())


def load_feeds() -> List[tuple]:
    """Load feeds from `planet.json` or fallback to `planet.ini`.

    Returns a list of tuples describing each feed: (key, title, url, publication_id, issn).
    """
    # Use the project's JSON file `research.json`.
    p = config.RESEARCH_JSON
    if not p.exists():
        logger.debug("planet file not found: %s", p)
        return []
    # prefer JSON planets; original project used planet.json
    if p.suffix == ".json":
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        feeds = data.get("feeds", {})
        results = []
        for key, info in feeds.items():
                url = info.get("feed")
                if url:
                    pub_id = info.get("publication_id")
                    issn = info.get("issn")
                    processor = info.get("processor") if isinstance(info, dict) else None
                    # Return a tuple: (key, title, url, publication_id, issn, processor)
                    results.append((key, info.get("title"), url, pub_id, issn, processor))
        return results
    # fallback: caller can import ednews.build.read_planet for ini
    return []


def fetch_feed(session, key, feed_title, url, publication_doi=None, issn=None, timeout=20):
    """Fetch and parse a single feed URL.

    Returns a dict containing feed metadata and a list of parsed entries. The
    function attempts to filter entries to those from the feed's most recent
    publication date when possible.
    """
    logger.info("fetching feed %s (%s)", key, url)
    try:
        resp = session.get(url, timeout=timeout, headers={"User-Agent": config.USER_AGENT})
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
        entries.append({
            "guid": guid,
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "_entry": e,
            "_feed_publication_id": publication_doi,
            "_feed_issn": issn,
        })

    # Ensure we only return entries that were published on the same (most recent) date
    # for this feed. Prefer structured `published_parsed` when available, otherwise
    # try to extract an ISO YYYY-MM-DD with a regex. If no parseable dates are
    # found at all, fall back to returning all entries.
    def entry_date_iso(e):
        ent = e.get("_entry") or {}
        # feedparser sometimes provides a struct_time in published_parsed
        pp = ent.get("published_parsed") or ent.get("updated_parsed")
        if pp:
            try:
                return datetime(*pp[:6]).date().isoformat()
            except Exception:
                pass
        # fallback: try to extract YYYY-MM-DD from string
        pub_s = (e.get("published") or "")
        m = re.search(r"(\d{4}-\d{2}-\d{2})", pub_s)
        if m:
            return m.group(1)
        return None

    dates = [entry_date_iso(en) for en in entries]
    parseable = [d for d in dates if d]
    if parseable:
        latest_date = max(parseable)
        filtered = []
        for i, en in enumerate(entries):
            d = dates[i]
            if d == latest_date:
                filtered.append(en)
        entries = filtered

    logger.debug("parsed %d entries from feed %s (filtered to %d by latest date)", len(parsed.entries), key, len(entries))
    return {"key": key, "title": feed_title, "url": url, "publication_id": publication_doi, "error": None, "entries": entries}


def title_suitable_for_crossref_lookup(title: str) -> bool:
    """Return True if a title is likely appropriate for a Crossref title lookup.

    Performs simple heuristics such as minimum length, blacklist checks and
    numeric/DOI detection to decide whether a title should be used for a
    Crossref query.
    """
    if not title:
        return False
    t = title.strip()
    if len(t) < 10:
        return False
    blacklist = {"editorial", "editorial board", "correction", "corrections", "erratum", "letter to the editor", "front matter"}
    if t.lower() in blacklist:
        return False
    words = [w for w in re.split(r"\s+", t) if w]
    if len(words) <= 3:
        if re.search(r"\d{4}", t) or re.search(r"10\.\d{4,9}/", t):
            return True
        return False
    return True


def normalize_doi(doi: str) -> str | None:
    """Normalize and canonicalize a DOI-like string.

    Strips common URI prefixes, trailing punctuation and returns the core DOI
    in lowercase if a DOI pattern is detected. If no DOI is found but the
    input looks like a title, this function may attempt a Crossref lookup.
    """
    if not doi:
        return None
    doi = doi.strip()
    doi = re.sub(r"^(doi:\s*|https?://(dx\.)?doi\.org/)", "", doi, flags=re.IGNORECASE)
    doi = doi.strip()
    doi = re.split(r"[?#]", doi, maxsplit=1)[0]
    doi = doi.rstrip(" .;,)/]")
    doi = doi.strip('"\'<>[]()')
    m = re.search(r"(10\.\d{4,9}/\S+)", doi)
    if m:
        core = m.group(1)
        core = core.rstrip(" .;,)/]")
        # remove surrounding quotes/brackets/angle-brackets if present
        core = core.strip('"\'<>[]()')
        # DOIs are case-insensitive; store canonical lowercase form
        return core.lower()
    if not re.search(r"10\.\d{4,9}/", doi) and "/" not in doi and title_suitable_for_crossref_lookup(doi):
        try:
            found = crossref.query_crossref_doi_by_title(doi)
            if found:
                m2 = re.search(r"(10\.\d{4,9}/\S+)", found)
                if m2:
                    raw = m2.group(1)
                    raw = raw.rstrip(" .;,)/]")
                    raw = raw.strip('"\'<>[]()')
                    return raw.lower()
                return str(found).lower()
        except Exception:
            logging.getLogger("ednews.feeds").debug("CrossRef title lookup failed")
        return None


# (extract_doi_from_entry is defined below; see helper `extract_and_normalize_doi` after it)


def extract_doi_from_entry(entry) -> str | None:
    """Try to extract a DOI from a feed entry.

    The function examines common fields such as 'doi', 'links', 'id', and the
    contents of 'summary' and 'content' looking for DOI patterns or DOI URLs.
    Returns a normalized DOI or None.
    """
    for key in ("doi", "dc:identifier", "doi"):
        v = entry.get(key)
        if v:
            d = normalize_doi(str(v))
            if d:
                return d
    for l in entry.get("links", []) or []:
        href = l.get("href")
        if not href:
            continue
        m = re.search(r"doi\.org/(10\.\d{4,9}/[^\s'\"]+)", href, flags=re.IGNORECASE)
        if m:
            return normalize_doi(m.group(1))
    idv = entry.get("id") or entry.get("guid")
    if idv:
        m = re.search(r"(10\.\d{4,9}/[^\s'\"]+)", str(idv))
        if m:
            return normalize_doi(m.group(1))
    # Check the simple `link` field for DOI URLs (many feeds put doi in link)
    link_val = (entry.get("link") or "")
    if link_val:
        m_link_doi = re.search(r"doi\.org/(10\.\d{4,9}/[^\s'\"]+)", link_val, flags=re.IGNORECASE)
        if m_link_doi:
            return normalize_doi(m_link_doi.group(1))
    try:
        link_val = (entry.get("link") or "")
        if link_val:
            m_nber = re.search(r"/papers/(w\d+)", link_val)
            if m_nber:
                suffix = m_nber.group(1)
                feed_pid = entry.get("_feed_publication_id") if isinstance(entry, dict) else None
                if not feed_pid and "nber.org" in link_val:
                    feed_pid = "10.3386"
                if feed_pid:
                    return normalize_doi(f"{feed_pid}/{suffix}")
    except Exception:
        pass
    text_candidates = []
    if entry.get("summary"):
        text_candidates.append(entry.get("summary"))
    for content in entry.get("content", []) or []:
        text_candidates.append(content.get("value") or "")
    for txt in text_candidates:
        if not txt:
            continue
        # If the DOI appears inside an HTML href (e.g., <a href="https://doi.org/...">),
        # searching the raw text for doi.org URLs can find it before tags are stripped.
        m_href = re.search(r"doi\.org/(10\.\d{4,9}/[^\s'\"<>]+)", txt, flags=re.IGNORECASE)
        if m_href:
            return normalize_doi(m_href.group(1))
        t = re.sub(r"<[^>]+>", " ", txt)
        m = re.search(r"(10\.\d{4,9}/[^\s'\"<>]+)", t)
        if m:
            return normalize_doi(m.group(1))
    return None


def extract_and_normalize_doi(entry) -> str | None:
    """Centralized DOI extraction helper.

    Accepts a feedparser entry or a plain dict. Returns a canonicalized DOI
    (lowercase, stripped prefixes/punctuation) or None.
    """
    # Delegate to the main extractor (defined above) and ensure we return
    # a canonicalized DOI or None. This wrapper centralizes future changes.
    try:
        return extract_doi_from_entry(entry)
    except Exception:
        return None


def extract_authors_from_entry(entry) -> str | None:
    """Extract a comma-separated author string from a feed entry.

    Supports feedparser-style `authors` lists as well as legacy `author` and
    `dc_creator` fields. Returns a single string or None.
    """
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
    if not authors and entry.get("dc_creator"):
        authors = [entry.get("dc_creator")]
    if authors:
        cleaned = [html.unescape(a) for a in authors if a]
        return ", ".join(cleaned)
    return None


def extract_abstract_from_entry(entry) -> str | None:
    """Extract an abstract/summary text from a feed entry.

    Prefers `summary` then the first `content` block. HTML tags are stripped
    and entities unescaped.
    """
    abstract = entry.get("summary") or None
    if not abstract:
        for c in entry.get("content", []) or []:
            v = c.get("value")
            if v:
                abstract = v
                break
    if abstract:
        txt = re.sub(r"<[^>]+>", " ", abstract)
        return html.unescape(txt).strip()
    return None


def save_entries(conn, feed_id, feed_title, entries):
    """Persist feed entries into the database `items` table.

    Performs deduplication by link and attempts to attach DOIs and upsert
    related article records when a DOI is found.
    """
    cur = conn.cursor()
    inserted = 0
    logger.debug("saving %d entries for feed %s (%s)", len(entries), feed_id, feed_title)
    for e in entries:
        # Skip entirely empty entries (no title, link, or content)
        try:
            if not entry_has_content(e):
                logger.debug("skipping empty entry for feed %s: %r", feed_id, e)
                continue
        except Exception:
            # On unexpected shapes, be conservative and attempt to process
            pass
        # Filter: exclude entries with titles that are editorial board notes
        try:
            title_val = (e.get("title") or "")
            if isinstance(title_val, str):
                tnorm = title_val.strip().lower()
                # consult configured title filters (if present)
                try:
                    filters = config.TITLE_FILTERS
                except Exception:
                    filters = []
                if any(tnorm == f.strip().lower() for f in (filters or [])):
                    logger.info("skipping filtered title '%s' for feed %s: %r", title_val, feed_id, e)
                    continue
        except Exception:
            pass
        try:
            doi = None
            link_val = (e.get("link") or "").strip()
            if link_val:
                try:
                    cur.execute("SELECT id, doi FROM items WHERE link = ? LIMIT 1", (link_val,))
                    existing = cur.fetchone()
                    if existing:
                        existing_id, existing_doi = existing[0], existing[1]
                        logger.debug("item with same link already exists (id=%s, link=%s, doi=%s)", existing_id, link_val, existing_doi)
                        if not existing_doi:
                            try:
                                entry_obj = e.get("_entry") or {}
                                maybe_doi = extract_doi_from_entry(entry_obj) or extract_doi_from_entry(e)
                                if maybe_doi:
                                    maybe_doi = normalize_doi(maybe_doi)
                                    if maybe_doi:
                                        try:
                                            feed_issn = e.get("_feed_issn") if isinstance(e, dict) else None
                                            feed_pub_id = e.get("_feed_publication_id") if isinstance(e, dict) else None
                                            ensured = eddb.ensure_article_row(conn, maybe_doi, title=e.get("title"), authors=None, abstract=None, feed_id=feed_id, publication_id=feed_pub_id, issn=feed_issn)
                                        except Exception:
                                            ensured = None
                                        try:
                                            cur.execute("UPDATE items SET doi = ? WHERE id = ?", (maybe_doi, existing_id))
                                            conn.commit()
                                            logger.info("attached DOI %s to existing item id=%s", maybe_doi, existing_id)
                                        except Exception:
                                            logger.debug("failed to attach doi %s to existing item id=%s", maybe_doi, existing_id)
                                        try:
                                            cur.execute("SELECT published FROM articles WHERE doi = ? LIMIT 1", (maybe_doi,))
                                            rowp = cur.fetchone()
                                            if rowp and rowp[0]:
                                                try:
                                                    cur.execute("UPDATE items SET published = ? WHERE id = ?", (rowp[0], existing_id))
                                                    conn.commit()
                                                    logger.info("updated item id=%s published date from article DOI %s", existing_id, maybe_doi)
                                                except Exception:
                                                    logger.debug("failed to update item.published for id=%s", existing_id)
                                        except Exception:
                                            pass
                            except Exception:
                                logger.debug("failed to extract/attach DOI for existing item link=%s", link_val)
                        continue
                except Exception:
                    logger.debug("link existence check failed for link=%s; continuing with insert", link_val)

            cur.execute(
                """
                INSERT OR IGNORE INTO items
                (feed_id, doi, guid, title, link, published, summary, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feed_id,
                    doi,
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
                item_rowid = cur.lastrowid
            else:
                existing_link = e.get("link") or ""
                logger.debug("item already exists (skipping enrichment) for link: %s", existing_link)
                continue
            entry_obj = e.get("_entry") or {}
            doi = extract_doi_from_entry(entry_obj) or extract_doi_from_entry(e)
            pub_pid = None
            try:
                pub_pid = e.get("_feed_publication_id")
            except Exception:
                pub_pid = None
            if not doi:
                link_val = (e.get("link") or "")
                if ("www.sciencedirect.com" in link_val or "sciencedirect.com" in link_val):
                    lookup_title = e.get("title") or (entry_obj.get("title") if isinstance(entry_obj, dict) else None)
                    if lookup_title and title_suitable_for_crossref_lookup(lookup_title):
                        try:
                            found = crossref.query_crossref_doi_by_title(lookup_title, preferred_publication_id=pub_pid)
                            if found:
                                logger.info("ScienceDirect title lookup found DOI %s for title: %s", found, lookup_title)
                                doi = found
                        except Exception:
                            logger.debug("CrossRef title lookup failed for ScienceDirect title: %s", lookup_title)

            if doi:
                doi = normalize_doi(doi)
                title_feed = e.get("title") or (entry_obj.get("title") if isinstance(entry_obj, dict) else None)
                authors_feed = extract_authors_from_entry(entry_obj) or extract_authors_from_entry(e)
                abstract_feed = extract_abstract_from_entry(entry_obj) or extract_abstract_from_entry(e)
                feed_issn = None
                try:
                    feed_issn = e.get("_feed_issn")
                except Exception:
                    feed_issn = None

                # Try to fetch Crossref metadata for this DOI and prefer its fields.
                # If the DOI already exists in the articles table, skip the network
                # request to Crossref to avoid unnecessary lookups.
                cr = None
                try:
                    if eddb.article_exists(conn, doi):
                        logger.info("Skipping CrossRef lookup for DOI %s because it already exists in DB; loading stored metadata", doi)
                        cr = eddb.get_article_metadata(conn, doi) or None
                    else:
                        cr = crossref.fetch_crossref_metadata(doi, conn=conn)
                except Exception:
                    logger.debug("Crossref lookup or existence check failed for DOI=%s", doi)

                authors_final = None
                abstract_final = None
                published_final = None
                raw_crossref = None

                if isinstance(cr, dict):
                    authors_final = cr.get("authors") or None
                    abstract_final = cr.get("abstract") or None
                    published_final = cr.get("published") or None
                    raw_crossref = cr.get("raw")

                # Prefer Crossref values when available, fall back to feed values
                title_final = title_feed
                authors_final = authors_final or authors_feed
                abstract_final = abstract_final or abstract_feed
                published_final = published_final or (e.get("published") or None)

                try:
                    aid = eddb.upsert_article(
                        conn,
                        doi,
                        title=title_final,
                        authors=authors_final,
                        abstract=abstract_final,
                        feed_id=feed_id,
                        publication_id=pub_pid,
                        issn=feed_issn,
                        published=published_final,
                    )
                    # If we have raw Crossref XML, store it in the articles.crossref_xml column
                    if raw_crossref:
                        try:
                            cur.execute("UPDATE articles SET crossref_xml = ? WHERE doi = ?", (raw_crossref, doi))
                            conn.commit()
                        except Exception:
                            logger.debug("failed to store crossref_xml for doi=%s", doi)

                    # Attach DOI to item and update item.published from article if available
                    if aid and item_rowid:
                        try:
                            cur.execute("UPDATE items SET doi = ? WHERE id = ?", (doi, item_rowid))
                            cur.execute("SELECT published FROM articles WHERE doi = ? LIMIT 1", (doi,))
                            rowp = cur.fetchone()
                            if rowp and rowp[0]:
                                cur.execute("UPDATE items SET published = ? WHERE id = ?", (rowp[0], item_rowid))
                            conn.commit()
                        except Exception:
                            logger.debug("failed to attach doi %s to item %s", doi, item_rowid)
                except Exception:
                    logger.exception("failed to upsert article for doi=%s", doi)
        except Exception:
            continue
    conn.commit()
    logger.info("saved %d new items for feed %s", inserted, feed_id)
    return inserted
