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


def load_feeds() -> List[tuple]:
    p = config.PLANET_JSON if config.PLANET_JSON.exists() else config.PLANET_INI
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
                results.append((key, info.get("title"), url, pub_id, issn))
        return results
    # fallback: caller can import ednews.build.read_planet for ini
    return []


def fetch_feed(session, key, feed_title, url, publication_doi=None, issn=None, timeout=20):
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

    logger.debug("parsed %d entries from feed %s", len(entries), key)
    return {"key": key, "title": feed_title, "url": url, "publication_id": publication_doi, "error": None, "entries": entries}


def title_suitable_for_crossref_lookup(title: str) -> bool:
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
        core = core.strip('"\'<>[]()')
        return core
    if "/" not in doi:
        try:
            found = crossref.query_crossref_doi_by_title(doi)
            if found:
                m2 = re.search(r"(10\.\d{4,9}/\S+)", found)
                if m2:
                    return m2.group(1).rstrip(" .;,)/]").strip('"\'<>[]()')
                return found
        except Exception:
            logging.getLogger("ednews.feeds").debug("CrossRef title lookup failed")
        return None


def extract_doi_from_entry(entry) -> str | None:
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
        t = re.sub(r"<[^>]+>", " ", txt)
        m = re.search(r"(10\.\d{4,9}/[^\s'\"<>]+)", t)
        if m:
            return normalize_doi(m.group(1))
    return None


def extract_authors_from_entry(entry) -> str | None:
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
    cur = conn.cursor()
    inserted = 0
    logger.debug("saving %d entries for feed %s (%s)", len(entries), feed_id, feed_title)
    for e in entries:
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
                title = e.get("title") or (entry_obj.get("title") if isinstance(entry_obj, dict) else None)
                authors = extract_authors_from_entry(entry_obj) or extract_authors_from_entry(e)
                abstract = extract_abstract_from_entry(entry_obj) or extract_abstract_from_entry(e)
                feed_issn = None
                try:
                    feed_issn = e.get("_feed_issn")
                except Exception:
                    feed_issn = None
                ensured = eddb.ensure_article_row(conn, doi, title=title, authors=authors, abstract=abstract, feed_id=feed_id, publication_id=pub_pid, issn=feed_issn)
                try:
                    if ensured and item_rowid:
                        cur.execute("UPDATE items SET doi = ? WHERE id = ?", (doi, item_rowid))
                        try:
                            cur.execute("SELECT published FROM articles WHERE doi = ? LIMIT 1", (doi,))
                            rowp = cur.fetchone()
                            if rowp and rowp[0]:
                                cur.execute("UPDATE items SET published = ? WHERE id = ?", (rowp[0], item_rowid))
                                conn.commit()
                        except Exception:
                            logger.debug("failed to update item.published for new item id=%s", item_rowid)
                except Exception:
                    logger.debug("failed to attach doi %s to item %s", doi, item_rowid)
        except Exception:
            continue
    conn.commit()
    logger.info("saved %d new items for feed %s", inserted, feed_id)
    return inserted
