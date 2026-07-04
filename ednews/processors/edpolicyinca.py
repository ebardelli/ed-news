"""PACE scrapers for edpolicyinca.org — publications and commentaries."""

import logging
from typing import List, Dict
from urllib.parse import urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL = "https://edpolicyinca.org"


# ---------------------------------------------------------------------------
# Publications parser  (/publications)
# ---------------------------------------------------------------------------

def edpolicyinca_publications_processor(html: str, base_url: str = BASE_URL) -> List[Dict]:
    """Parse the PACE publications listing page.

    Relevant fields in each .views-row:
    - .field--name-node-title h2 a         → title
    - .field--name-field-subtitle          → subtitle (appended to title with ': ')
    - .field--name-field-authors           → authors
    - .field--name-field-publication-date  → published date
    - .field--name-body .field__item       → summary
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: List[Dict] = []

    for item in soup.select(".views-row"):
        title_el = item.select_one(".field--name-node-title h2 a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        subtitle_el = item.select_one(".field--name-field-subtitle")
        if subtitle_el:
            subtitle = subtitle_el.get_text(strip=True)
            if subtitle:
                title = f"{title}: {subtitle}"

        link = str(title_el.get("href") or "")
        if link and not link.startswith("http"):
            link = urljoin(base_url, link)

        time_el = item.select_one(".field--name-field-publication-date time[datetime]")
        published = str(time_el.get("datetime") or "") if time_el else ""

        summary = ""
        body_el = item.select_one(".field--name-body .field__item")
        if body_el:
            p = body_el.find("p")
            summary = (p or body_el).get_text(" ", strip=True)

        authors = _extract_authors(item)
        guid = link.rstrip("/").rsplit("/", 1)[-1] if link else None

        entries.append({
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "authors": authors,
            "guid": guid,
        })

    return entries


# ---------------------------------------------------------------------------
# Commentaries parser  (/commentaries)
# ---------------------------------------------------------------------------

def edpolicyinca_commentaries_processor(html: str, base_url: str = BASE_URL) -> List[Dict]:
    """Parse the PACE commentaries listing page.

    Relevant fields in each .views-row:
    - .field--name-node-title h2 a       → title
    - .field--name-field-authors         → authors
    - .field--name-field-article-date    → published date
    - .field--name-field-summary         → summary
    """
    soup = BeautifulSoup(html, "html.parser")
    entries: List[Dict] = []

    for item in soup.select(".views-row"):
        title_el = item.select_one(".field--name-node-title h2 a")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        link = str(title_el.get("href") or "")
        if link and not link.startswith("http"):
            link = urljoin(base_url, link)

        time_el = item.select_one(".field--name-field-article-date time[datetime]")
        published = str(time_el.get("datetime") or "") if time_el else ""

        summary = ""
        summary_el = item.select_one(".field--name-field-summary .field__item")
        if summary_el:
            p = summary_el.find("p")
            summary = (p or summary_el).get_text(" ", strip=True)

        authors = _extract_authors(item)
        guid = link.rstrip("/").rsplit("/", 1)[-1] if link else None

        entries.append({
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
            "authors": authors,
            "guid": guid,
        })

    return entries


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _extract_authors(item) -> str:
    authors_el = item.select_one(".field--name-field-authors .field__items")
    if not authors_el:
        return ""
    links = authors_el.select(".field__item a")
    if links:
        return ", ".join(a.get_text(strip=True) for a in links)
    return authors_el.get_text(" ", strip=True)


def _make_scraper(session):
    try:
        import cloudscraper
        return cloudscraper.create_scraper()
    except ImportError:
        logger.warning("cloudscraper not available; falling back to requests session")
        return session


def _fetch_paginated(scraper, url: str, parser) -> List[Dict]:
    entries: List[Dict] = []
    next_url: str | None = url

    while next_url:
        try:
            resp = scraper.get(next_url, timeout=30)
            resp.raise_for_status()
        except Exception as exc:
            logger.error("Failed to fetch %s: %s", next_url, exc)
            break

        entries.extend(parser(resp.text, base_url=BASE_URL))

        soup = BeautifulSoup(resp.text, "html.parser")
        next_link = (
            soup.select_one("a[rel='next']")
            or soup.select_one("li.pager__item--next a")
            or soup.select_one("li.next a")
            or soup.select_one(".pager-next a")
        )
        if next_link:
            href = str(next_link.get("href") or "")
            next_url = urljoin(BASE_URL, href) if href and not href.startswith("http") else (href or None)
        else:
            next_url = None

    return entries


def _normalize_published(raw: str) -> str:
    if not raw:
        return raw
    s = raw.strip()
    try:
        from datetime import datetime
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.date().isoformat()
        except Exception:
            pass
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(s)
            return dt.date().isoformat()
        except Exception:
            pass
    except Exception:
        pass
    return s


def _postprocessor_db(conn, feed_key: str, entries) -> int:
    """Write listing-page data to the articles table.

    Reads summary and authors from items (saved by the preprocessor) rather than
    the entry dict, which the fetch pipeline rebuilds from a narrower DB query.
    """
    try:
        from ednews import db as eddb
    except Exception:
        return 0

    cur = conn.cursor()
    enriched = 0

    for entry in entries:
        link = (entry.get("link") or "").strip()
        if not link:
            continue

        synthetic_doi = link
        title = entry.get("title")
        published = _normalize_published(entry.get("published") or "") or None

        cur.execute(
            "SELECT summary, authors FROM items WHERE feed_id = ? AND link = ? LIMIT 1",
            (feed_key, link),
        )
        row = cur.fetchone()
        abstract = (row[0] if row else None) or None
        authors = (row[1] if row else None) or None

        try:
            aid = eddb.upsert_article(
                conn,
                synthetic_doi,
                title=title,
                authors=authors,
                abstract=abstract,
                feed_id=feed_key,
                published=published,
            )
            if aid:
                enriched += 1
                try:
                    cur.execute(
                        "UPDATE items SET doi = ? WHERE feed_id = ? AND link = ?",
                        (synthetic_doi, feed_key, link),
                    )
                    conn.commit()
                except Exception:
                    pass
        except Exception as exc:
            logger.warning("upsert_article failed for %s: %s", link, exc)

    return enriched


# ---------------------------------------------------------------------------
# Publications preprocessor + postprocessor
# ---------------------------------------------------------------------------

def edpolicyinca_publications_preprocessor(
    session, url: str, publication_id: str | None = None, issn: str | None = None
) -> List[Dict]:
    scraper = _make_scraper(session)
    if not scraper:
        return []
    return _fetch_paginated(scraper, url, edpolicyinca_publications_processor)


def edpolicyinca_publications_postprocessor_db(
    conn, feed_key: str, entries, session=None,
    publication_id: str | None = None, issn: str | None = None,
) -> int:
    return _postprocessor_db(conn, feed_key, entries)


# ---------------------------------------------------------------------------
# Commentaries preprocessor + postprocessor
# ---------------------------------------------------------------------------

def edpolicyinca_commentaries_preprocessor(
    session, url: str, publication_id: str | None = None, issn: str | None = None
) -> List[Dict]:
    scraper = _make_scraper(session)
    if not scraper:
        return []
    return _fetch_paginated(scraper, url, edpolicyinca_commentaries_processor)


def edpolicyinca_commentaries_postprocessor_db(
    conn, feed_key: str, entries, session=None,
    publication_id: str | None = None, issn: str | None = None,
) -> int:
    return _postprocessor_db(conn, feed_key, entries)


# ---------------------------------------------------------------------------
# Legacy aliases (kept for any existing references)
# ---------------------------------------------------------------------------

edpolicyinca_processor = edpolicyinca_publications_processor
edpolicyinca_preprocessor = edpolicyinca_publications_preprocessor
edpolicyinca_postprocessor_db = edpolicyinca_publications_postprocessor_db
