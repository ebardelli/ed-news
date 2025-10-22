"""Crossref integration helpers for ed-news.

This module contains utilities to lookup DOIs by title, fetch Crossref
metadata (preferring the JSON REST API and falling back to Unixref XML),
and normalize date strings returned by Crossref. Functions in this module
are used by feed processing and ScienceDirect enrichment helpers.
"""

import logging
import json
import re
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

logger = logging.getLogger("ednews.crossref")


from functools import lru_cache
from ednews import http as http_helper
from ednews import config as _config


def _query_crossref_doi_by_title_uncached(title: str, preferred_publication_id: str | None = None, timeout: int = 8) -> str | None:
    """Lookup a DOI on Crossref by article title (uncached implementation).

    This function performs a network request to Crossref's /works endpoint
    searching by title. If a `preferred_publication_id` is provided the
    implementation will prefer returned DOIs that start with that prefix.

    Parameters
    ----------
    title : str
        Article title to search for.
    preferred_publication_id : str | None
        Optional DOI prefix to prefer when selecting a result.
    timeout : int
        HTTP request timeout in seconds.

    Returns
    -------
    str | None
        The discovered DOI string or None if not found.
    """
    if not title:
        return None
    try:
        headers = {"User-Agent": getattr(_config, 'USER_AGENT', 'ed-news-fetcher/1.0'), "Accept": "application/json"}
        params = {"query.title": title, "rows": 20}
        logger.debug("CrossRef title lookup for title: %s", title)
        connect_to = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
        read_to = timeout or getattr(_config, 'CROSSREF_TIMEOUT', 30)
        used_timeout = (connect_to, read_to)
        data = http_helper.get_json("https://api.crossref.org/works", params=params, headers=headers, timeout=used_timeout, retries=getattr(_config, 'CROSSREF_RETRIES', 3), backoff=getattr(_config, 'CROSSREF_BACKOFF', 0.3), status_forcelist=getattr(_config, 'CROSSREF_STATUS_FORCELIST', None))
        items = data.get("message", {}).get("items", []) if isinstance(data, dict) else []
        if not items:
            return None
        if preferred_publication_id:
            pref = preferred_publication_id.rstrip().lower()
            for it in items:
                d = (it.get("DOI") or "").lower()
                if d.startswith(pref):
                    logger.info("CrossRef title lookup: selected DOI %s matching preferred_publication_id %s for title: %s", d, pref, title)
                    return d
        doi = items[0].get("DOI")
        if doi:
            logger.info("CrossRef title lookup: found DOI %s for title: %s", doi, title)
            return doi
    except Exception as e:
        logger.debug("CrossRef title lookup error for '%s': %s", title, e)
    return None


@lru_cache(maxsize=256)
def query_crossref_doi_by_title(title: str, preferred_publication_id: str | None = None, timeout: int = 8) -> str | None:
    # Use a cached wrapper around the networked implementation. Note that
    # lru_cache will treat the (title, preferred_publication_id, timeout)
    # tuple as the cache key; timeouts should generally be stable for calls.
    return _query_crossref_doi_by_title_uncached(title, preferred_publication_id, timeout)


def fetch_crossref_metadata(doi: str, timeout: int = 10, conn: object | None = None) -> dict | None:
    """Fetch Crossref metadata for a DOI, preferring JSON and falling back to XML.

    The function will attempt to fetch JSON from the Crossref REST API. If
    that fails it falls back to the legacy Unixref XML endpoint. It extracts
    authors, abstract, raw payload and a best-effort publication date.

    Parameters
    ----------
    doi : str
        DOI to lookup.
    timeout : int
        HTTP request timeout in seconds.

    Returns
    -------
    dict | None
        Dictionary with any of the keys 'authors', 'abstract', 'raw', 'published'
        when available, or None if the lookup failed.
    """
    if not doi:
        return None
    # If this DOI already exists in the local articles DB, skip the
    # Crossref network lookup to avoid unnecessary API requests. Import
    # and open the DB connection lazily to avoid circular imports.
    try:
        # If a conn is provided, use it; otherwise try to open the configured DB
        # path lazily. Use ednews.db helpers when available.
        from ednews.db import article_exists
        if conn is None:
            try:
                from ednews import config as _cfg
                import sqlite3
                conn_local = sqlite3.connect(str(_cfg.DB_PATH))
                try:
                    if article_exists(conn_local, doi):
                        logger.info("Skipping CrossRef lookup for DOI %s because it already exists in DB", doi)
                        try:
                            conn_local.close()
                        except Exception:
                            pass
                        return None
                finally:
                    try:
                        conn_local.close()
                    except Exception:
                        pass
            except Exception:
                # Fall back to network lookup if DB access is not possible
                pass
        else:
            try:
                if article_exists(conn, doi):
                    logger.info("Skipping CrossRef lookup for DOI %s because it already exists in DB", doi)
                    return None
            except Exception:
                # If the provided conn can't be used for existence check, fall through
                pass
    except Exception:
        # If ednews.db isn't importable, proceed with the network lookup
        pass
    # Try to fetch JSON from the Crossref REST API first and prefer the
    # message.created -> date-parts field for determining a publication date.
    # If JSON isn't available or parsing fails, fall back to the unixref XML
    # endpoint (dx.crossref.org) as before.
    # quote DOI path component safely (don't quote slashes inside DOI suffix)
    from urllib.parse import quote
    quoted = quote(doi, safe="/:")
    json_url = f"https://api.crossref.org/works/{quoted}"
    json_headers = {"Accept": "application/json", "User-Agent": "ed-news-fetcher/1.0"}
    raw_xml = None
    root = None
    json_message = None
    logger.info("CrossRef JSON lookup for DOI %s -> %s", doi, json_url)
    # Use centralized HTTP helper with configured timeouts/retries
    try:
        connect_to = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
        read_to = timeout or getattr(_config, 'CROSSREF_TIMEOUT', 30)
        used_timeout = (connect_to, read_to)
        json_resp = http_helper.get_json(json_url, headers=json_headers, timeout=used_timeout, retries=getattr(_config, 'CROSSREF_RETRIES', 3), backoff=getattr(_config, 'CROSSREF_BACKOFF', 0.3), status_forcelist=getattr(_config, 'CROSSREF_STATUS_FORCELIST', None), requests_module=requests)
    except Exception:
        json_resp = None
    json_message = json_resp.get('message') if isinstance(json_resp, dict) else None

    if not json_message:
        # fallback to legacy unixref XML
        url = f"http://dx.crossref.org/{doi}"
        headers = {"Accept": "application/vnd.crossref.unixref+xml", "User-Agent": "ed-news-fetcher/1.0"}
        try:
            logger.info("CrossRef lookup for DOI %s -> %s", doi, url)
            try:
                connect_to = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
                read_to = timeout or getattr(_config, 'CROSSREF_TIMEOUT', 30)
                used_timeout = (connect_to, read_to)
                raw_text = http_helper.get_text(url, headers=headers, timeout=used_timeout, retries=getattr(_config, 'CROSSREF_RETRIES', 3), backoff=getattr(_config, 'CROSSREF_BACKOFF', 0.3), status_forcelist=getattr(_config, 'CROSSREF_STATUS_FORCELIST', None), requests_module=requests)
            except Exception:
                raw_text = None
            if not raw_text:
                logger.warning("CrossRef lookup failed for %s", doi)
                return None
            raw_xml = raw_text
            root = ET.fromstring(raw_xml)
        except Exception:
            logger.warning("CrossRef lookup failed for %s", doi)
            return None

    def localname(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    abstract = None
    # If we have JSON message, try to extract abstract / authors from it;
    # otherwise use the XML tree (root) as before.
    if json_message:
        # abstract may be present as HTML or plain text in message['abstract']
        try:
            a = json_message.get('abstract')
            if a and isinstance(a, str) and a.strip():
                abstract = a.strip()
        except Exception:
            abstract = None
    if root is not None:
        for elem in root.iter():
            if localname(elem.tag).lower() == "abstract":
                text = "".join(elem.itertext()).strip()
                if text:
                    abstract = text
                    break
    # Build a parent map so we can detect if an element is inside a reference/citation
    parent_map = {c: p for p in root.iter() for c in p} if root is not None else {}

    # Tags whose presence in the ancestor chain indicate we are inside the references
    reference_ancestor_tags = {
        'reference', 'ref', 'citation', 'citation_list', 'ref-list', 'references'
    }

    authors_list = []
    # If JSON present, extract authors from JSON message (ordered)
    if json_message:
        try:
            ja = json_message.get('author') or []
            for a in ja:
                if isinstance(a, dict):
                    given = a.get('given') or ''
                    family = a.get('family') or ''
                else:
                    given = ''
                    family = ''
                if given or family:
                    authors_list.append(' '.join([p for p in (given.strip(), family.strip()) if p]))
        except Exception:
            pass

    if root is not None:
        for parent in root.iter():
            tag = localname(parent.tag).lower()
            if tag in ("person_name", "contributor", "name", "author", "creator", "person"):
                # skip any person_name / author nodes that are inside a reference/citation block
                cur = parent
                inside_ref = False
                while cur in parent_map:
                    cur = parent_map[cur]
                    if localname(cur.tag).lower() in reference_ancestor_tags:
                        inside_ref = True
                        break
                if inside_ref:
                    continue

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

    authors = None
    if authors_list:
        seen = set()
        dedup = []
        for a in authors_list:
            if a and a not in seen:
                dedup.append(a)
                seen.add(a)
        authors = ", ".join(dedup)

    out = {k: v for k, v in (("authors", authors), ("abstract", abstract)) if v}

    # attempt to extract a publication date (best-effort)
    published = None
    try:
        if json_message:
            # Prefer JSON-created date-parts via a helper that centralizes
            # the various Crossref message fields (created, published-print, ...)
            published = _extract_published_from_json(json_message)

        # If no JSON-derived date, try XML tree parsing as before
        if not published and root is not None:
            for elem in root.iter():
                tag = localname(elem.tag).lower()
                if tag in ("publication_date", "pub_date", "issued", "published", "publicationdate", "created"):
                    y = None
                    m = None
                    d = None
                    for child in elem:
                        ctag = localname(child.tag).lower()
                        text = (child.text or "").strip()
                        if ctag in ("year",) and text:
                            y = text
                        elif ctag in ("month",) and text:
                            m = text.zfill(2)
                        elif ctag in ("day", "date", "dayofmonth") and text:
                            d = text.zfill(2)
                    if y:
                        parts = [y]
                        if m:
                            parts.append(m)
                        if d:
                            parts.append(d)
                        raw = "-".join(parts)
                        published = raw
                        break
    except Exception:
        published = None

    # raw may come from XML or JSON; prefer XML raw when available to keep
    # existing stored format, otherwise store the JSON text
    if raw_xml:
        out["raw"] = raw_xml
    else:
        try:
            # if we fetched JSON, include its text representation
            if json_message is not None and json_resp is not None:
                try:
                    out["raw"] = json.dumps(json_resp)
                except Exception:
                    out["raw"] = str(json_resp)
        except Exception:
            pass
    if published:
        # Try to normalize the extracted date to an ISO-like full datetime when
        # possible (so callers can parse via fromisoformat). If normalization
        # fails (partial dates), keep the original string.
        normalized = normalize_crossref_datetime(published)
        out["published"] = normalized if normalized else published
    return out


def _extract_published_from_json(message: dict) -> str | None:
    """Extract an ISO-ish date string from Crossref JSON message date fields.

    Looks for 'created' first, then falls back to 'published-print',
    'published-online', 'issued', and 'published'. Each of these typically
    contains a 'date-parts' array of arrays such as [[YYYY, M, D]]. We return
    a string like 'YYYY', 'YYYY-MM' or 'YYYY-MM-DD' depending on available
    parts. If the message contains a 'date-time' field, return that string
    directly.
    """
    if not message or not isinstance(message, dict):
        return None

    def build_from_date_parts(dp_list):
        if not dp_list or not isinstance(dp_list, list) or not dp_list[0]:
            return None
        parts = dp_list[0]
        try:
            out_parts = []
            for i, x in enumerate(parts):
                if i == 0:
                    out_parts.append(str(int(x)))
                else:
                    out_parts.append(str(int(x)).zfill(2))
            return "-".join(out_parts)
        except Exception:
            return None

    # Prefer created
    c = message.get('created')
    if isinstance(c, dict):
        # Prefer explicit date-parts (YYYY, YYYY-MM or YYYY-MM-DD) when present
        dp = c.get('date-parts')
        res = build_from_date_parts(dp)
        if res:
            return res
        # Fall back to a full date-time string if no structured date-parts exist
        dt = c.get('date-time') or c.get('date_time') or c.get('date')
        if isinstance(dt, str) and dt.strip():
            return dt.strip()

    for key in ('published-print', 'published-online', 'issued', 'published'):
        mobj = message.get(key)
        if isinstance(mobj, dict):
            # Prefer structured date-parts first, then fallback to any date-time
            dp = mobj.get('date-parts')
            res = build_from_date_parts(dp)
            if res:
                return res
            dt = mobj.get('date-time') or mobj.get('date_time') or mobj.get('date')
            if isinstance(dt, str) and dt.strip():
                return dt.strip()

    return None


def normalize_crossref_datetime(dt_str: str) -> str | None:
    """Normalize a Crossref-derived datetime or date string to ISO format.

    Accepts partial date strings such as 'YYYY' or 'YYYY-MM' and returns them
    unchanged. For full datetimes attempts to parse via datetime.fromisoformat
    and returns an ISO 8601 string including timezone information when possible.
    """
    if not dt_str:
        return None
    s = str(dt_str).strip()
    if not s:
        return None
    # If the string is a date-only value like YYYY or YYYY-MM or YYYY-MM-DD,
    # preserve it as-is (tests expect date-only strings to remain unchanged).
    if re.match(r"^\d{4}(?:-\d{2}(?:-\d{2})?)?$", s):
        return s
    try:
        if s.endswith('Z'):
            s2 = s[:-1] + '+00:00'
        else:
            s2 = s
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except Exception:
        return None
