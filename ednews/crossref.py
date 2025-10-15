import logging
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

logger = logging.getLogger("ednews.crossref")


def query_crossref_doi_by_title(title: str, preferred_publication_id: str | None = None, timeout: int = 8) -> str | None:
    if not title:
        return None
    try:
        headers = {"User-Agent": "ed-news-fetcher/1.0", "Accept": "application/json"}
        params = {"query.title": title, "rows": 20}
        logger.debug("CrossRef title lookup for title: %s", title)
        resp = requests.get("https://api.crossref.org/works", params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("message", {}).get("items", []) or []
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


def fetch_crossref_metadata(doi: str, timeout: int = 10) -> dict | None:
    if not doi:
        return None
    url = f"http://dx.crossref.org/{doi}"
    headers = {"Accept": "application/vnd.crossref.unixref+xml", "User-Agent": "ed-news-fetcher/1.0"}
    try:
        logger.info("CrossRef lookup for DOI %s -> %s", doi, url)
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
        raw_xml = resp.content.decode('utf-8', errors='replace')
        root = ET.fromstring(raw_xml)
    except Exception:
        logger.warning("CrossRef lookup failed for %s", doi)
        return None

    def localname(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    abstract = None
    for elem in root.iter():
        if localname(elem.tag).lower() == "abstract":
            text = "".join(elem.itertext()).strip()
            if text:
                abstract = text
                break

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
        for elem in root.iter():
            tag = localname(elem.tag).lower()
            if tag in ("publication_date", "pub_date", "issued", "published", "publicationdate"):
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

    out["raw"] = raw_xml
    if published:
        out["published"] = published
    return out


def normalize_crossref_datetime(dt_str: str) -> str | None:
    if not dt_str:
        return None
    s = str(dt_str).strip()
    if not s:
        return None
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
