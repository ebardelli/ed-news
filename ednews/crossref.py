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
    try:
        logger.info("CrossRef JSON lookup for DOI %s -> %s", doi, json_url)
        resp = requests.get(json_url, headers=json_headers, timeout=timeout)
        resp.raise_for_status()
        try:
            data = resp.json()
            json_message = data.get("message") if isinstance(data, dict) else None
        except Exception:
            # Not JSON (or DummyResp in tests) - will fall back to XML below
            json_message = None
    except Exception:
        json_message = None

    if not json_message:
        # fallback to legacy unixref XML
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
        # Prefer JSON 'created' date-parts when available (this avoids using
        # published-print which can sometimes be a future print issue date).
        if json_message:
            # created -> date-parts
            c = json_message.get('created') or {}
            dp = c.get('date-parts') if isinstance(c, dict) else None
            if dp and isinstance(dp, list) and dp and isinstance(dp[0], list) and dp[0]:
                parts = [str(int(x)).zfill(2) if i > 0 else str(int(x)) for i, x in enumerate(dp[0])]
                # ensure month/day are zero-padded to two digits if present
                raw = "-".join(parts)
                published = raw
            else:
                # fallback to other message date fields if present
                for key in ('published-print', 'published-online', 'issued', 'published'):
                    mobj = json_message.get(key)
                    if isinstance(mobj, dict):
                        dp = mobj.get('date-parts')
                        if dp and isinstance(dp, list) and dp and isinstance(dp[0], list) and dp[0]:
                            parts = [str(int(x)).zfill(2) if i > 0 else str(int(x)) for i, x in enumerate(dp[0])]
                            raw = "-".join(parts)
                            published = raw
                            break
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
            if json_message is not None:
                out["raw"] = resp.content.decode('utf-8', errors='replace')
        except Exception:
            pass
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
