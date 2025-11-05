from typing import Dict, List
from bs4 import BeautifulSoup
from ednews import feeds as feeds_mod


def edworkingpapers_processor(html: str, base_url: str | None = "https://edworkingpapers.com", publication_id: str | None = None) -> List[Dict]:
    """Parse EdWorkingPapers listing page HTML and extract paper items.

    Returns a list of dicts with keys: title, link, summary, published.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict] = []

    # The fixture contains a <ul class="list-papers"> with <li class="col"> entries
    list_container = soup.select_one("ul.list-papers")
    if not list_container:
        return out

    for li in list_container.select("li.col"):
        # Title and relative link
        a = li.select_one("h5 a")
        if not a:
            continue
        title = a.get_text(strip=True)
        link = a.get("href") or ""
        if base_url and link.startswith("/"):
            link = base_url.rstrip("/") + link

        # Summary: the first <p> inside the card (there is a nested <p><p> in fixture)
        summary = ""
        p = li.select_one("p")
        if p:
            summary = p.get_text(" ", strip=True)

        # Published date: <time datetime="..."> inside .list-paper-authors
        published = ""
        author_block = li.select_one(".list-paper-authors time.datetime")
        if author_block:
            # Prefer the datetime attribute if present
            dt = author_block.get("datetime") or author_block.get_text(strip=True)
            published = dt

        # Do not attempt to extract or fabricate DOIs from the listing page.
        # DOI discovery is performed by the DB postprocessor which fetches
        # the article page and extracts authoritative metadata.
        # Use the article's last path segment as a stable GUID (e.g. 'ai25-1322').
        guid = link.rstrip('/').rsplit('/', 1)[-1] if link else None
        out.append({"title": title, "link": link, "summary": summary, "published": published, "guid": guid})

    return out


def edworkingpapers_feed_processor(session, feed_url: str, publication_id: str | None = None, issn: str | None = None):
    """Feed-style processor used by the fetcher.

    Accepts a requests-like session and a URL, fetches the HTML and returns
    a dict with keys similar to other feed processors: key, title, url,
    publication_id, error, entries (list of item dicts).
    """
    try:
        resp = session.get(feed_url, timeout=20)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        return {"key": None, "title": None, "url": feed_url, "publication_id": publication_id, "error": str(e), "entries": []}

    entries = edworkingpapers_processor(html, base_url=feed_url, publication_id=publication_id)
    # The CLI's fetcher expects a fetcher-style processor to return a list of
    # entry dicts. Return the entries list directly.
    return entries


def edworkingpapers_postprocessor_db(conn, feed_key: str, entries, session=None, publication_id: str | None = None, issn: str | None = None, force: bool = False, check_fields: list | None = None):
    """DB-level postprocessor for EdWorkingPapers.

    For each entry, fetch the article page (e.g., https://edworkingpapers.com/ai25-1322),
    extract DOI, title, authors, abstract, published, and upsert into `articles`.
    Also attach the DOI to corresponding rows in `items` when found.
    Returns the number of articles upserted.
    """
    if not entries:
        return 0
    try:
        from ednews import db as eddb
        from ednews import feeds as feeds_mod
    except Exception:
        return 0

    cur = conn.cursor()
    updated = 0
    # Preload existing items and article metadata to avoid unnecessary lookups.
    try:
        cur.execute("SELECT guid, link, doi FROM items WHERE feed_id = ?", (feed_key,))
        rows = cur.fetchall()
        items_by_link = {r[1]: (r[2] if len(r) > 2 else None) for r in rows if r and r[1]}
        items_by_guid = {r[0]: (r[2] if len(r) > 2 else None) for r in rows if r and r[0]}
    except Exception:
        items_by_link = {}
        items_by_guid = {}

    # Load existing articles metadata for quick checks (only those with a DOI)
    try:
        cur.execute("SELECT doi, authors, abstract, published FROM articles WHERE COALESCE(doi, '') != ''")
        rows = cur.fetchall()
        articles_meta = {r[0]: {'authors': r[1], 'abstract': r[2], 'published': r[3]} for r in rows if r and r[0]}
    except Exception:
        articles_meta = {}

    # In-run cache keyed by suffix/page_url to avoid duplicate fetches for same article
    inrun_cache: dict = {}
    for e in entries:
        try:
            # Determine article page URL and suffix.
            link = (e.get('link') or '').strip()
            if not link:
                continue
            suffix = link.rstrip('/').rsplit('/', 1)[-1]
            page_url = f"https://edworkingpapers.com/{suffix}"

            # If we've already processed this suffix in this run, skip repeated work
            if suffix in inrun_cache and not force:
                # nothing to do; continue
                continue

            # Quick-check: if item already has a DOI and the corresponding
            # article row has required metadata, skip fetching unless force=True.
            try:
                existing_doi = None
                # prefer exact link lookup, then guid
                existing_doi = items_by_link.get(link) or items_by_guid.get(e.get('guid'))
                if existing_doi and not force:
                    meta = articles_meta.get(existing_doi)
                    if meta:
                        # If check_fields provided, ensure those fields are set on article
                        if check_fields:
                            ok = True
                            for f in check_fields:
                                if not meta.get(f):
                                    ok = False
                                    break
                            if ok:
                                inrun_cache[suffix] = True
                                continue
                        else:
                            # Default behavior: skip if authors or abstract or published present
                            if meta.get('authors') or meta.get('abstract') or meta.get('published'):
                                inrun_cache[suffix] = True
                                continue
            except Exception:
                pass

            # Fetch the article page
            html = None
            try:
                if session:
                    resp = session.get(page_url, timeout=20)
                    resp.raise_for_status()
                    html = resp.text
                else:
                    import requests

                    resp = requests.get(page_url, timeout=20)
                    resp.raise_for_status()
                    html = resp.text
            except Exception:
                # If the page can't be fetched, skip this entry
                continue

            # Parse the article page and extract metadata
            try:
                soup = BeautifulSoup(html, 'html.parser')
            except Exception:
                soup = None

            doi = None
            title = e.get('title') or None
            authors = None
            abstract = None
            published = None

            # 1) Try to find a DOI in meta tags
            try:
                if soup:
                    # common meta names: citation_doi, dc.identifier, or links
                    m = soup.find('meta', attrs={'name': 'citation_doi'})
                    if m and m.get('content'):
                        doi = feeds_mod.normalize_doi(m.get('content'))
                    if not doi:
                        # sometimes DOI is in a <span> or in text; search for doi.org or 10.26300
                        txt = soup.get_text(' ', strip=True)
                        cand = feeds_mod.extract_and_normalize_doi({'summary': txt})
                        if cand:
                            doi = cand
            except Exception:
                doi = None

            # 2) Title: prefer H1 or title tag
            try:
                if soup:
                    h1 = soup.select_one('h1')
                    if h1 and h1.get_text(strip=True):
                        title = h1.get_text(strip=True)
                    if not title:
                        tt = soup.title
                        if tt and tt.string:
                            title = tt.string.strip()
            except Exception:
                pass

            # 3) Authors: look for meta citation_author or a authors block
            try:
                if soup:
                    authors_list = []
                    for ma in soup.select('meta[name="citation_author"]'):
                        v = ma.get('content')
                        if v:
                            authors_list.append(v.strip())
                    if not authors_list:
                        # fallback: look for element with class containing 'author'
                        for el in soup.select('[class*=author]'):
                            txt = el.get_text(' ', strip=True)
                            if txt:
                                authors_list.append(txt.strip())
                    if authors_list:
                        # Normalize and remove any empty fragments
                        cleaned = [a.strip() for a in authors_list if a and a.strip()]
                        if cleaned:
                            authors = ', '.join(cleaned)
            except Exception:
                pass

            # 4) Abstract: prefer meta[name="abstract"] first (site includes
            # an abstract meta tag in the header). If absent, prefer the body
            # div `div.field--name-body.field__item`, then the div immediately
            # after the publication time element. Other fallbacks are used as
            # final resorts.
            try:
                if soup:
                    # 4a) Meta abstract
                    m_abs = soup.find('meta', attrs={'name': 'abstract'})
                    if m_abs and m_abs.get('content'):
                        seed = m_abs.get('content').strip()
                        # Try to find a body div that contains the seed. Prefer the
                        # body div when it contains the seed and its text is longer
                        # than the meta seed (this captures cases where the meta is
                        # an abbreviated summary but the full paragraph is in the body).
                        body_candidates = soup.select('div.field--name-body')
                        chosen_body = None
                        for bc in body_candidates:
                            bc_txt = bc.get_text(' ', strip=True)
                            if seed in bc_txt:
                                chosen_body = bc
                                break
                        if chosen_body:
                            # prefer the body div content if it provides more text
                            bc_txt = chosen_body.get_text(' ', strip=True)
                            if len(bc_txt) >= len(seed):
                                abstract = bc_txt
                            else:
                                # fallback to seed
                                abstract = seed
                        else:
                            # If no body div matched, look for any p/div containing the seed
                            found_seed = soup.find(lambda tag: tag.name in ('p', 'div') and seed in tag.get_text(' ', strip=True))
                            if found_seed:
                                # Walk ancestors to find a div whose classes include
                                # 'field--name-body' or 'field__item'. fall back to
                                # nearest parent div with substantial text.
                                node = found_seed
                                found_anc = None
                                while node and getattr(node, 'name', None) != '[document]':
                                    if getattr(node, 'name', None) == 'div':
                                        classes = node.get('class') or []
                                        if any('field--name-body' in c for c in classes) or any('field__item' in c for c in classes):
                                            found_anc = node
                                            break
                                    node = node.parent
                                if found_anc:
                                    abstract = found_anc.get_text(' ', strip=True)
                                else:
                                    parent_div = found_seed.find_parent('div')
                                    while parent_div:
                                        txt = parent_div.get_text(' ', strip=True)
                                        if len(txt) > 200:
                                            abstract = txt
                                            break
                                        parent_div = parent_div.find_parent('div')
                                    if not abstract:
                                        abstract = seed
                    else:
                        # 4b) body div
                        body_div = soup.select_one('div.field--name-body.field__item')
                        if body_div:
                            abstract = body_div.get_text(' ', strip=True)
                        else:
                            # 4c) div after publication time
                            time_el = soup.select_one('div.field--name-field-wp-date time')
                            if time_el:
                                parent = time_el.find_parent()
                                if parent:
                                    nxt = parent.find_next_sibling('div')
                                    if nxt:
                                        abstract = nxt.get_text(' ', strip=True)
                            # 4d) other meta fallbacks
                            if not abstract:
                                for name in ('description', 'twitter:description'):
                                    m = soup.find('meta', attrs={'name': name})
                                    if m and m.get('content'):
                                        abstract = m.get('content').strip()
                                        break
                            # 4e) document search fallback
                            if not abstract:
                                found = soup.find(lambda tag: tag.name in ('p', 'div') and len(tag.get_text(' ', strip=True)) > 50)
                                if found:
                                    abstract = found.get_text(' ', strip=True)
            except Exception:
                pass

            # 5) Published date: look for time[datetime] or meta citation_publication_date
            try:
                if soup:
                    t = soup.find('time')
                    if t and t.get('datetime'):
                        published = t.get('datetime')
                    else:
                        m2 = soup.find('meta', attrs={'name': 'citation_publication_date'})
                        if m2 and m2.get('content'):
                            published = m2.get('content')
            except Exception:
                pass

            # Normalize published to date-only (YYYY-MM-DD) when possible.
            if published:
                try:
                    # Try ISO formats first; handle trailing Z (UTC) by replacing with +00:00
                    from datetime import datetime
                    s = str(published).strip()
                    try:
                        if s.endswith('Z'):
                            dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
                        else:
                            dt = datetime.fromisoformat(s)
                        published = dt.date().isoformat()
                    except Exception:
                        # Fallback to email.utils parsedate_to_datetime for RFC dates
                        try:
                            from email.utils import parsedate_to_datetime

                            dt = parsedate_to_datetime(s)
                            published = dt.date().isoformat()
                        except Exception:
                            # As a last resort, leave published as-is (string)
                            published = s
                except Exception:
                    # Keep original if normalization fails
                    pass

            # If DOI wasn't found, as a last resort construct from publication_id and suffix
            if not doi and publication_id and suffix:
                try:
                    doi = feeds_mod.normalize_doi(f"{publication_id}/{suffix}")
                except Exception:
                    doi = None

            # Upsert article row when we have a DOI
            if doi:
                try:
                    aid = eddb.upsert_article(conn, doi, title=title, authors=authors, abstract=abstract, feed_id=feed_key, publication_id=publication_id, issn=issn, published=published)
                    if aid:
                        updated += 1
                        # attach DOI to items rows matching this link/guid/url_hash
                        try:
                            cur.execute('UPDATE items SET doi = ? WHERE feed_id = ? AND link = ?', (doi, feed_key, link))
                            cur.execute('UPDATE items SET doi = ? WHERE feed_id = ? AND guid = ?', (doi, feed_key, e.get('guid') or ''))
                            try:
                                import hashlib

                                url_hash = hashlib.sha256(link.encode('utf-8')).hexdigest()
                                cur.execute('UPDATE items SET doi = ? WHERE feed_id = ? AND url_hash = ?', (doi, feed_key, url_hash))
                            except Exception:
                                pass
                            conn.commit()
                        except Exception:
                            pass
                except Exception:
                    continue
        except Exception:
            continue
    return updated
