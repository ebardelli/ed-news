from typing import Dict, List
from bs4 import BeautifulSoup


def fcmat_processor(html: str, base_url: str | None = None) -> List[Dict]:
    """Parse FCMAT headlines page HTML and extract headline items.

    Returns a list of dicts: title, link, summary, published.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: List[Dict] = []

    container = soup.select_one("section#fcmatnewsupdates")
    if not container:
        # fallback: search whole document for the column blocks
        blocks = soup.select(".col-lg-4")
    else:
        blocks = container.select(".col-lg-4")

    for b in blocks:
        a = b.select_one("h4 a")
        if not a:
            continue
        title = a.get_text(strip=True)
        link = str(a.get("href") or "")
        # Normalize relative links if base_url provided
        if base_url and link and link.startswith("/"):
            link = base_url.rstrip("/") + link

        date_p = b.select_one("p.date-published")
        published = str(date_p.get_text(separator=" ", strip=True)) if date_p else ""

        # The summary is usually the first <p> after the date paragraph
        summary = ""
        if date_p:
            # find the next sibling paragraph inside the block
            next_p = date_p.find_next_sibling("p")
            if next_p:
                summary = next_p.get_text(strip=True)
        else:
            # fallback: first non-date paragraph
            p = b.select_one("p:not(.date-published)")
            if p:
                summary = p.get_text(strip=True)

        out.append(
            {"title": title, "link": link, "summary": summary, "published": published}
        )

    return out


# Backwards-compatible preprocessor alias
def fcmat_preprocessor(session_or_html, base_url: str | None = None):
    # Accept either a requests-like session+url signature or the raw-html signature
    # If session_or_html is a string, treat it as HTML content
    if isinstance(session_or_html, str):
        return fcmat_processor(session_or_html, base_url=base_url)
    # Otherwise we expect (session, url) signature
    try:
        session = session_or_html
        # caller will pass URL in base_url
        resp = session.get(base_url, timeout=15)
        resp.raise_for_status()
        return fcmat_processor(resp.text, base_url=base_url)
    except Exception:
        return []
