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
        link = a.get("href") or ""
        # Normalize relative links if base_url provided
        if base_url and link and link.startswith("/"):
            link = base_url.rstrip("/") + link

        date_p = b.select_one("p.date-published")
        published = date_p.get_text(separator=" ", strip=True) if date_p else ""

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

        out.append({"title": title, "link": link, "summary": summary, "published": published})

    return out
