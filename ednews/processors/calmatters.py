import logging
import re

from bs4 import BeautifulSoup
from dateutil import parser as dateutil_parser

from .pagination import paginate_wordpress

logger = logging.getLogger(__name__)


def _parse_page(content: bytes):
    soup = BeautifulSoup(content, "html.parser")
    entries = []
    for item in soup.select("div.story-info"):
        title_el = item.select_one("a.story-title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        link = title_el.get("href", "")
        excerpt_el = item.select_one("div.story-excerpt")
        summary = excerpt_el.get_text(strip=True) if excerpt_el else ""
        published = ""
        meta_el = item.select_one("div.story-meta")
        if meta_el:
            meta_text = meta_el.get_text(separator=" ", strip=True)
            if "•" in meta_text:
                date_part = meta_text.split("•", 1)[1].strip()
            else:
                date_part = re.sub(r"^By\s+.+?\s{2,}", "", meta_text).strip()
            try:
                published = dateutil_parser.parse(date_part).date().isoformat()
            except Exception:
                published = date_part
        entries.append({
            "guid": link,
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
        })
    return entries


def calmatters_preprocessor(session, url, publication_id=None, issn=None):
    """Scrape CalMatters education pages with DB-aware pagination."""
    return paginate_wordpress(session, url, _parse_page)
