import re
import shutil
from pathlib import Path

import xml.etree.ElementTree as ET
import pytest

from ednews import build as build_mod


def make_article_with_link(link, published="2025-01-01T12:00:00Z"):
    return {
        "title": "Article Special",
        "link": link,
        "content": "Content",
        "abstract": "Abstract",
        "published": published,
        "raw": {"published": published},
    }


def make_headline_with_link(link, published="2025-01-01"):
    return {
        "id": 1,
        "title": "Headline Special",
        "link": link,
        "text": "Text",
        "published": published,
    }


@pytest.mark.usefixtures("tmp_path")
def test_rss_links_are_escaped(monkeypatch, tmp_path):
    # URL containing reserved XML characters &, ?, =
    special_url = "https://example.org/search?q=a&b=1=2"

    articles = [make_article_with_link(special_url)]
    headlines = [make_headline_with_link(special_url)]

    # Patch data sources used by the build process
    monkeypatch.setattr(
        build_mod,
        "read_articles",
        lambda db_path, limit=None, days=None, publications=None: articles,
    )
    monkeypatch.setattr(
        build_mod, "read_news_headlines", lambda db_path, limit=None: headlines
    )

    out_dir = tmp_path / "build"
    if out_dir.exists():
        shutil.rmtree(out_dir)

    # Run build to render RSS files
    build_mod.build(out_dir)

    # For each feed file, ensure item <link> contents are XML-escaped
    for fname in ("research.rss", "headlines.rss", "index.rss"):
        fp = out_dir / fname
        assert fp.exists(), f"{fname} not written"
        txt = fp.read_text(encoding="utf-8")

        # Extract <item>...</item> blocks and capture their <link> contents
        matches = re.findall(r"<item>.*?<link>(.*?)</link>.*?</item>", txt, re.DOTALL)
        assert matches, f"no item links found in {fname}"

        for raw_link in matches:
            # Raw link text in the XML should contain the escaped ampersand
            assert "&amp;" in raw_link, f"ampersand not escaped in {fname}: {raw_link}"
            # No unescaped '&' should remain besides the &amp; entity
            assert "&" not in raw_link.replace("&amp;", ""), f"unescaped & in {fname}: {raw_link}"

        # Also parse with an XML parser to verify the element text equals the original URL
        root = ET.fromstring(txt)
        items = root.findall('.//item')
        for it in items:
            link_elem = it.find('link')
            assert link_elem is not None
            assert link_elem.text == special_url
