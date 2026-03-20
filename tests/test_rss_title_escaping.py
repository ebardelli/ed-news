import re
import shutil
from pathlib import Path

import xml.etree.ElementTree as ET
import pytest

from ednews import build as build_mod


def make_article_with_title(title, published="2025-01-01T12:00:00Z"):
    return {
        "title": title,
        "link": "https://example.org/item",
        "content": "Content",
        "abstract": "Abstract",
        "published": published,
        "raw": {"published": published},
    }


def make_headline_with_title(title, published="2025-01-01"):
    return {
        "id": 1,
        "title": title,
        "link": "https://example.org/h",
        "text": "Text",
        "published": published,
    }


@pytest.mark.usefixtures("tmp_path")
def test_rss_titles_are_escaped(monkeypatch, tmp_path):
    # Title containing reserved XML characters & and <
    special_title = "Book Review: Ruge, J & Thomas, S. <Inquiry>"

    articles = [make_article_with_title(special_title)]
    headlines = [make_headline_with_title(special_title)]

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

    # For each feed file, ensure item <title> contents are XML-escaped
    for fname in ("research.rss", "headlines.rss", "index.rss"):
        fp = out_dir / fname
        assert fp.exists(), f"{fname} not written"
        txt = fp.read_text(encoding="utf-8")

        # Extract <item>...</item> blocks and capture their <title> contents
        matches = re.findall(r"<item>.*?<title>(.*?)</title>.*?</item>", txt, re.DOTALL)
        assert matches, f"no item titles found in {fname}"

        for raw_title in matches:
            # Raw title text in the XML should contain the escaped ampersand and lt
            assert "&amp;" in raw_title, f"ampersand not escaped in {fname}: {raw_title}"
            assert "&lt;" in raw_title, f"less-than not escaped in {fname}: {raw_title}"
            # Allow standard XML entities; ensure no other unescaped '&' remains
            cleaned = raw_title
            for ent in ("&amp;", "&lt;", "&gt;", "&quot;", "&apos;"):
                cleaned = cleaned.replace(ent, "")
            assert "&" not in cleaned, f"unescaped & in {fname}: {raw_title}"

        # Also parse with an XML parser to verify the element text equals the original title
        root = ET.fromstring(txt)
        items = root.findall('.//item')
        for it in items:
            title_elem = it.find('title')
            assert title_elem is not None
            assert title_elem.text == special_title
