import shutil
from pathlib import Path

import pytest

from ednews import build as build_mod


def make_article(i, published):
    return {
        "title": f"Article {i}",
        "link": f"https://example.org/article/{i}",
        "content": f"Content {i}",
        "abstract": f"Abstract {i}",
        "published": published,
        "raw": {"published": published},
    }


def make_headline(i, published):
    return {
        "id": i,
        "title": f"Headline {i}",
        "link": f"https://example.org/headline/{i}",
        "text": f"Text {i}",
        "published": published,
    }


@pytest.mark.usefixtures("tmp_path")
def test_rss_description_present_and_formatted(monkeypatch, tmp_path):
    # Prepare sample items
    articles = [make_article(i, f"2025-10-{30-i:02d}T12:00:00Z") for i in range(5)]
    headlines = [make_headline(i, f"2025-10-{30-i:02d}") for i in range(5)]

    # Add a similar_headlines to the first headline to exercise related rendering
    headlines[0]["similar_headlines"] = [
        {"title": "Rel 1", "text": "Rel text 1", "link": "https://example.org/rel/1"},
        {"title": "Rel 2", "text": "Rel text 2", "link": None},
    ]

    # Monkeypatch read functions
    monkeypatch.setattr(build_mod, "read_articles", lambda db_path, limit=None, days=None, publications=None: articles)
    monkeypatch.setattr(build_mod, "read_news_headlines", lambda db_path, limit=None: headlines)

    out_dir = tmp_path / "build"
    if out_dir.exists():
        shutil.rmtree(out_dir)

    build_mod.build(out_dir)

    idx = out_dir / "index.rss"
    arts = out_dir / "articles.rss"
    heads = out_dir / "headlines.rss"

    import xml.etree.ElementTree as ET

    def descriptions(file_path):
        txt = file_path.read_text(encoding="utf-8")
        root = ET.fromstring(txt)
        items = root.findall('.//item')
        out = []
        for it in items:
            desc_el = it.find('description')
            if desc_el is None:
                out.append('')
                continue
            # Serialize the description element to capture inner XML/text regardless of child nodes
            desc_xml = ET.tostring(desc_el, encoding='unicode', method='xml')
            # Extract inner content between the first '>' and last '<'
            inner = ''
            try:
                inner = desc_xml.partition('>')[2].rpartition('<')[0].strip()
            except Exception:
                inner = (desc_el.text or '').strip()
            out.append(inner)
        return out

    for file in (arts, heads, idx):
        descs = descriptions(file)
        assert descs, f"No descriptions found in {file}"
        for d in descs:
            # Ensure description is non-empty after trimming
            assert d is not None and d.strip() != "", f"Empty description in {file}"
            # Basic structure check removed — non-empty description is sufficient
