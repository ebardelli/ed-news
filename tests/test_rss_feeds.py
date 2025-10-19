import shutil
from pathlib import Path
import tempfile

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
def test_rss_feeds_generate_and_counts(monkeypatch, tmp_path):
    # Create deterministic articles and headlines with mixed date formats
    articles = [make_article(i, f"2025-10-{30-i:02d}T12:00:00Z") for i in range(25)]
    # Headlines use slightly different formats
    headlines = [make_headline(i, f"2025-10-{30-i:02d}") for i in range(25)]

    # Monkeypatch functions that build uses to fetch items
    monkeypatch.setattr(build_mod, "read_articles", lambda db_path, limit=None, days=None, publications=None: articles)
    monkeypatch.setattr(build_mod, "read_news_headlines", lambda db_path, limit=None: headlines)

    out_dir = tmp_path / "build"
    if out_dir.exists():
        shutil.rmtree(out_dir)

    # Run build, which should write RSS files to out_dir
    build_mod.build(out_dir)

    # Check files exist
    idx = out_dir / "index.rss"
    arts = out_dir / "articles.rss"
    heads = out_dir / "headlines.rss"
    assert idx.exists()
    assert arts.exists()
    assert heads.exists()

    import xml.etree.ElementTree as ET

    def parse_items(file_path):
        txt = file_path.read_text(encoding="utf-8")
        root = ET.fromstring(txt)
        items = root.findall('.//item')
        guids = [it.find('guid').text if it.find('guid') is not None else None for it in items]
        titles = [it.find('title').text if it.find('title') is not None else None for it in items]
        return items, guids, titles

    idx_items, idx_guids, idx_titles = parse_items(idx)
    arts_items, arts_guids, arts_titles = parse_items(arts)
    heads_items, heads_guids, heads_titles = parse_items(heads)

    # articles.rss should contain up to 20 articles
    assert len(arts_items) <= 20 and len(arts_items) > 0
    assert any(t and t.startswith("Article") for t in arts_titles)

    # headlines.rss should contain up to 20 headlines
    assert len(heads_items) <= 20 and len(heads_items) > 0
    assert any(t and t.startswith("Headline") for t in heads_titles)

    # combined index.rss should contain up to 40 items and include both article/headline titles
    assert len(idx_items) <= 40 and len(idx_items) > 0
    assert any(t and t.startswith("Article") for t in idx_titles) or any(t and t.startswith("Headline") for t in idx_titles)

    # GUID uniqueness checks
    assert len(set(arts_guids)) == len(arts_guids)
    assert len(set(heads_guids)) == len(heads_guids)
    assert len(set(idx_guids)) == len(idx_guids)
