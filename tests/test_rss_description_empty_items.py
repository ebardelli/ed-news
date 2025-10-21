import shutil
from pathlib import Path

import pytest

from ednews import build as build_mod


def make_article_empty(i, published):
    # No 'abstract' or 'content'
    return {
        "title": f"Article {i}",
        "link": f"https://example.org/article/{i}",
        "published": published,
        "raw": {"published": published},
    }


def make_headline_empty(i, published):
    # No 'text'
    return {
        "id": i,
        "title": f"Headline {i}",
        "link": f"https://example.org/headline/{i}",
        "published": published,
    }


@pytest.mark.usefixtures("tmp_path")
def test_rss_description_empty_items(monkeypatch, tmp_path):
    # Prepare items that lack description content
    articles = [make_article_empty(i, f"2025-10-{30-i:02d}T12:00:00Z") for i in range(3)]
    headlines = [make_headline_empty(i, f"2025-10-{30-i:02d}") for i in range(3)]

    # Ensure no similar_headlines are present
    # Monkeypatch read functions
    monkeypatch.setattr(build_mod, "read_articles", lambda db_path, limit=None, days=None, publications=None: articles)
    monkeypatch.setattr(build_mod, "read_news_headlines", lambda db_path, limit=None: headlines)
    # Prevent embeddings code from populating similar_headlines during tests
    try:
        # Patch the specific helpers used by build() if present
        import ednews.embeddings as _emb

        monkeypatch.setattr(_emb, "find_similar_headlines_by_rowid", lambda conn, nid, top_n=5: [])
        monkeypatch.setattr(_emb, "create_headlines_vec", lambda conn: None)
    except Exception:
        # If embeddings module isn't importable in the test env, ignore
        pass

    out_dir = tmp_path / "build"
    if out_dir.exists():
        shutil.rmtree(out_dir)

    build_mod.build(out_dir)

    idx = out_dir / "index.rss"
    arts = out_dir / "articles.rss"
    heads = out_dir / "headlines.rss"

    import xml.etree.ElementTree as ET

    def descriptions_inner(file_path):
        txt = file_path.read_text(encoding="utf-8")
        root = ET.fromstring(txt)
        items = root.findall('.//item')
        out = []
        for it in items:
            desc_el = it.find('description')
            if desc_el is None:
                # missing description is acceptable for empty-content items
                out.append(None)
                continue
            # Extract inner content between the first '>' and last '<'
            desc_xml = ET.tostring(desc_el, encoding='unicode', method='xml')
            try:
                inner = desc_xml.partition('>')[2].rpartition('<')[0].strip()
            except Exception:
                inner = (desc_el.text or '').strip()
            out.append(inner)
        return out

    # For files that list these items, ensure the description is either missing or empty
    for file in (arts, heads, idx):
        descs = descriptions_inner(file)
        assert descs, f"No items found in {file}"
        for d in descs:
            # Accept either None (no <description> element) or empty string
            assert d is None or d == "", f"Expected empty/missing description for empty-content item in {file}, got: {d!r}"
