import json
from pathlib import Path

from ednews.news import fcmat_processor


def test_fcmat_processor_extracts_headlines():
    fixture = Path(__file__).parent / "fixtures" / "fcmat.html"
    html = fixture.read_text(encoding="utf-8")
    items = fcmat_processor(html, base_url="https://www.fcmat.org")
    # Expect at least 3 items from the fixture
    assert isinstance(items, list)
    assert len(items) >= 3
    # Each item should have title and link
    for it in items[:3]:
        assert "title" in it and it["title"]
        assert "link" in it and it["link"]
        assert "summary" in it
        assert "published" in it
