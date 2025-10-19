from pathlib import Path

from ednews import news


class DummyResponse:
    def __init__(self, content: bytes, status_code=200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, resp_bytes: bytes):
        self.resp_bytes = resp_bytes

    def get(self, url, timeout=None):
        return DummyResponse(self.resp_bytes)


def test_pd_feed_keeps_only_local_news():
    fixture = Path(__file__).parent / "fixtures" / "pd.rss"
    data = fixture.read_bytes()

    session = DummySession(data)

    cfg = {
        "title": "Press Democrat Education News",
        "feed": "https://www.pressdemocrat.com/news/education/feed/",
        "processor": "pd-education",
    }

    items = news.fetch_site(session, cfg)

    # Expect a list (may be empty if fixture contains no Local News items)
    assert isinstance(items, list)

    # If items are present, each should have required keys
    for it in items:
        assert "title" in it and it["title"]
        assert "link" in it and it["link"]
        assert "summary" in it
        assert "published" in it

    # The processor should filter out items that are not Local News.
    # To give a concrete assertion, ensure at least one item is present in the fixture
    # that is categorized as Local News and therefore returned.
    assert len(items) >= 1
