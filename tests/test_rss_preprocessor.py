import pytest

from ednews.processors import rss


def test_rss_preprocessor_returns_entries(monkeypatch):
    # fake fetch_feed returns a dict with entries
    def fake_fetch(session, key, feed_title, url, publication_doi=None, issn=None):
        return {"entries": [{"title": "one"}, {"title": "two"}]}

    monkeypatch.setattr(rss, "feeds", rss.feeds)
    monkeypatch.setattr(rss.feeds, "fetch_feed", fake_fetch)

    out = rss.rss_preprocessor(session=None, feed_url="https://example.org/feed", publication_id="pid", issn="1234")
    assert isinstance(out, list)
    assert len(out) == 2
    assert out[0]["title"] == "one"


def test_rss_preprocessor_handles_non_dict(monkeypatch):
    # If fetch_feed returns None or non-dict, we should get an empty list
    def fake_fetch(session, key, feed_title, url, publication_doi=None, issn=None):
        return None

    monkeypatch.setattr(rss.feeds, "fetch_feed", fake_fetch)
    out = rss.rss_preprocessor(session=None, feed_url="https://example.org/feed")
    assert out == []


def test_rss_preprocessor_uses_positional_fallback(monkeypatch):
    # Simulate a fetch_feed implementation that raises TypeError when called
    # with keyword args but works when called with positional args.
    def fake_fetch(*args, **kwargs):
        if kwargs:
            raise TypeError("bad-call")
        # positional call should return a dict with entries
        return {"entries": [{"title": "positional"}]}

    monkeypatch.setattr(rss.feeds, "fetch_feed", fake_fetch)

    out = rss.rss_preprocessor(session=None, feed_url="https://example.org/feed", publication_id="pid", issn="1234")
    assert isinstance(out, list)
    assert len(out) == 1
    assert out[0]["title"] == "positional"
