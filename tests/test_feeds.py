import pytest
from ednews import feeds


def test_load_feeds_no_planet(tmp_path, monkeypatch):
    # Ensure load_feeds returns empty when no planet file exists
    monkeypatch.setattr(feeds, 'config', feeds.config)
    # Temporarily point to a non-existent path via config
    monkeypatch.setattr(feeds.config, 'PLANET_JSON', tmp_path / 'missing.json')
    res = feeds.load_feeds()
    assert res == []


def test_fetch_feed_parses_entries(monkeypatch):
    class DummyResp:
        content = b""

    session = type('S', (), {'get': lambda self, url, timeout, headers: DummyResp()})()

    # feedparser.parse will handle empty content but produce no entries
    result = feeds.fetch_feed(session, 'k', 'title', 'http://example.com/feed')
    assert isinstance(result, dict)
    assert 'entries' in result
    assert isinstance(result['entries'], list)
