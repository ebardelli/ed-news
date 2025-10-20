import pytest
from ednews import feeds


def test_load_feeds_no_planet(tmp_path, monkeypatch):
    # Ensure load_feeds returns empty when no planet file exists
    monkeypatch.setattr(feeds, 'config', feeds.config)
    # Temporarily point to a non-existent path via config
    monkeypatch.setattr(feeds.config, 'RESEARCH_JSON', tmp_path / 'missing.json')
    res = feeds.load_feeds()
    assert res == []


def test_fetch_feed_parses_entries(monkeypatch):
    # Use the provided fixture RSS file so feedparser actually parses entries
    from pathlib import Path
    fixture = Path(__file__).resolve().parent / "fixtures" / "aera.rss"
    assert fixture.exists(), "test fixture aera.rss must be present"
    content = fixture.read_bytes()

    class FixtureResp:
        def __init__(self, content):
            self.content = content

        def raise_for_status(self):
            return None

    session = type('S', (), {'get': lambda self, url, timeout, headers=None: FixtureResp(content)})()

    result = feeds.fetch_feed(session, 'k', 'AERJ', 'https://journals.sagepub.com/action/showFeed?ui=0&mi=ehikzz&ai=2b4&jc=aera&type=etoc&feed=rss')
    assert isinstance(result, dict)
    assert 'entries' in result
    assert isinstance(result['entries'], list)
    # The fixture should include at least one entry
    assert len(result['entries']) > 0


def test_normalize_doi():
    # Test valid DOI normalization
    assert feeds.normalize_doi("https://doi.org/10.1234/abcd") == "10.1234/abcd"
    assert feeds.normalize_doi("doi:10.1234/abcd") == "10.1234/abcd"
    assert feeds.normalize_doi("10.1234/abcd") == "10.1234/abcd"

    # Test DOI with extra characters
    assert feeds.normalize_doi("https://doi.org/10.1234/abcd?param=value") == "10.1234/abcd"
    assert feeds.normalize_doi("10.1234/abcd.") == "10.1234/abcd"

    # Test invalid DOI
    assert feeds.normalize_doi("invalid-doi") is None

    # Test empty input
    assert feeds.normalize_doi("") is None
    assert feeds.normalize_doi(None) is None
