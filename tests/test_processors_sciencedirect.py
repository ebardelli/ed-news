import pytest
from types import SimpleNamespace
from ednews.processors import sciencedirect_feed_processor

class DummySession:
    def __init__(self, resp_content):
        self._content = resp_content
    def get(self, url, timeout=None, headers=None):
        return SimpleNamespace(content=self._content, status_code=200, raise_for_status=lambda: None)


def test_sciencedirect_processor_title_lookup(monkeypatch):
    # Simulate a ScienceDirect RSS with one item lacking DOI but with title
    fake_rss = b"""
    <rss><channel>
      <item>
        <title>Test Article Title</title>
        <link>https://www.sciencedirect.com/science/article/pii/S0000000000000000</link>
        <guid>g1</guid>
        <pubDate>2020-01-01</pubDate>
        <description>Summary</description>
      </item>
    </channel></rss>
    """

    session = DummySession(fake_rss)

    # Patch crossref title lookup to return a DOI
    def fake_query(title, preferred_publication_id=None):
        assert 'Test Article Title' in title
        return '10.1234/fake.doi'

    monkeypatch.setattr('ednews.processors.sciencedirect.crossref.query_crossref_doi_by_title', fake_query)

    entries = sciencedirect_feed_processor(session, 'http://fake/feed', publication_id='10.1234', issn='0000-0000')
    assert isinstance(entries, list)
    assert len(entries) == 1
    e = entries[0]
    assert e.get('doi') == '10.1234/fake.doi'
    assert e.get('title') == 'Test Article Title'

