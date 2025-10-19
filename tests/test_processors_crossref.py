import pytest
from ednews.processors.crossref import crossref_enricher_processor
from types import SimpleNamespace


def test_crossref_enricher_adds_metadata(monkeypatch):
    # Prepare a single entry without DOI but with a title
    entries = [{
        'guid': 'g1',
        'title': 'Some Very Interesting Article Title',
        'link': 'https://example.com/article',
        'published': '',
        'summary': 'summary',
    }]

    # Patch query_crossref_doi_by_title and fetch_crossref_metadata
    def fake_query(title, preferred_publication_id=None):
        # accept any title passed in by the test
        return '10.9999/fake'

    def fake_fetch(doi):
        assert doi == '10.9999/fake'
        return {'authors': 'A B', 'abstract': 'Abs', 'published': '2020-02-02', 'raw': '<xml/>'}

    monkeypatch.setattr('ednews.crossref.query_crossref_doi_by_title', fake_query)
    monkeypatch.setattr('ednews.crossref.fetch_crossref_metadata', fake_fetch)

    out = crossref_enricher_processor(entries, session=None, publication_id=None, issn=None)
    assert isinstance(out, list)
    assert len(out) == 1
    e = out[0]
    assert e.get('doi') == '10.9999/fake'
    assert e.get('authors') == 'A B'
    assert e.get('abstract') == 'Abs'
    assert e.get('published') == '2020-02-02'
    assert e.get('crossref_raw') == '<xml/>'
