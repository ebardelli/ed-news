import pytest

import types

from ednews import crossref


def test_query_crossref_doi_by_title_positional(monkeypatch):
    # Patch the internal uncached function to avoid network
    def fake_uncached(title, preferred_publication_id=None, timeout=8):
        assert title == "My Title"
        assert preferred_publication_id == "10.1234"
        assert timeout == 5
        return "10.1234/xyz"

    # Replace the cached wrapper so our fake is actually called
    monkeypatch.setattr(crossref, "_query_crossref_doi_by_title_cached", fake_uncached)

    # Call wrapper positionally
    out = crossref.query_crossref_doi_by_title("My Title", "10.1234", 5)
    assert out == "10.1234/xyz"


def test_query_crossref_doi_by_title_keyword(monkeypatch):
    def fake_uncached(title, preferred_publication_id=None, timeout=8):
        assert title == "Other Title"
        assert preferred_publication_id is None
        assert timeout == 8
        return "10.9999/abc"

    monkeypatch.setattr(crossref, "_query_crossref_doi_by_title_cached", fake_uncached)

    out = crossref.query_crossref_doi_by_title(title="Other Title")
    assert out == "10.9999/abc"


def test_fetch_crossref_metadata_positional_and_keyword(monkeypatch):
    # Patch the internal implementation to avoid network and DB
    def fake_impl(doi, timeout=10, conn=None, force=False):
        assert doi == "10.5555/foo"
        assert timeout == 2
        assert conn is None
        assert force is True
        return {"authors": "A B", "abstract": "X"}

    monkeypatch.setattr(crossref, "_fetch_crossref_metadata_impl", fake_impl)

    # Positional call
    out = crossref.fetch_crossref_metadata("10.5555/foo", 2, None, True)
    assert isinstance(out, dict)
    assert out.get('authors') == "A B"

    # Keyword call
    out2 = crossref.fetch_crossref_metadata(doi="10.5555/foo", timeout=2, force=True)
    assert isinstance(out2, dict)
    assert out2.get('abstract') == "X"
