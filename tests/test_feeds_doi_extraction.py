import pytest
from ednews import feeds
from ednews import crossref


def test_extract_explicit_fields():
    e = {"doi": "doi:10.1000/ABC123"}
    assert feeds.extract_and_normalize_doi(e) == "10.1000/abc123"


def test_extract_from_links_and_urls():
    e = {"links": [{"href": "https://doi.org/10.2000/XYZ"}]}
    assert feeds.extract_and_normalize_doi(e) == "10.2000/xyz"

    e2 = {"link": "http://dx.doi.org/10.3000/DEF"}
    assert feeds.extract_and_normalize_doi(e2) == "10.3000/def"


def test_trailing_punctuation_and_case():
    assert feeds.normalize_doi("10.4000/GHI.") == "10.4000/ghi"
    assert feeds.normalize_doi("DOI:10.5000/JKL,") == "10.5000/jkl"


def test_doi_inside_html_summary():
    s = '<p>Read more at <a href="https://doi.org/10.6000/MNO">link</a>.</p>'
    e = {"summary": s}
    assert feeds.extract_and_normalize_doi(e) == "10.6000/mno"


def test_id_guid_contains_doi():
    e = {"id": "urn:doi:10.7000/PQR"}
    assert feeds.extract_and_normalize_doi(e) == "10.7000/pqr"


def test_plain_text_triggers_crossref_lookup(monkeypatch):
    # When the entry text looks like a title, the normalize function may
    # call crossref.query_crossref_doi_by_title. Mock the networked function
    # to return a DOI once and test caching.
    calls = {"count": 0}

    def fake_uncached(title, preferred_publication_id=None, timeout=8):
        calls["count"] += 1
        if "interesting title" in title:
            return "10.8000/RETURNED"
        return None

    monkeypatch.setattr(crossref, '_query_crossref_doi_by_title_uncached', fake_uncached)
    # Clear the cache if any
    try:
        crossref.query_crossref_doi_by_title.cache_clear()
    except Exception:
        pass

    # Title that is suitable for lookup
    res = feeds.normalize_doi("interesting title that looks like a paper")
    assert res == "10.8000/returned"
    # Call again; cached wrapper should prevent a second backend call
    res2 = feeds.normalize_doi("interesting title that looks like a paper")
    assert res2 == "10.8000/returned"
    assert calls["count"] == 1


def test_no_false_positives_for_short_text():
    assert feeds.normalize_doi("12345") is None
    assert feeds.normalize_doi("abc") is None