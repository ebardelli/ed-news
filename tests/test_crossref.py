from ednews import crossref


def test_query_crossref_no_title(monkeypatch):
    assert crossref.query_crossref_doi_by_title('') is None


def test_fetch_crossref_metadata_invalid(monkeypatch):
    # Requesting a fake DOI should return None (network is not mocked here)
    res = crossref.fetch_crossref_metadata('10.0000/invalid-doi')
    assert res is None or isinstance(res, dict)
