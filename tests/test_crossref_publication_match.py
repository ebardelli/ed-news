def test_query_crossref_title_prefers_publication(monkeypatch):
    """When Crossref returns multiple items for a title, prefer a DOI that
    starts with the preferred publication_id prefix.
    """
    fake_resp = {"message": {"items": [{"DOI": "10.1111/one"}, {"DOI": "10.3333/target"}]}}

    def fake_get_json(url, params=None, headers=None, timeout=None, retries=None, backoff=None, status_forcelist=None, requests_module=None):
        return fake_resp

    import ednews.http as http_mod
    monkeypatch.setattr(http_mod, 'get_json', fake_get_json)

    from ednews.crossref import _query_crossref_doi_by_title_uncached

    # Preferred publication_id should select the matching DOI
    res = _query_crossref_doi_by_title_uncached('Some title', preferred_publication_id='10.3333')
    assert res == '10.3333/target'

    # Without a preferred prefix, the first item is chosen
    res2 = _query_crossref_doi_by_title_uncached('Some title', preferred_publication_id=None)
    assert res2 == '10.1111/one'
