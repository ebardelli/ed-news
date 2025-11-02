import sqlite3

import ednews.crossref as crossref_mod


def test_crossref_prefers_publication_id_nonfirst(monkeypatch):
    """When the Crossref API returns multiple items for a title, ensure
    that a DOI matching the feed's publication_id is selected even if it
    is not the first item in the returned list.
    """
    title = "The Insurance Value of Financial Aid"

    # Simulate Crossref /works JSON returning three items where the third
    # item has the desired publication id in the DOI suffix.
    fake_resp = {
        "message": {
            "items": [
                {"DOI": "10.3386/w28669", "title": ["The Insurance Value of Financial Aid"]},
                {"DOI": "10.0000/other", "title": ["Insurance Value of Aid"]},
                {"DOI": "10.1162/edfp_a_00442", "title": ["The Insurance Value of Financial Aid"]},
            ]
        }
    }

    def fake_get_json(url, params=None, headers=None, timeout=None, retries=None, backoff=None, status_forcelist=None, requests_module=None):
        return fake_resp

    import ednews.http as http_mod
    monkeypatch.setattr(http_mod, 'get_json', fake_get_json)

    # Also patch fetch_crossref_metadata to avoid network calls when postprocessors run
    def fake_fetch(doi, timeout=10, conn=None, force=False):
        return {'authors': 'A', 'abstract': 'abs'}

    monkeypatch.setattr(crossref_mod, 'fetch_crossref_metadata', fake_fetch)

    # Preferred short publication id (suffix match expected)
    preferred = 'edfp'

    # Clear cached wrapper if present
    try:
        crossref_mod.query_crossref_doi_by_title.cache_clear()
    except Exception:
        pass

    found = crossref_mod._query_crossref_doi_by_title_uncached(title, preferred_publication_id=preferred)
    assert found is not None
    assert found.lower().startswith('10.1162') or 'edfp' in found.lower()
