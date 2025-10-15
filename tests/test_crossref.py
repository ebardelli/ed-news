from ednews import crossref


def test_query_crossref_no_title(monkeypatch):
    assert crossref.query_crossref_doi_by_title('') is None


def test_fetch_crossref_metadata_invalid(monkeypatch):
    # Requesting a fake DOI should return None (network is not mocked here)
    res = crossref.fetch_crossref_metadata('10.0000/invalid-doi')
    assert res is None or isinstance(res, dict)


def test_normalize_crossref_datetime():
    # Test valid datetime strings
    assert crossref.normalize_crossref_datetime("2025-10-14T12:34:56Z") == "2025-10-14T12:34:56+00:00"
    assert crossref.normalize_crossref_datetime("2025-10-14T12:34:56") == "2025-10-14T12:34:56+00:00"

    # Test invalid datetime strings
    assert crossref.normalize_crossref_datetime("invalid-datetime") is None
    assert crossref.normalize_crossref_datetime("") is None
    assert crossref.normalize_crossref_datetime(None) is None

    # Test datetime without timezone
    assert crossref.normalize_crossref_datetime("2025-10-14T12:34:56") == "2025-10-14T12:34:56+00:00"


def test_fetch_crossref_metadata_parses_unixref(monkeypatch):
    import pathlib
    fixture_path = pathlib.Path(__file__).parent / 'fixtures' / 'crossref_unixref.xml'
    sample_xml = fixture_path.read_text(encoding='utf-8')

    class DummyResp:
        def __init__(self, content_bytes):
            self.content = content_bytes

        def raise_for_status(self):
            return None

    def fake_get(url, headers=None, timeout=None):
        return DummyResp(sample_xml.encode('utf-8'))

    monkeypatch.setattr(crossref, 'requests', type('R', (), {'get': staticmethod(fake_get)}))

    res = crossref.fetch_crossref_metadata('10.1234/example.doi')
    assert isinstance(res, dict)
    # raw should be present and contain the abstract text
    assert 'raw' in res and 'This is an example abstract.' in res['raw']
    # published should be formatted as YYYY-MM-DD
    assert res.get('published') == '2024-05-09'
    # authors should include both authors in order
    assert res.get('authors') == 'Jane Doe, John Smith'
