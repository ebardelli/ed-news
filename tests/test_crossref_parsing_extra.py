import pathlib
import json

from ednews import crossref


class DummyResp:
    def __init__(self, content_bytes, json_obj=None, headers=None):
        self.content = content_bytes
        self._json = json_obj
        self.headers = headers or {}

    def raise_for_status(self):
        return None

    def json(self):
        if self._json is not None:
            return self._json
        raise ValueError("no json")


def test_fetch_crossref_metadata_json_created(monkeypatch):
    # JSON message contains 'created' with date-parts and authors/abstract
    message = {
        "created": {"date-parts": [[2023, 7, 2]]},
        "author": [{"given": "Alice", "family": "Author"}],
        "abstract": "<jats:p>JSON abstract text.</jats:p>",
    }
    payload = {"message": message}
    content = json.dumps(payload).encode('utf-8')

    def fake_get(url, headers=None, timeout=None):
        return DummyResp(content, json_obj=payload)

    monkeypatch.setattr(crossref, 'requests', type('R', (), {'get': staticmethod(fake_get)}))

    res = crossref.fetch_crossref_metadata('10.1111/example.json')
    assert isinstance(res, dict)
    assert res.get('published') in ('2023-07-02', '2023-07-02T00:00:00+00:00')
    assert 'Alice Author' in res.get('authors')
    assert 'JSON abstract text.' in res.get('abstract')


def test_fetch_crossref_metadata_json_published_print(monkeypatch):
    # JSON message missing 'created' but has 'published-print'
    message = {
        "published-print": {"date-parts": [[2022, 12]]},
        "author": [{"given": "Bob", "family": "Writer"}],
    }
    payload = {"message": message}
    content = json.dumps(payload).encode('utf-8')

    def fake_get(url, headers=None, timeout=None):
        return DummyResp(content, json_obj=payload)

    monkeypatch.setattr(crossref, 'requests', type('R', (), {'get': staticmethod(fake_get)}))

    res = crossref.fetch_crossref_metadata('10.2222/example.json')
    assert isinstance(res, dict)
    # published should be at least year-month
    assert res.get('published') in ('2022-12', '2022-12T00:00:00+00:00')
    assert 'Bob Writer' in res.get('authors')


def test_fetch_crossref_metadata_unixref(monkeypatch):
    fixture_path = pathlib.Path(__file__).parent / 'fixtures' / 'crossref_unixref.xml'
    sample_xml = fixture_path.read_text(encoding='utf-8')

    def fake_get(url, headers=None, timeout=None):
        return DummyResp(sample_xml.encode('utf-8'))

    monkeypatch.setattr(crossref, 'requests', type('R', (), {'get': staticmethod(fake_get)}))

    res = crossref.fetch_crossref_metadata('10.1234/example.doi')
    assert isinstance(res, dict)
    assert 'raw' in res and 'This is an example abstract.' in res['raw']
    assert res.get('published', '').startswith('2024-05-09')
    assert res.get('authors') == 'Jane Doe, John Smith'
