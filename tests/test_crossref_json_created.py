import json
from pathlib import Path

from ednews import crossref


def test_fetch_crossref_metadata_prefers_json_created(monkeypatch):
    fixture = Path(__file__).resolve().parent / 'fixtures' / 'crossref_json_created_fixture.json'
    data_text = fixture.read_text(encoding='utf-8')

    class DummyResp:
        def __init__(self, content_bytes, is_json=True):
            self._content = content_bytes
            self.is_json = is_json

        def raise_for_status(self):
            return None

        def json(self):
            if self.is_json:
                return json.loads(self._content.decode('utf-8'))
            raise ValueError("not json")

        @property
        def content(self):
            return self._content

    def fake_get(url, headers=None, timeout=None):
        return DummyResp(data_text.encode('utf-8'))

    monkeypatch.setattr(crossref, 'requests', type('R', (), {'get': staticmethod(fake_get)}))

    res = crossref.fetch_crossref_metadata('10.1000/jsoncreated')
    assert isinstance(res, dict)
    # created date-parts = [2025,9,22] should be preferred over published-print 2026-03
    assert res.get('published') == '2025-09-22'
    assert 'authors' in res and 'Alice' in res['authors']
    assert 'abstract' in res and 'JSON abstract' in res['abstract']
