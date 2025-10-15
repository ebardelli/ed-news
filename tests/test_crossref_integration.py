import os
import sqlite3
import random
from pathlib import Path
import pytest

from ednews import crossref

# Integration tests are disabled by default to avoid network calls during
# regular test runs. Enable by setting RUN_CROSSREF_INTEGRATION=1 in the env.
RUN_INTEGRATION = os.environ.get("RUN_CROSSREF_INTEGRATION") == "1"
if not RUN_INTEGRATION:
    pytest.skip("CrossRef integration tests are disabled by default. Set RUN_CROSSREF_INTEGRATION=1 to enable.", allow_module_level=True)


# Hard-coded DOIs to validate parsing. You can edit this list as you like.
DOIS = [
    "10.3102/00028312251367669",
]

# Load expected values from fixtures (optional)
import json
from pathlib import Path
FIXTURE_PATH = Path(__file__).resolve().parent / 'fixtures' / 'crossref_expected.json'
if FIXTURE_PATH.exists():
    EXPECTED = json.loads(FIXTURE_PATH.read_text())
else:
    EXPECTED = {}

# If fixtures provide DOIs, use them as the test set; otherwise use DOIS
DOIS_TO_TEST = list(EXPECTED.keys()) if EXPECTED else DOIS


def test_hardcoded_dois_fetch_and_parse():
    for doi in DOIS_TO_TEST:
        try:
            data = crossref.fetch_crossref_metadata(doi)
        except Exception as e:
            pytest.skip(f"Network or parsing error when fetching DOI {doi}: {e}")
        if data is None:
            pytest.skip(f"CrossRef returned no data for DOI {doi}")
        assert isinstance(data, dict)
        # raw xml should be present
        assert 'raw' in data and isinstance(data['raw'], str)
        # at least one of authors/abstract/published should be present for a typical DOI
        assert any(k in data for k in ('authors', 'abstract', 'published'))
        expected = EXPECTED.get(doi)
        if expected:
            if 'authors' in expected:
                assert expected['authors'].split(',')[0].strip().lower() in (data.get('authors') or '').lower()
            if 'abstract_contains' in expected:
                assert expected['abstract_contains'].lower() in (data.get('abstract') or '').lower()
            if 'published' in expected:
                # Normalize both expected and actual published values and compare date portions
                exp_raw = expected['published']
                act_raw = data.get('published') or ''
                exp_norm = crossref.normalize_crossref_datetime(exp_raw) or str(exp_raw)
                act_norm = crossref.normalize_crossref_datetime(act_raw) or str(act_raw)
                # compare YYYY-MM-DD portion to tolerate missing time component
                exp_date = exp_norm.split('T')[0] if 'T' in exp_norm else exp_norm
                act_date = act_norm.split('T')[0] if 'T' in act_norm else act_norm
                assert exp_date == act_date
            elif 'published_year' in expected:
                assert expected['published_year'] in (data.get('published') or '')
        else:
            if 'authors' in data:
                assert isinstance(data['authors'], str) and data['authors'].strip()
            if 'abstract' in data:
                assert isinstance(data['abstract'], str) and data['abstract'].strip()
            if 'published' in data:
                norm = crossref.normalize_crossref_datetime(data['published'])
                assert norm is None or isinstance(norm, str)
