from ednews import feeds


def test_normalize_doi_simple():
    assert feeds.normalize_doi('10.1234/abc.1') == '10.1234/abc.1'


def test_normalize_doi_with_prefix():
    assert feeds.normalize_doi('doi:10.5678/xyz') == '10.5678/xyz'
    assert feeds.normalize_doi('https://doi.org/10.5678/xyz') == '10.5678/xyz'


def test_extract_doi_from_entry_id():
    entry = {'id': 'urn:doi:10.9999/entry1'}
    assert feeds.extract_doi_from_entry(entry) == '10.9999/entry1'
