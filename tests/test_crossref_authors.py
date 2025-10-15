import json
from pathlib import Path


def test_crossref_fixture_authors_string():
    fixture = Path(__file__).resolve().parent / 'fixtures' / '10.1016-j.jmathb.2025.101284.json'
    data = json.loads(fixture.read_text(encoding='utf-8'))
    msg = data.get('message', {})
    authors = msg.get('author', [])
    names = []
    for a in authors:
        given = a.get('given') or ''
        family = a.get('family') or ''
        if given or family:
            names.append(' '.join([p for p in (given.strip(), family.strip()) if p]))
    authors_str = ', '.join(names)
    assert authors_str == 'George Kinnear, Matthew Inglis'
