import re
from datetime import datetime, timezone

from ednews.cli import normalize_cli_date


def test_preserve_date_fragments():
    assert normalize_cli_date('2025') == '2025'
    assert normalize_cli_date('2025-04') == '2025-04'
    assert normalize_cli_date('2025-04-01') == '2025-04-01'


def test_datetime_without_timezone_treated_as_utc():
    res = normalize_cli_date('2025-01-02T03:04')
    # Should parse and append +00:00 timezone
    assert re.match(r'^2025-01-02T03:04:00\+00:00$', res)


def test_datetime_with_z_preserved_or_parsed():
    res = normalize_cli_date('2025-01-02T03:04:05Z')
    # Accept either Z or +00:00 timezone forms
    assert res in ('2025-01-02T03:04:05+00:00', '2025-01-02T03:04:05Z')


def test_none_returns_none():
    assert normalize_cli_date(None) is None
