"""Tests for the CalMatters education preprocessor."""

import pytest
import requests
from ednews.processors.calmatters import _parse_page, calmatters_preprocessor


FIXTURE_PATH = "tests/fixtures/calmatters.html"


def load_fixture() -> bytes:
    with open(FIXTURE_PATH, "rb") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Parser unit tests (no network, no DB)
# ---------------------------------------------------------------------------

def test_parse_page_returns_entries():
    entries = _parse_page(load_fixture())
    assert len(entries) >= 5


def test_parse_page_first_entry_fields():
    entries = _parse_page(load_fixture())
    first = entries[0]
    assert first["title"] == "Top science research jobs lack diversity. A California college program aims to curb that"
    assert first["link"] == "https://calmatters.org/education/2026/07/stem-phd-diversity/"
    assert first["guid"] == first["link"]
    assert first["published"] == "2026-07-02"
    assert "STEM" in first["summary"] or "diversity" in first["summary"].lower()


def test_parse_page_all_entries_have_required_fields():
    entries = _parse_page(load_fixture())
    for e in entries:
        assert e.get("title"), f"missing title: {e}"
        assert e.get("link"), f"missing link: {e}"
        assert e.get("guid"), f"missing guid: {e}"


def test_parse_page_published_iso_format():
    entries = _parse_page(load_fixture())
    import re
    for e in entries:
        pub = e.get("published", "")
        if pub:
            assert re.match(r"\d{4}-\d{2}-\d{2}", pub), f"bad date format: {pub!r}"


def test_parse_page_no_author_in_summary():
    entries = _parse_page(load_fixture())
    for e in entries:
        assert "By " not in (e.get("summary") or "")


def test_parse_page_empty_html():
    assert _parse_page(b"<html><body></body></html>") == []


# ---------------------------------------------------------------------------
# Preprocessor integration: single-page mock (no DB)
# ---------------------------------------------------------------------------

class MockResponse:
    def __init__(self, content: bytes, status_code: int = 200):
        self.content = content
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


def test_preprocessor_returns_entries(monkeypatch):
    monkeypatch.setattr("ednews.processors.pagination.open_db_conn", lambda: None)

    session = requests.Session()
    monkeypatch.setattr(session, "get", lambda url, **kw: MockResponse(load_fixture()))

    entries = calmatters_preprocessor(session, "https://calmatters.org/category/education/")
    assert len(entries) >= 5


def test_preprocessor_paginates_when_first_page_has_new(monkeypatch):
    """Fetches page 2 when page 1 has new entries (DB returns None → no DB check)."""
    monkeypatch.setattr("ednews.processors.pagination.open_db_conn", lambda: None)

    page2_html = (
        "<html><body>"
        '<div class="story-info">'
        '<a class="story-title" href="https://calmatters.org/education/2026/06/page2-article/">Page 2 article</a>'
        '<div class="story-excerpt default-blurb">Page 2 summary.</div>'
        "<div class=\"story-meta\">By Author Name • June 15, 2026</div>"
        "</div></body></html>"
    ).encode("utf-8")

    call_urls = []

    def fake_get(url, **kw):
        call_urls.append(url)
        if "page/2" in url:
            return MockResponse(page2_html)
        return MockResponse(load_fixture())

    session = requests.Session()
    monkeypatch.setattr(session, "get", fake_get)

    entries = calmatters_preprocessor(session, "https://calmatters.org/category/education/")

    assert any("page/2" in u for u in call_urls), "should have fetched page 2"
    assert any(e["link"] == "https://calmatters.org/education/2026/06/page2-article/" for e in entries)


def test_preprocessor_stops_when_all_known(monkeypatch):
    """Stops after page 1 when all entries are already in the DB."""
    import sqlite3
    from ednews.db import init_db

    conn = sqlite3.connect(":memory:")
    init_db(conn)

    # Pre-populate headlines with all fixture links so has_new_entries returns False
    page1_entries = _parse_page(load_fixture())
    cur = conn.cursor()
    for e in page1_entries:
        cur.execute(
            "INSERT OR IGNORE INTO headlines (source, title, link, first_seen) VALUES (?, ?, ?, ?)",
            ("calmatters-education", e["title"], e["link"], "2026-07-01"),
        )
    conn.commit()

    monkeypatch.setattr("ednews.processors.pagination.open_db_conn", lambda: conn)

    call_urls = []

    def fake_get(url, **kw):
        call_urls.append(url)
        return MockResponse(load_fixture())

    session = requests.Session()
    monkeypatch.setattr(session, "get", fake_get)

    calmatters_preprocessor(session, "https://calmatters.org/category/education/")

    assert not any("page/2" in u for u in call_urls), "should NOT have fetched page 2"


def test_preprocessor_handles_fetch_error(monkeypatch):
    monkeypatch.setattr("ednews.processors.pagination.open_db_conn", lambda: None)

    session = requests.Session()
    monkeypatch.setattr(session, "get", lambda url, **kw: MockResponse(b"", 503))

    entries = calmatters_preprocessor(session, "https://calmatters.org/category/education/")
    assert entries == []
