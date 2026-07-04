"""Tests for the PACE publications and commentaries processors."""

import sqlite3
import pytest
from ednews.processors import (
    edpolicyinca_publications_processor,
    edpolicyinca_publications_preprocessor,
    edpolicyinca_publications_postprocessor_db,
    edpolicyinca_commentaries_processor,
    edpolicyinca_commentaries_preprocessor,
    edpolicyinca_commentaries_postprocessor_db,
)
from ednews.db import init_db


def load_fixture(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Publications parser (live fixture)
# ---------------------------------------------------------------------------

def test_publications_processor_listing():
    html = load_fixture("tests/fixtures/edpolicyinca_listing.html")
    entries = edpolicyinca_publications_processor(html)
    assert len(entries) >= 20, f"expected at least 20 entries, got {len(entries)}"

    first = entries[0]
    assert first["title"] == "Subtraction and Substitution: The Role of Schools in Math Course-Taking"
    assert first["link"] == "https://edpolicyinca.org/publications/subtraction-and-substitution-schools"
    assert first["guid"] == "subtraction-and-substitution-schools"
    assert first["published"] == "2026-04-28T12:00:00Z"
    assert "Beryl Larson" in first["authors"]
    assert "course-taking" in first["summary"].lower()


def test_publications_processor_no_subtitle():
    html = """
    <div class="views-row">
      <div class="field--name-node-title"><h2><a href="/publications/no-subtitle">No Subtitle Article</a></h2></div>
      <div class="field--name-field-authors"><div class="field__items">
        <div class="field__item"><a href="/authors/x">Author X</a></div>
      </div></div>
      <div class="field--name-field-publication-date"><div class="field__item"><time datetime="2025-01-01T00:00:00Z">Jan 2025</time></div></div>
      <div class="field--name-body"><div class="field__item"><p>Body text here.</p></div></div>
    </div>
    """
    entries = edpolicyinca_publications_processor(html)
    assert len(entries) == 1
    assert entries[0]["title"] == "No Subtitle Article"


def test_publications_processor_summary_not_newsletter():
    html = load_fixture("tests/fixtures/edpolicyinca_listing.html")
    entries = edpolicyinca_publications_processor(html)
    for entry in entries:
        assert "newsletter" not in (entry.get("summary") or "").lower()
        assert "subscribe" not in (entry.get("summary") or "").lower()


def test_publications_processor_empty():
    assert edpolicyinca_publications_processor("") == []
    assert edpolicyinca_publications_processor("<html><body><p>nothing</p></body></html>") == []


# ---------------------------------------------------------------------------
# Commentaries parser (live fixture)
# ---------------------------------------------------------------------------

def test_commentaries_processor_listing():
    html = load_fixture("tests/fixtures/edpolicyinca_commentaries_listing.html")
    entries = edpolicyinca_commentaries_processor(html)
    assert len(entries) >= 20, f"expected at least 20 entries, got {len(entries)}"

    first = entries[0]
    assert "COVID" in first["title"]
    assert first["link"] == "https://edpolicyinca.org/newsroom/what-covid-taught-us-about-students-social-emotional-development-and-why-ca-should-rethink-how-it-provides-support"
    assert first["published"] == "2026-05-21T12:00:00Z"
    assert first["authors"] == "Yang Caroline Wang"
    assert "pandemic" in first["summary"].lower()


def test_commentaries_processor_summary_not_newsletter():
    html = load_fixture("tests/fixtures/edpolicyinca_commentaries_listing.html")
    entries = edpolicyinca_commentaries_processor(html)
    for entry in entries:
        assert "newsletter" not in (entry.get("summary") or "").lower()
        assert "subscribe" not in (entry.get("summary") or "").lower()


def test_commentaries_processor_empty():
    assert edpolicyinca_commentaries_processor("") == []


# ---------------------------------------------------------------------------
# Preprocessors (pagination using live fixtures as page 1)
# ---------------------------------------------------------------------------

class MockResponse:
    def __init__(self, text: str, status_code: int = 200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


def test_publications_preprocessor_pagination(monkeypatch):
    page1 = load_fixture("tests/fixtures/edpolicyinca_listing.html")
    page1_count = len(edpolicyinca_publications_processor(page1))

    page2 = """
    <html><body>
    <div class="views-row">
      <div class="field--name-node-title"><h2><a href="/publications/page2-article">Page 2 Article</a></h2></div>
      <div class="field--name-field-publication-date"><div class="field__item"><time datetime="2025-01-10T00:00:00Z">Jan 2025</time></div></div>
      <div class="field--name-body"><div class="field__item"><p>Page 2 summary.</p></div></div>
    </div>
    </body></html>
    """
    call_count = {"n": 0}

    class FakeScraper:
        def get(self, url, timeout=30):
            call_count["n"] += 1
            return MockResponse(page1 if call_count["n"] == 1 else page2)

    monkeypatch.setattr("ednews.processors.edpolicyinca._make_scraper", lambda s: FakeScraper())

    entries = edpolicyinca_publications_preprocessor(None, "https://edpolicyinca.org/publications")
    assert len(entries) == page1_count + 1
    assert entries[-1]["guid"] == "page2-article"


def test_commentaries_preprocessor_pagination(monkeypatch):
    page1 = load_fixture("tests/fixtures/edpolicyinca_commentaries_listing.html")
    page1_count = len(edpolicyinca_commentaries_processor(page1))

    page2 = """
    <html><body>
    <div class="views-row">
      <div class="field--name-node-title"><h2><a href="/newsroom/page2-commentary">Page 2 Commentary</a></h2></div>
      <div class="field--name-field-article-date"><div class="field__item"><time datetime="2025-01-10T00:00:00Z">Jan 10, 2025</time></div></div>
      <div class="field--name-field-summary"><div class="field__item"><p>Page 2 commentary summary.</p></div></div>
    </div>
    </body></html>
    """
    call_count = {"n": 0}

    class FakeScraper:
        def get(self, url, timeout=30):
            call_count["n"] += 1
            return MockResponse(page1 if call_count["n"] == 1 else page2)

    monkeypatch.setattr("ednews.processors.edpolicyinca._make_scraper", lambda s: FakeScraper())

    entries = edpolicyinca_commentaries_preprocessor(None, "https://edpolicyinca.org/commentaries")
    assert len(entries) == page1_count + 1
    assert entries[-1]["guid"] == "page2-commentary"


def test_preprocessor_fetch_error(monkeypatch):
    class BrokenScraper:
        def get(self, url, timeout=30):
            raise ConnectionError("network error")

    monkeypatch.setattr("ednews.processors.edpolicyinca._make_scraper", lambda s: BrokenScraper())

    assert edpolicyinca_publications_preprocessor(None, "https://edpolicyinca.org/publications") == []
    assert edpolicyinca_commentaries_preprocessor(None, "https://edpolicyinca.org/commentaries") == []


# ---------------------------------------------------------------------------
# Postprocessor (shared logic, tested via publications variant)
# ---------------------------------------------------------------------------

def test_postprocessor_reads_from_items():
    """Postprocessor reads summary and authors from items, not the entry dict."""
    conn = sqlite3.connect(":memory:")
    init_db(conn)

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO items (feed_id, guid, title, link, published, summary, authors) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            "pace_publications",
            "california-school-funding-2025",
            "California School Funding Report 2025",
            "https://edpolicyinca.org/publications/california-school-funding-2025",
            "2025-03-15",
            "An analysis of California school funding policies.",
            "Jane Smith, John Doe",
        ),
    )
    conn.commit()

    # Entry dict has only the narrow fields the fetch pipeline passes
    entries = [{"title": "California School Funding Report 2025",
                "link": "https://edpolicyinca.org/publications/california-school-funding-2025",
                "guid": "california-school-funding-2025", "published": "2025-03-15"}]

    count = edpolicyinca_publications_postprocessor_db(conn, "pace_publications", entries)
    assert count == 1

    cur.execute("SELECT authors, abstract FROM articles WHERE doi = ?",
                ("https://edpolicyinca.org/publications/california-school-funding-2025",))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == "Jane Smith, John Doe"
    assert "school funding" in row[1].lower()


def test_postprocessor_no_http_fetches(monkeypatch):
    """Postprocessor must not make any HTTP requests."""
    class SpyScraper:
        def get(self, url, timeout=30):
            raise AssertionError("postprocessor must not fetch individual pages")

    monkeypatch.setattr("ednews.processors.edpolicyinca._make_scraper", lambda s: SpyScraper())

    conn = sqlite3.connect(":memory:")
    init_db(conn)

    cur = conn.cursor()
    cur.execute(
        "INSERT INTO items (feed_id, guid, title, link, published, summary, authors) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("pace_commentaries", "some-commentary", "Some Commentary",
         "https://edpolicyinca.org/newsroom/some-commentary",
         "2026-05-01", "A commentary summary.", "Some Author"),
    )
    conn.commit()

    entries = [{"title": "Some Commentary",
                "link": "https://edpolicyinca.org/newsroom/some-commentary",
                "guid": "some-commentary", "published": "2026-05-01"}]

    count = edpolicyinca_commentaries_postprocessor_db(conn, "pace_commentaries", entries)
    assert count == 1


def test_postprocessor_empty_entries():
    conn = sqlite3.connect(":memory:")
    init_db(conn)
    assert edpolicyinca_publications_postprocessor_db(conn, "pace_publications", []) == 0
    assert edpolicyinca_commentaries_postprocessor_db(conn, "pace_commentaries", []) == 0
