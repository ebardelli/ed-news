import sqlite3
from ednews.processors.edworkingpapers import edworkingpapers_postprocessor_db


class FakeResp:
    def __init__(self, text):
        self.text = text
    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, html):
        self.html = html

    def get(self, url, timeout=20):
        return FakeResp(self.html)


def make_inmemory_db():
    conn = sqlite3.connect(":memory:")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            doi TEXT UNIQUE,
            title TEXT,
            authors TEXT,
            abstract TEXT,
            crossref_xml TEXT,
            feed_id TEXT,
            publication_id TEXT,
            issn TEXT,
            fetched_at TEXT,
            published TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_id TEXT,
            doi TEXT,
            guid TEXT,
            title TEXT,
            link TEXT,
            url_hash TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT
        )
        """
    )
    conn.commit()
    return conn


def test_edworkingpapers_published_date_normalized():
    # HTML where the article page has a time element with full datetime
    html = """
    <html>
    <head>
      <meta name="citation_doi" content="10.26300/ai25-1322" />
      <meta name="abstract" content="Short abstract." />
    </head>
    <body>
      <h1>Test Paper Title</h1>
      <time datetime="2025-11-05T12:00:00Z">Nov 5, 2025</time>
      <div class="field--name-body field__item">Paper body here</div>
    </body>
    </html>
    """

    conn = make_inmemory_db()
    session = FakeSession(html)
    entries = [{"link": "https://edworkingpapers.com/ai25-1322", "guid": "ai25-1322", "title": "Test Paper Title"}]

    updated = edworkingpapers_postprocessor_db(conn, "edwp", entries, session=session, publication_id="10.26300")
    assert updated == 1

    cur = conn.cursor()
    cur.execute("SELECT published FROM articles WHERE doi = ?", ("10.26300/ai25-1322",))
    row = cur.fetchone()
    assert row is not None
    published = row[0]
    # Ensure time and timezone are removed; only date remains
    assert published == "2025-11-05"
