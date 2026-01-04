import os
import re
import sqlite3
import tempfile
from pathlib import Path

from ednews import build


def test_combined_articles_published_formats_parsing():
    # Representative published value formats observed in the DB
    samples = [
        ("doi-iso", "Title", "link", "Feed", "content", "2026-01-03", "auth"),
        ("doi-iso-time", "Title", "link", "Feed", "content", "2026-01-03T12:34:56Z", "auth"),
        ("doi-rfc-gmt", "Title", "link", "Feed", "content", "Tue, 30 Sep 2025 00:00:00 GMT", "auth"),
        ("doi-rfc-utc", "Title", "link", "Feed", "content", "Sat, 01 Nov 2025 00:00:00 UTC", "auth"),
        ("doi-rfc-plus", "Title", "link", "Feed", "content", "Mon, 01 Dec 2025 00:00:00 +0000", "auth"),
    ]

    # Create a temporary sqlite file and populate combined_articles table
    tmp = tempfile.NamedTemporaryFile(delete=False)
    tmp_path = tmp.name
    tmp.close()

    conn = sqlite3.connect(tmp_path)
    cur = conn.cursor()
    cur.execute(
        "CREATE TABLE combined_articles (doi TEXT, title TEXT, link TEXT, feed_title TEXT, content TEXT, published TEXT, authors TEXT)"
    )
    cur.executemany(
        "INSERT INTO combined_articles (doi, title, link, feed_title, content, published, authors) VALUES (?,?,?,?,?,?,?)",
        samples,
    )
    conn.commit()
    conn.close()

    # Use the project's read_articles to parse and format dates
    articles = build.read_articles(Path(tmp_path), limit=10)

    # Expect at least one parsed article per sample inserted
    assert len(articles) >= len(samples)

    short_date_re = re.compile(r"^[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4}$")

    seen = set()
    for a in articles:
        raw = a.get("raw", {}).get("published")
        if raw in [s[5] for s in samples]:
            seen.add(raw)
            # ensure formatted published is a short, human-readable date
            pub = a.get("published")
            assert pub is not None
            assert short_date_re.match(pub), f"unexpected short date format: {pub!r} for raw {raw!r}"

    # ensure all sample raw published strings were parsed and returned
    assert set([s[5] for s in samples]).issubset(seen)

    try:
        os.unlink(tmp_path)
    except Exception:
        pass
