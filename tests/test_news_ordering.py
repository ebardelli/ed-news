from pathlib import Path
import sqlite3
from datetime import datetime

from ednews import news
from ednews import db as eddb
from ednews import build


class DummyResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class DummySession:
    def __init__(self, resp_text):
        self.resp_text = resp_text

    def get(self, url, timeout=None):
        return DummyResponse(self.resp_text)


def parse_short_date(s):
    if not s:
        return None
    from email.utils import parsedate_to_datetime
    try:
        # Try RFC/email parsing first
        dt = parsedate_to_datetime(s)
        if dt is not None:
            return dt
    except Exception:
        pass
    try:
        return datetime.fromisoformat(s)
    except Exception:
        pass
    # Try common display format like 'Fri, 17 Oct 2025'
    try:
        return datetime.strptime(s, "%a, %d %b %Y")
    except Exception:
        return None


def test_headlines_sorted_by_date(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "fcmat.html"
    html = fixture.read_text(encoding="utf-8")

    # prepare DB file
    db_path = tmp_path / "news.db"
    conn = sqlite3.connect(str(db_path))
    eddb.init_db(conn)

    # fetch and persist using dummy session
    session = DummySession(html)
    results = news.fetch_all(session=session, conn=conn)
    assert "fcmat" in results
    items = results["fcmat"]
    assert len(items) >= 3

    # close connection to simulate build reading from DB file
    conn.close()

    headlines = build.read_news_headlines(db_path, limit=20)
    assert len(headlines) >= 3

    # parse dates and ensure non-increasing order (newest first)
    parsed = [parse_short_date(h.get("published")) for h in headlines]
    # Filter out None values at the end
    filtered = [d for d in parsed if d is not None]
    assert filtered, "No parseable dates found in headlines"
    for i in range(1, len(filtered)):
        assert filtered[i] <= filtered[i - 1], f"Headlines not sorted: {filtered[i-1]} < {filtered[i]}"
