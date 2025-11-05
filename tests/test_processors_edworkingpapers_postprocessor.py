from ednews.processors import edworkingpapers_postprocessor_db
from ednews.db import init_db
import sqlite3


class FakeResponse:
    def __init__(self, text):
        self.text = text

    def raise_for_status(self):
        return None


class FakeSession:
    def get(self, url, timeout=20):
        # Only support the specific article fixture used in tests
        if url.endswith('/ai25-1322'):
            with open('tests/fixtures/ai25-1322.html', 'r', encoding='utf-8') as fh:
                return FakeResponse(fh.read())
        raise RuntimeError('Unexpected URL in FakeSession: ' + url)


def test_edworkingpapers_postprocessor_db_fetches_and_upserts_article():
    conn = sqlite3.connect(':memory:')
    init_db(conn)

    entries = [
        {
            'title': 'The reliability of classroom observations and student surveys in non-research settings: Evidence from Argentina',
            'link': 'https://edworkingpapers.com/edworkingpapers/ai25-1322',
            'guid': 'ai25-1322',
            'published': '2025-11-01T12:00:00Z',
            'summary': 'There is a growing consensus on the need to measure teaching effectiveness',
        }
    ]

    session = FakeSession()
    updated = edworkingpapers_postprocessor_db(conn, 'edwp', entries, session=session, publication_id='10.26300')
    assert updated == 1

    cur = conn.cursor()
    cur.execute("SELECT doi, title, authors, published FROM articles WHERE feed_id = 'edwp'")
    rows = cur.fetchall()
    assert len(rows) == 1
    doi, title, authors, published = rows[0]
    # DOI should be the one extracted from the article page
    assert doi == '10.26300/nvmr-5e94'
    assert 'Alejandro J. Ganimian' in authors
    assert 'Andrew D. Ho' in authors
    # The postprocessor normalizes published to a date-only ISO string
    assert published == '2025-11-01'
    # The full abstract should have been extracted from the article page
    cur.execute("SELECT abstract FROM articles WHERE feed_id = 'edwp'")
    abstr = cur.fetchone()[0]
    assert abstr is not None
    expected = (
        "There is a growing consensus on the need to measure teaching effectiveness using multiple instruments. "
        "Yet, guidance on how to achieve reliable ratings derives largely from formal research in high-income countries. "
        "We study the reliability of classroom observations and student surveys conducted by practitioners in a middle-income country. "
        "Both instruments can achieve relatively high reliability (0.6–0.8 on a 0–1 scale) when averaged across raters and occasions, "
        "but the reliability of observations varies widely (from 0.4 to 0.8) based mostly on how raters are assigned to teachers. "
        "We use Generalizability Theory to estimate how reliability improves by increasing the number of times teachers are observed or the number of respondents to surveys. "
        "We recommend that practitioners design their teacher feedback systems based on analyses of their own data, instead of assuming that instruments and rubrics will generate scores with the same reliability as research settings."
    )
    # Normalize whitespace for comparison
    assert " ".join(abstr.split()) == " ".join(expected.split())
