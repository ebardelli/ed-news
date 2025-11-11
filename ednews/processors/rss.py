"""Generic RSS/Atom preprocessor wrapper.

This module exposes `rss_preprocessor(session, feed_url, publication_id=None, issn=None)`
which delegates to `ednews.feeds.fetch_feed` and returns the `entries` list so it
can be used as a drop-in preprocessor for RSS/Atom feeds.
"""

from typing import List
from .. import feeds


def rss_preprocessor(
    session, feed_url: str, publication_id: str | None = None, issn: str | None = None
) -> List[dict]:
    """Fetch an RSS/Atom feed and return the parsed entries list.

    This simply wraps `ednews.feeds.fetch_feed` and returns the `entries` list
    to match preprocessor semantics used by the CLI.
    """
    # Ensure we pass the feed URL as the `url` parameter to `fetch_feed`.
    # Use explicit keywords to avoid positional-argument mismatches if
    # `fetch_feed` signature changes.
    if hasattr(feeds, "fetch_feed"):
        try:
            res = feeds.fetch_feed(
                session=session,
                key="",
                feed_title=None,
                url=feed_url,
                publication_doi=publication_id,
                issn=issn,
            )
        except TypeError:
            # Backwards-compatible fallback: older fetch_feed signatures may
            # have different parameter ordering. Call with positional args
            # using the local names we have.
            res = feeds.fetch_feed(session, "", "", feed_url, publication_id, issn)
    else:
        res = {}
    # fetch_feed returns a dict; ensure we return a list
    if isinstance(res, dict):
        return res.get("entries") or []
    return []
