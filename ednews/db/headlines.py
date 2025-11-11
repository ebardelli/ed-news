"""Headlines/news helpers split from ednews.db.__init__."""
import logging, sqlite3
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from .. import config

logger = logging.getLogger("ednews.db.headlines")

def upsert_news_item(
    conn: sqlite3.Connection,
    source: str,
    title: str | None,
    text: str | None,
    link: str | None,
    published: str | None = None,
    first_seen: str | None = None,
) -> int | bool:
    if not (title or link):
        return False
    cur = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    if first_seen:
        try:
            try:
                fs_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            except Exception:
                fs_dt = parsedate_to_datetime(first_seen)
            first_seen = fs_dt.isoformat()
        except Exception:
            first_seen = now
    else:
        first_seen = now
    if published:
        try:
            s = str(published).strip()
            pub_dt = None
            try:
                pub_dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                pub_dt = None
            if pub_dt is None:
                try:
                    pub_dt = parsedate_to_datetime(s)
                except Exception:
                    pub_dt = None
            if pub_dt is None:
                for fmt in (
                    "%Y-%m-%dT%H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%d",
                    "%b %d, %Y",
                    "%B %d, %Y",
                    "%d %b %Y",
                ):
                    try:
                        pub_dt = datetime.strptime(s, fmt)
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        break
                    except Exception:
                        continue
            if pub_dt is not None:
                published = pub_dt.isoformat()
        except Exception:
            published = published
    else:
        published = config.DEFAULT_MISSING_DATE
    try:
        cur.execute(
            """
            INSERT INTO headlines (source, title, text, link, first_seen, published)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(link, title) DO UPDATE SET
                text = COALESCE(excluded.text, headlines.text),
                published = COALESCE(excluded.published, headlines.published)
            """,
            (source, title, text, link, first_seen, published),
        )
        conn.commit()
        cur.execute(
            "SELECT id FROM headlines WHERE link = ? AND title = ? LIMIT 1",
            (link, title),
        )
        row = cur.fetchone()
        return int(row[0]) if row and isinstance(row[0], int) else False
    except Exception:
        logger.exception(
            "Failed to upsert news_item source=%s title=%s link=%s", source, title, link
        )
        return False

def save_headlines(conn: sqlite3.Connection, source: str, items: list[dict]) -> int:
    if not items:
        return 0
    count = 0
    for it in items:
        try:
            title = it.get("title"); link = it.get("link")
            text = it.get("summary") or it.get("text") or None
            published = it.get("published")
            res = upsert_news_item(conn, source, title, text, link, published=published)
            if res:
                count += 1
        except Exception:
            logger.exception("Failed to save headline for source=%s item=%s", source, it)
    logger.info("Saved %d/%d headlines for source=%s", count, len(items), source)
    return count

def save_news_items(conn: sqlite3.Connection, source: str, items: list[dict]) -> int:
    return save_headlines(conn, source, items)

__all__ = ["upsert_news_item", "save_headlines", "save_news_items"]
