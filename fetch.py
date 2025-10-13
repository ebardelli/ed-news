#!/usr/bin/env python3
"""Fetch RSS/Atom feeds listed in planet.json and save items to ednews.db.

Usage: python fetch.py

This script:
- reads feeds from planet.json
- fetches them in parallel using requests
- parses with feedparser
- stores items with simple deduplication (by guid/link/title+published)
"""
import json
import sqlite3
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import feedparser
import requests


ROOT = Path(__file__).resolve().parent
PLANET = ROOT / "planet.json"
DB_PATH = ROOT / "ednews.db"


def load_feeds():
    with PLANET.open("r", encoding="utf-8") as f:
        data = json.load(f)
    feeds = data.get("feeds", {})
    # normalize to list of (key, feedurl)
    results = []
    for key, info in feeds.items():
        url = info.get("feed")
        if url:
            results.append((key, info.get("title"), url))
    return results


def init_db(conn):
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_key TEXT,
            feed_title TEXT,
            guid TEXT,
            title TEXT,
            link TEXT,
            published TEXT,
            summary TEXT,
            fetched_at TEXT,
            UNIQUE(guid, link, title, published)
        )
        """
    )
    conn.commit()


def fetch_feed(session, key, feed_title, url, timeout=20):
    """Fetch a feed URL and return parsed feed entries.

    Returns: list of dicts with fields matching DB columns.
    """
    try:
        resp = session.get(url, timeout=timeout, headers={"User-Agent": "ed-news-fetcher/1.0"})
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as e:
        return {"key": key, "title": feed_title, "url": url, "error": str(e), "entries": []}

    entries = []
    for e in parsed.entries:
        guid = e.get("id") or e.get("guid") or e.get("link") or e.get("title")
        title = e.get("title", "")
        link = e.get("link", "")
        published = e.get("published") or e.get("updated") or ""
        summary = e.get("summary", "")
        entries.append({
            "guid": guid,
            "title": title,
            "link": link,
            "published": published,
            "summary": summary,
        })

    return {"key": key, "title": feed_title, "url": url, "error": None, "entries": entries}


def save_entries(conn, feed_key, feed_title, entries):
    cur = conn.cursor()
    inserted = 0
    for e in entries:
        try:
            cur.execute(
                """
                INSERT OR IGNORE INTO items
                (feed_key, feed_title, guid, title, link, published, summary, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    feed_key,
                    feed_title,
                    e.get("guid"),
                    e.get("title"),
                    e.get("link"),
                    e.get("published") or datetime.utcnow().isoformat(),
                    e.get("summary"),
                    datetime.utcnow().isoformat(),
                ),
            )
            if cur.rowcount:
                inserted += 1
        except Exception:
            # Ignore single-row errors; continue with others
            continue
    conn.commit()
    return inserted


def main():
    feeds = load_feeds()
    if not feeds:
        print("No feeds found in planet.json", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    session = requests.Session()

    results = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = {ex.submit(fetch_feed, session, key, title, url): (key, title, url) for key, title, url in feeds}
        for fut in as_completed(futures):
            meta = futures[fut]
            try:
                res = fut.result()
            except Exception as exc:
                print(f"Error fetching {meta[2]}: {exc}")
                continue
            if res.get("error"):
                print(f"Failed: {meta[2]} -> {res['error']}")
                continue
            count = save_entries(conn, res["key"], res["title"], res["entries"])
            print(f"{res['key']}: fetched {len(res['entries'])} entries, inserted {count}")

    conn.close()


if __name__ == "__main__":
    main()
