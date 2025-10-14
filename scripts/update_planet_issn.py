#!/usr/bin/env python3
"""Search CrossRef journals by title to find candidate ISSNs.

Usage: python scripts/issn_lookup.py "Journal Title"

Prints a short list of matching journals with ISSNs that you can copy into planet.json
"""
import sys
import requests
import logging
import os
import json
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("issn_lookup")

ROOT = Path(__file__).resolve().parent.parent
PLANET = ROOT / "planet.json"
CROSSREF_BASE = "https://api.crossref.org/journals"


def search_journals(title: str, rows: int = 10, timeout: int = 10):
    headers = {"User-Agent": "ed-news-issn-lookup/1.0", "Accept": "application/json"}
    params = {"query": title, "rows": rows}
    try:
        resp = requests.get(CROSSREF_BASE, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("message", {}).get("items", []) or []
        return items
    except Exception as e:
        logger.error("CrossRef journal search failed: %s", e)
        return []


def load_planet():
    try:
        with PLANET.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_planet(data):
    # backup
    try:
        bak = PLANET.with_suffix(".json.bak")
        if PLANET.exists():
            PLANET.replace(bak)
    except Exception:
        pass
    with PLANET.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def choose_candidate(results):
    print("Found the following matches:")
    for i, r in enumerate(results, start=1):
        journal_title = r.get("title")
        issn = r.get("ISSN") or []
        publisher = r.get("publisher")
        print(f"{i}. {journal_title}\n   ISSN(s): {', '.join(issn)}\n   Publisher: {publisher}\n")
    print("0. Cancel")
    while True:
        val = input("Select a number to pick an ISSN (or 0 to cancel): ")
        try:
            n = int(val)
        except Exception:
            print("Please enter a number")
            continue
        if n == 0:
            return None
        if 1 <= n <= len(results):
            return results[n - 1]
        print("Invalid selection")


if __name__ == "__main__":
    # Command-line options:
    # - no args: iterate all feeds in planet.json and search by their title
    # - <feed-key>: search for that feed only and write selected ISSN into that feed
    # - <title>: treat as free-form title search (no write)
    # Optional flags:
    # --force : overwrite existing issn
    # --auto  : automatically pick the first candidate and write it
    args = sys.argv[1:]
    force = False
    auto = False
    if "--force" in args:
        force = True
        args.remove("--force")
    if "--auto" in args:
        auto = True
        args.remove("--auto")

    planet = load_planet()
    feeds = planet.get("feeds", {}) if isinstance(planet, dict) else {}

    def process_search_term_for_feed(feed_key, info):
        search_term = info.get("title") or info.get("feed") or feed_key
        print(f"\nSearching for feed '{feed_key}' using title: {search_term}")
        results = search_journals(search_term, rows=20)
        if not results:
            print("  No matches found")
            return
        if auto:
            picked = results[0]
        else:
            picked = choose_candidate(results)
        if not picked:
            print("  Skipped")
            return
        issns = picked.get("ISSN") or []
        if not issns:
            print("  Selected entry had no ISSN; nothing to write")
            return
        chosen_issn = issns[0]
        print(f"  Selected ISSN: {chosen_issn}")
        # write into planet.json under feeds[feed_key].issn
        try:
            planet["feeds"][feed_key]["issn"] = chosen_issn
            save_planet(planet)
            print(f"  Wrote issn={chosen_issn} to feed {feed_key} in {PLANET}")
        except Exception as e:
            print("  Failed to write planet.json:", e)

    if not args:
        # batch mode: iterate feeds and prompt for those missing publication_id
        for fk, info in feeds.items():
            if not isinstance(info, dict):
                continue
            if info.get("issn") and not force:
                print(f"Skipping {fk} (already has issn={info.get('issn')})")
                continue
            process_search_term_for_feed(fk, info)
        print("Done")
        sys.exit(0)

    # if we have a single arg, handle accordingly
    arg = args[0]
    if arg in feeds:
        info = feeds.get(arg) or {}
        if info.get("issn") and not force:
            print(f"Feed {arg} already has issn={info.get('issn')}. Use --force to overwrite.")
            sys.exit(0)
        process_search_term_for_feed(arg, info)
        sys.exit(0)

    # Otherwise treat arg as a free-form title search, no writing
    search_term = arg
    results = search_journals(search_term, rows=20)
    if not results:
        print("No matches found")
        sys.exit(0)
    picked = choose_candidate(results)
    if not picked:
        print("Cancelled")
        sys.exit(0)
    issns = picked.get("ISSN") or []
    if not issns:
        print("Selected entry had no ISSN; nothing to write")
        sys.exit(0)
    print(f"Selected ISSN: {issns[0]}")
