#!/usr/bin/env python3
"""Find the most common DOI per feed in the ednews.db and add it to planet.json.

Usage:
  python scripts/update_planet_dois.py           # show suggestions
  python scripts/update_planet_dois.py --apply   # apply suggestions to planet.json (creates a backup)
  python scripts/update_planet_dois.py --feed lni --apply   # only update one feed
  python scripts/update_planet_dois.py --min-count 3      # only propose DOIs seen at least 3 times

This script is conservative: by default it only prints suggestions. Use --apply to modify planet.json.
"""
from pathlib import Path
import sqlite3
import json
import argparse
from datetime import datetime
import re
import sys

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "ednews.db"
PLANET_PATH = ROOT / "planet.json"
BACKUP_SUFFIX = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def normalize_doi(s: str) -> str | None:
    if not s:
        return None
    doi = str(s).strip()
    doi = re.sub(r"^(doi:\s*|https?://(dx\.)?doi\.org/)", "", doi, flags=re.IGNORECASE)
    doi = doi.split("#", 1)[0].split("?", 1)[0]
    doi = doi.strip(' "\'<>[]()')
    if not doi:
        return None
    # best-effort: return core if contains 10.<digits>/
    m = re.search(r"(10\.\d{4,9}/\S+)", doi)
    if m:
        core = m.group(1)
        return core.rstrip(' .;,)/]')
    return doi


def extract_publication_id(doi: str) -> str | None:
    """Return the publication id portion of a DOI (everything before the first '/')"""
    if not doi:
        return None
    d = str(doi).strip()
    if not d:
        return None
    # ensure we operate on the normalized core
    core = normalize_doi(d) or d
    if '/' in core:
        return core.split('/', 1)[0].strip()
    return core.strip()


def longest_common_prefix(strs: list[str]) -> str:
    """Return the longest common prefix of a list of strings."""
    if not strs:
        return ""
    # start with the shortest string as upper bound
    s1 = min(strs, key=len)
    s2 = max(strs, key=len)
    for i, ch in enumerate(s1):
        if ch != s2[i]:
            return s1[:i]
    return s1


def get_feeds_from_planet():
    if not PLANET_PATH.exists():
        print(f"planet.json not found at {PLANET_PATH}")
        sys.exit(1)
    with PLANET_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    feeds = data.get("feeds", {})
    return data, feeds


def find_most_common_doi_per_feed(conn, feed_keys=None, min_count=2, min_prefix_len=6):
    cur = conn.cursor()
    results = {}
    if feed_keys:
        keys = list(feed_keys)
    else:
        cur.execute("SELECT DISTINCT feed_key FROM items")
        keys = [r[0] for r in cur.fetchall() if r[0]]

    for k in keys:
        # fetch all DOIs for this feed (include repeats so we can count occurrences)
        cur.execute("SELECT doi FROM items WHERE feed_key = ? AND doi IS NOT NULL", (k,))
        rows = [r[0] for r in cur.fetchall() if r and r[0]]
        if not rows or len(rows) < min_count:
            continue
        # normalize DOIs
        norm_rows = [normalize_doi(r) for r in rows]
        norm_rows = [r for r in norm_rows if r]
        if not norm_rows:
            continue
        # Greedy token-prefix search:
        # Build candidate prefixes by taking registrant + first N tokens of the suffix
        # (split suffix on '.') for N=1..M, count how many DOIs start with each candidate,
        # and pick the candidate with the highest match count. Tie-breaker: longer prefix (more tokens).
        from collections import Counter
        candidates = Counter()
        sample_for = {}
        for d in norm_rows:
            if '/' not in d:
                continue
            reg, suffix = d.split('/', 1)
            # tokens are the dot-separated components of the suffix
            tokens = suffix.split('.') if suffix else []
            for n in range(1, len(tokens) + 1):
                candidate = f"{reg}/" + '.'.join(tokens[:n])
                candidates[candidate] += 1
                # store a sample DOI for this candidate
                if candidate not in sample_for:
                    sample_for[candidate] = d

        if not candidates:
            # no candidate prefixes; fallback to the most common registrant
            regs = [d.split('/', 1)[0] for d in norm_rows if '/' in d]
            if regs:
                reg_counter = Counter(regs)
                reg, regcnt = reg_counter.most_common(1)[0]
                # pick a representative DOI for this registrant
                rep_counter = Counter([d for d in norm_rows if d.startswith(reg + '/')])
                representative = rep_counter.most_common(1)[0][0] if rep_counter else None
                results[k] = {"publication_id": reg, "count": regcnt, "representative_doi": representative}
            continue

        # Filter candidates by minimum prefix length and minimum count
        valid = [(cand, cnt) for cand, cnt in candidates.items() if cnt >= min_count and len(cand) >= min_prefix_len]
        if not valid:
            # no valid candidate prefixes: fallback to registrant
            regs = [d.split('/', 1)[0] for d in norm_rows if '/' in d]
            if regs:
                reg_counter = Counter(regs)
                reg, regcnt = reg_counter.most_common(1)[0]
                rep_counter = Counter([d for d in norm_rows if d.startswith(reg + '/')])
                representative = rep_counter.most_common(1)[0][0] if rep_counter else None
                results[k] = {"publication_id": reg, "count": regcnt, "representative_doi": representative}
            continue

        # Choose best candidate: highest count, then longest prefix (by token count then string length)
        def score(item):
            cand, cnt = item
            token_count = cand.count('.')  # approximate token count
            return (cnt, token_count, len(cand))

        best = max(valid, key=score)
        best_cand, best_count = best
        representative = sample_for.get(best_cand)
        results[k] = {
            "publication_id": best_cand,
            "count": best_count,
            "representative_doi": representative,
        }
    return results


def apply_updates_to_planet(suggestions: dict, planet_json_path=PLANET_PATH):
    data, feeds = get_feeds_from_planet()
    changed = False
    for key, info in suggestions.items():
        feed_obj = feeds.get(key)
        if not feed_obj:
            # skip keys not in planet.json
            continue
        # Only write publication_id (DOI prefix). Do not write a full publication_doi.
        pub_id = info.get("publication_id")
        # If suggestions contain only a full doi, derive pub_id from it
        if not pub_id and info.get("doi"):
            pub_id = extract_publication_id(info.get("doi"))
        if not pub_id:
            continue

        # check existing publication id or derive from any stored publication_doi/journal_doi
        existing_pid = feed_obj.get("publication_id")
        if not existing_pid:
            existing = feed_obj.get("publication_doi") or feed_obj.get("journal_doi")
            if existing:
                existing_pid = extract_publication_id(existing)
        if existing_pid and str(existing_pid) == str(pub_id):
            continue

        feed_obj["publication_id"] = str(pub_id)
        changed = True
    if not changed:
        return False
    # backup
    backup_path = planet_json_path.with_name(planet_json_path.name + ".bak." + BACKUP_SUFFIX)
    planet_json_path.replace(backup_path)
    # write new planet.json
    with planet_json_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    return True


def main():
    p = argparse.ArgumentParser(description="Suggest or apply publication DOI entries into planet.json based on ednews.db")
    p.add_argument("--apply", action="store_true", help="apply changes to planet.json (creates a backup)")
    p.add_argument("--min-count", type=int, default=2, help="minimum occurrences of DOI in a feed to consider it (default 2)")
    p.add_argument("--min-prefix-len", type=int, default=6, help="minimum length for a common DOI prefix to be accepted (default 6)")
    p.add_argument("--feed", help="only consider this feed key")
    args = p.parse_args()

    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}")
        sys.exit(1)

    data, feeds_map = get_feeds_from_planet()
    feed_keys = [args.feed] if args.feed else list(feeds_map.keys())

    conn = sqlite3.connect(str(DB_PATH))
    try:
        suggestions = find_most_common_doi_per_feed(conn, feed_keys=feed_keys, min_count=args.min_count, min_prefix_len=args.min_prefix_len)
    finally:
        conn.close()

    if not suggestions:
        print("No suggestions found (try lowering --min-count or ensure DB has DOI values).")
        return

    print("Suggested publication ids to add to planet.json:")
    for k, info in sorted(suggestions.items()):
        pubid = info.get("publication_id")
        rep = info.get("representative_doi") or ""
        if not pubid and rep:
            pubid = extract_publication_id(rep)
        print(f"  {k}: publication_id={pubid} (seen {info['count']} times) representative_doi={rep}")

    if args.apply:
        ok = apply_updates_to_planet(suggestions, PLANET_PATH)
        if ok:
            print(f"Applied updates to {PLANET_PATH} (backup created).")
        else:
            print("No changes were necessary; planet.json unchanged.")


if __name__ == '__main__':
    main()
