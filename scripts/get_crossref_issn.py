#!/usr/bin/env python3
"""Lookup recent works from Crossref by ISSN and print/save JSON results.

Usage:
    scripts/get_crossref_issn.py 1234-5678
    scripts/get_crossref_issn.py 1234-5678 --rows 20 --offset 0 --out out.json

This helper re-uses `ednews.http` and `ednews.config` when available to
respect project timeouts/retries. It's intended for ad-hoc lookups and
testing; it does minimal result normalization and prints the Crossref
API `message` object.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

logger = logging.getLogger("issn_crossref")

try:
    from ednews import http as http_helper
    from ednews import config as _config
except Exception:  # pragma: no cover - optional in isolated dev envs
    http_helper = None
    _config = None


def fetch_by_issn(issn: str, rows: int = 20, offset: int = 0, timeout: float = 15.0, sort: str | None = None, order: str | None = None) -> dict | None:
    """Query Crossref /works filtering by ISSN.

    Returns the parsed JSON response (the top-level Crossref object) or None
    if the request failed.
    """
    if not issn:
        return None
    url = "https://api.crossref.org/works"
    headers = {"User-Agent": getattr(_config, 'USER_AGENT', 'ed-news-fetcher/1.0'), "Accept": "application/json"}
    params = {"filter": f"issn:{issn}", "rows": rows, "offset": offset}
    # Allow sorting (e.g. 'deposited' or 'published-online') and ordering ('asc'/'desc')
    if sort:
        params['sort'] = sort
    if order:
        params['order'] = order

    # Prefer project http helper if available (respects retries/timeouts)
    try:
        if http_helper is not None:
            connect_to = getattr(_config, 'CROSSREF_CONNECT_TIMEOUT', 5)
            read_to = timeout
            used_timeout = (connect_to, read_to)
            return http_helper.get_json(url, params=params, headers=headers, timeout=used_timeout, retries=getattr(_config, 'CROSSREF_RETRIES', 3), backoff=getattr(_config, 'CROSSREF_BACKOFF', 0.3), status_forcelist=getattr(_config, 'CROSSREF_STATUS_FORCELIST', None))
        # Fallback to requests
        import requests

        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        try:
            return resp.json()
        except Exception:
            logger.warning("Crossref returned non-JSON response: %s", resp.text[:200])
            return None
    except Exception as e:
        logger.exception("Crossref request failed: %s", e)
        return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Query Crossref works by ISSN")
    p.add_argument("issn", help="ISSN to lookup (format XXXX-XXXX or continuous digits)")
    p.add_argument("--rows", type=int, default=20, help="Number of results to return (rows)")
    p.add_argument("--offset", type=int, default=0, help="Offset for pagination")
    p.add_argument("--timeout", type=float, default=15.0, help="Request timeout in seconds")
    p.add_argument("--out", help="Write output JSON to this file instead of printing")
    p.add_argument("--sort", default="deposited", help="Field to sort by (default: deposited). Options: relevance, score, created, updated, deposited, published-online, published-print")
    p.add_argument("--order", default="desc", choices=("asc", "desc"), help="Sort order: asc or desc (default: desc)")
    args = p.parse_args(argv)

    data = fetch_by_issn(args.issn, rows=args.rows, offset=args.offset, timeout=args.timeout, sort=args.sort, order=args.order)
    if data is None:
        print(json.dumps({"error": "request_failed"}))
        return 2

    # Print only the message part which contains items and metadata
    message = data.get('message') if isinstance(data, dict) else None
    out = message if message is not None else data

    if args.out:
        with open(args.out, 'w', encoding='utf-8') as fh:
            json.dump(out, fh, indent=2, ensure_ascii=False)
        print(f"Wrote results to {args.out}")
    else:
        try:
            print(json.dumps(out, indent=2, ensure_ascii=False))
        except Exception:
            print(str(out))

    return 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(main())
