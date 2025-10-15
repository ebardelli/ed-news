#!/usr/bin/env python3
"""Fetch Crossref metadata for a DOI and print JSON.

Usage:
    scripts/get_crossref.py 10.1038/s41586-020-2649-2
    scripts/get_crossref.py --raw 10.1038/s41586-020-2649-2

This is a small helper for debugging and ad-hoc checks.
"""
import sys
import argparse
import json
import urllib.parse
import logging

try:
    import requests
except Exception:  # pragma: no cover - requests should be available in dev env
    requests = None


logger = logging.getLogger("get_crossref")


def fetch_crossref(doi: str, raw: bool = False, timeout: float = 10.0):
    """Fetch Crossref /works/{doi} and return parsed JSON or raw text.

    Args:
        doi: DOI string (e.g. 10.1038/s41586-020-2649-2)
        raw: If True, return raw text of response (not parsed JSON)
        timeout: request timeout in seconds

    Returns:
        tuple(status_code:int, data:dict|str)
    """
    if requests is None:
        raise RuntimeError("requests package is required. Install with: pip install requests")

    doi_encoded = urllib.parse.quote(doi, safe='')
    url = f"https://api.crossref.org/works/{doi_encoded}"
    headers = {
        "User-Agent": "ed-news-crossref-helper/1.0 (mailto:you@example.com)"
    }

    resp = requests.get(url, headers=headers, timeout=timeout)
    if raw:
        return resp.status_code, resp.text
    try:
        return resp.status_code, resp.json()
    except Exception:
        return resp.status_code, resp.text


def main(argv=None):
    p = argparse.ArgumentParser(description="Fetch Crossref metadata for a DOI and print JSON")
    p.add_argument("doi", help="DOI to fetch (e.g. 10.1038/s41586-020-2649-2)")
    p.add_argument("--raw", action="store_true", help="Print raw response body instead of parsing JSON")
    p.add_argument("--timeout", type=float, default=10.0, help="Request timeout in seconds")
    args = p.parse_args(argv)

    try:
        status, data = fetch_crossref(args.doi, raw=args.raw, timeout=args.timeout)
    except Exception as e:
        logger.exception("request failed")
        print(json.dumps({"error": str(e)}))
        sys.exit(2)

    out = {"status": status, "data": data}
    # If raw was requested and data is a string, print it directly
    if args.raw and isinstance(data, str):
        print(data)
        return

    # Pretty-print JSON-friendly output
    try:
        print(json.dumps(out, indent=2, ensure_ascii=False))
    except Exception:
        # fallback
        print(str(out))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
