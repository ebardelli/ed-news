"""HTTP helpers with retry/backoff/jitter and simple metrics for ed-news.

This module centralizes logic for performing HTTP GET requests with
configurable timeouts, retries, and exponential backoff. It exposes
convenience helpers for JSON and text responses and records simple
metrics counters that can be logged by callers.
"""
from __future__ import annotations

import logging
import time
import random
from typing import Any

import requests
import json

logger = logging.getLogger("ednews.http")

# Simple in-process metrics counters (module-level). For production, you may
# want to integrate with Prometheus/statsd instead.
metrics = {
    'requests_total': 0,
    'requests_failed': 0,
    'requests_retried': 0,
}


def _inc(metric: str, n: int = 1):
    try:
        metrics[metric] += n
    except Exception:
        pass


def request_with_retries(method: str, url: str, params=None, headers=None, timeout=(5, 30), retries: int = 3, backoff: float = 0.3, status_forcelist=None, requests_module=None):
    """Perform an HTTP request with simple retry/backoff/jitter.

    Args:
        method: HTTP method (only 'GET' supported currently)
        url: URL to request
        params: optional query params
        headers: optional headers
        timeout: tuple (connect, read) or single float read timeout
        retries: number of retry attempts (total attempts = retries+1)
        backoff: base backoff factor for exponential backoff
        status_forcelist: iterable of status codes considered retryable

    Returns:
        requests.Response on success

    Raises:
        Exception from last failed attempt when retries exhausted
    """
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]

    attempts = max(1, int(retries) + 1)
    last_exc = None
    for attempt in range(1, attempts + 1):
        _inc('requests_total', 1)
        try:
            req = requests_module if requests_module is not None else requests
            # Some tests monkeypatch a module with a .get method rather than
            # a .request convenience; support both by preferring .request.
            if hasattr(req, 'request'):
                resp = req.request(method, url, params=params, headers=headers, timeout=timeout)
            else:
                # fall back to module.get for GET requests
                if method.upper() == 'GET' and hasattr(req, 'get'):
                    # Some monkeypatched test helpers don't accept `params` kwarg;
                    # only include it when not None to preserve compatibility.
                    if params is None:
                        resp = req.get(url, headers=headers, timeout=timeout)
                    else:
                        resp = req.get(url, params=params, headers=headers, timeout=timeout)
                else:
                    raise RuntimeError('requests module does not support request/get')
            # Retry on certain status codes (if available), otherwise use raise_for_status when present
            try:
                status_code = getattr(resp, 'status_code', None)
            except Exception:
                status_code = None
            if status_code is not None and status_code in status_forcelist:
                last_exc = requests.HTTPError(f"status={status_code}")
                _inc('requests_retried', 1)
                raise last_exc
            # If the response object provides raise_for_status, call it to let it raise
            if hasattr(resp, 'raise_for_status'):
                try:
                    resp.raise_for_status()
                except Exception:
                    # propagate as HTTPError-like to trigger retry handling below
                    raise
            return resp
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, requests.HTTPError) as e:
            last_exc = e
            _inc('requests_retried', 1)
            logger.debug("HTTP request attempt %d/%d failed for %s: %s", attempt, attempts, url, e)
            if attempt < attempts:
                # exponential backoff with jitter
                sleep_for = backoff * (2 ** (attempt - 1))
                sleep_for = sleep_for * (0.8 + random.random() * 0.4)
                time.sleep(sleep_for)
                continue
            else:
                _inc('requests_failed', 1)
                logger.warning("HTTP request failed after %d attempts for %s: %s", attempts, url, e)
                raise


def get_json(url: str, params=None, headers=None, timeout=(5, 30), retries: int = 3, backoff: float = 0.3, status_forcelist=None, requests_module=None) -> Any:
    resp = request_with_retries('GET', url, params=params, headers=headers, timeout=timeout, retries=retries, backoff=backoff, status_forcelist=status_forcelist, requests_module=requests_module)
    try:
        if hasattr(resp, 'json'):
            return resp.json()
        # Fallback: if resp has _content or content, try to parse it
        body = None
        if hasattr(resp, '_content'):
            body = resp._content
        elif hasattr(resp, 'content'):
            body = resp.content
        if body:
            try:
                return json.loads(body.decode('utf-8'))
            except Exception:
                return None
        return None
    except Exception:
        return None


def get_text(url: str, params=None, headers=None, timeout=(5, 30), retries: int = 3, backoff: float = 0.3, status_forcelist=None, requests_module=None) -> str:
    resp = request_with_retries('GET', url, params=params, headers=headers, timeout=timeout, retries=retries, backoff=backoff, status_forcelist=status_forcelist, requests_module=requests_module)
    try:
        if hasattr(resp, 'text'):
            return resp.text
        if hasattr(resp, '_content'):
            b = resp._content
            try:
                return b.decode('utf-8', errors='replace')
            except Exception:
                return str(b)
        if hasattr(resp, 'content'):
            b = resp.content
            try:
                return b.decode('utf-8', errors='replace')
            except Exception:
                return str(b)
        return ''
    except Exception:
        return ''
