"""Compatibility shim for the refactored `ednews.db` package.

This file re-exports the public API from `ednews.db` package so existing
imports like `from ednews.db import upsert_article` continue to work.
"""

from .db import *  # re-export public names from package

