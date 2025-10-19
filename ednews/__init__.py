"""ednews package - refactored from top-level scripts.

Expose high-level functions for CLI usage.
"""
from . import config
from . import db as db

__all__ = [
    "config",
    "db",
    "feeds",
    "crossref",
    "sciencedirect",
    "build",
    "embeddings",
]

__version__ = "2025.10.19"
