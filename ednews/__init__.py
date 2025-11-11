"""ednews package - refactored from top-level scripts.

Expose high-level functions for CLI usage.
"""

from . import config
from . import db as db
from . import cli as cli

__all__ = [
    "config",
    "db",
    "cli",
    "feeds",
    "crossref",
    "processors",
    "build",
    "embeddings",
]

__version__ = "2025.10.19"
