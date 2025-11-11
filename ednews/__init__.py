"""ednews package - refactored from top-level scripts.

Expose high-level functions for CLI usage.
"""

from . import config
from . import db as db
from . import cli as cli

# Import optional submodules so they are present when referenced via __all__
from . import feeds, crossref, processors, build, embeddings  # noqa: F401

# Re-export DB-level postprocessors used by tests for monkeypatching
from .processors import (
    crossref_postprocessor_db,
    edworkingpapers_postprocessor_db,
)  # noqa: F401

__all__ = [
    "config",
    "db",
    "cli",
    "feeds",
    "crossref",
    "processors",
    "build",
    "embeddings",
    # explicit postprocessor symbols (tests patch these)
    "crossref_postprocessor_db",
    "edworkingpapers_postprocessor_db",
]

__version__ = "2025.10.19"
