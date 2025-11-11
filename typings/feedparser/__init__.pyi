"""Minimal stub for feedparser used by the project."""
from typing import Any, Dict, List, Optional


class ParsedEntry(dict):
	# dict-like; entries often accessed via .get and attribute-like keys
	pass


class ParsedFeed(dict):
	entries: List[ParsedEntry]



def parse(url_or_data: Any) -> ParsedFeed: ...

