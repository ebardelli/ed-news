"""Text normalization helpers shared across ingestion, build, and maintenance."""


_MOJIBAKE_REPLACEMENTS = {
    "â€™": "’",
    "â€˜": "‘",
    "â€œ": "“",
    "â€\u009d": "”",
    "â€¦": "…",
}


def recover_mojibake(text: str | None) -> str | None:
    """Recover UTF-8 bytes that were decoded as Latin-1.

    This targets the common mojibake pattern where characters like curly quotes
    appear as sequences such as ``â€™`` in stored text.
    """
    if text is None or not isinstance(text, str) or not text:
        return text

    for source_encoding in ("cp1252", "latin-1"):
        try:
            recovered = text.encode(source_encoding).decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            continue
        if "\ufffd" not in recovered:
            return recovered

    recovered = text
    for bad, good in _MOJIBAKE_REPLACEMENTS.items():
        recovered = recovered.replace(bad, good)
    return recovered


__all__ = ["recover_mojibake"]