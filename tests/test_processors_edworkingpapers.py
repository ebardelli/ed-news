from ednews.processors import edworkingpapers_processor


def load_fixture(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def test_edworkingpapers_processor_parses_fixture():
    html = load_fixture("tests/fixtures/edworkingpapers.html")
    items = edworkingpapers_processor(html, publication_id="10.26300")
    assert isinstance(items, list)
    # The fixture shows many items; ensure we parsed at least one and fields present
    assert len(items) > 0

    first = items[0]
    assert first.get("title") == "The reliability of classroom observations and student surveys in non-research settings: Evidence from Argentina"
    assert first.get("link").endswith("/ai25-1322")
    # The preprocessor should not construct a DOI; DOI discovery happens in the postprocessor
    assert "doi" not in first
    # The preprocessor should expose a stable guid derived from the link suffix
    assert first.get("guid") == "ai25-1322"
    assert "There is a growing consensus on the need to measure teaching effectiveness" in first.get("summary")
    assert "2025-11-01T12:00:00Z" in first.get("published")
