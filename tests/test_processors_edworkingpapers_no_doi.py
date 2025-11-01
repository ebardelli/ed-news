from ednews.processors.edworkingpapers import edworkingpapers_processor


def load_fixture(path: str) -> str:
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def test_edworkingpapers_preprocessor_no_doi():
    """The listing page preprocessor must not fabricate DOIs from internal ids.

    It should not return a 'doi' key on entry dicts; DOI discovery is handled
    by the article-page postprocessor.
    """
    html = load_fixture("tests/fixtures/edworkingpapers.html")
    entries = edworkingpapers_processor(html, base_url="https://edworkingpapers.com", publication_id="10.26300")
    assert isinstance(entries, list)
    assert len(entries) > 0
    # Ensure none of the entries include a 'doi' key populated with an
    # AI-style internal id like 'ai25-1322' or any DOI at this stage.
    for e in entries:
        assert "doi" not in e
