fetch:
    uv run ed-news fetch
    uv run ed-news issn-lookup
    uv run ed-news enrich-crossref --batch 100
    uv run ed-news embed

build:
    uv run ed-news build

serve:
    uv run ed-news serve

db:
    uv run ed-news manage-db run-all