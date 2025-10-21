fetch:
    uv run ed-news fetch
    # issn-lookup: --from-date/--until-date accept YYYY, YYYY-MM, YYYY-MM-DD, or datetimes like YYYY-MM-DDTHH:MM (no-tz -> UTC)
    uv run ed-news issn-lookup
    uv run ed-news enrich-crossref --batch 100
    uv run ed-news embed

build:
    uv run ed-news build

serve:
    uv run ed-news serve

db:
    uv run ed-news manage-db run-all