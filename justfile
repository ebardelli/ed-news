fetch:
    uv run ednews fetch
    # issn-lookup: --from-date/--until-date accept YYYY, YYYY-MM, YYYY-MM-DD, or datetimes like YYYY-MM-DDTHH:MM (no-tz -> UTC)
    uv run ednews issn-lookup
    # Crossref enrichment moved to per-feed postprocessors (see ednews.processors.crossref)
    uv run ednews embed

build:
    uv run ednews build

serve:
    uv run ednews serve

test target="":
    {{ if target == "crossref" { "RUN_CROSSREF_INTEGRATION=1" } else { "" } }} uv run pytest -q

db:
    uv run ednews manage-db run-all

get-db:
    s3cmd get --force s3://ed-news-cache/ednews.db .

put-db:
    s3cmd put ednews.db s3://ed-news-cache/