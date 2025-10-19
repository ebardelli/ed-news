fetch:
    uv run python main.py fetch
    uv run python main.py issn-lookup
    uv run python main.py enrich-crossref --batch 100
    uv run python main.py embed

build:
    uv run python main.py build

serve:
    uv run python -m http.server -d build

db:
    uv run python main.py manage-db run-all