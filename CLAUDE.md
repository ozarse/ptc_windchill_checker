# CLAUDE.md — oneplm_ingestion

## Project Overview

CLI tool that ingests PTC Windchill PLM data into a local SQLite database, runs attribute validation checks, downloads/extracts PDFs, and exports results.

- **Python 3.12**, Click CLI, SQLite, keyring, docling, requests
- Entry point: `oneplm` CLI (installed via `pip install -e .`)

## Repository Layout

```
src/oneplm_ingestion/   # All source modules
  cli.py                # Click entry point and all subcommands
  api.py                # WindchillClient — HTTP requests to Windchill OData API
  auth.py               # Credential storage/retrieval via keyring
  db.py                 # SQLite schema, connection, and CRUD helpers
  sync.py               # Fetches objects from API and upserts into DB
  pdf.py                # PDF download and docling text extraction
  checks.py             # Loads checks.json, runs comparisons, saves results
  export.py             # CSV export for objects and check results
  lookup.py             # Interactive lookup by number, follows relationships
  models.py             # Dataclasses: WindchillObject, PDFContent, CheckResult
  dataframe.py          # Pandas helpers for notebook/exploration use
config/
  types.json            # Object type definitions (human names → Windchill types)
  checks.json           # Validation rule definitions
data/
  oneplm.db             # SQLite database (gitignored)
  pdfs/                 # Downloaded PDFs (gitignored)
spec/                   # Windchill OData API spec JSON files (read-only reference)
notebooks/
  exploration.ipynb     # Jupyter notebook for interactive data exploration
tests/                  # pytest test suite
```

## Development Setup

```bash
# Create and activate venv
python -m venv .venv
.venv/Scripts/activate   # Windows

# Install with dev extras
pip install -e ".[dev]"

# For notebook exploration
pip install -e ".[notebook]"

# If the entry point script is missing after install
pip install --force-reinstall --no-deps -e .
```

Set the required environment variable before running:
```bash
export ONEPLM_BASE_URL=https://your-host/Windchill/servlet/odata
```

## Running the CLI

```bash
.venv/Scripts/oneplm --help
.venv/Scripts/oneplm -v <command>   # verbose/debug logging
```

Common workflow:
```bash
oneplm auth login          # store credentials in Windows keyring
oneplm init                # create DB tables
oneplm sync                # fetch all types from Windchill
oneplm check               # run all validation rules
oneplm export checks -o results.csv
```

## Running Tests and Linting

```bash
pytest                     # run all tests
pytest --cov=oneplm_ingestion tests/
ruff check src/            # lint
ruff format src/           # format
```

Ruff is configured in `pyproject.toml`: line length 120, target Python 3.10.

## Architecture Notes

### Database

Four tables in `data/oneplm.db`:

| Table | Purpose |
|---|---|
| `objects` | Windchill objects; full API response stored as `attributes_json` |
| `pdfs` | Downloaded PDF metadata and extracted text |
| `check_results` | One row per comparison result |
| `sync_log` | Last sync timestamp per type |

Attributes are stored as a JSON blob (`attributes_json`) and accessed with dot notation (e.g., `State.Value`, `ConfigurableModule.Value`).

### API

`WindchillClient` in [api.py](src/oneplm_ingestion/api.py) wraps Windchill OData:

- Base URL from `ONEPLM_BASE_URL` env var
- Credentials from keyring (`auth.py`)
- CSRF token from `v4/PTC`
- Pagination via `@odata.nextLink`
- Incremental sync: filters by `LastModified gt <last_sync_at>`

Key endpoints:
- Documents: `v6/DocMgmt/Documents/PTC.DocMgmt.<Type>`
- Parts: `v6/ProdMgmt/Parts/PTC.ProdMgmt.ProductDefinitionPart`
- PDF content: `Documents('{id}')/PrimaryContent` and `/Attachments`
- Relationships: `Parts('{id}')/DescribedBy`, `Documents('{id}')/DocUsageLinks`

### Object Types

Defined in [config/types.json](config/types.json). Each entry has:
- `human_name` — used in CLI flags and DB `type_name` column
- `windchill_type` — OData type string
- `api_endpoint` — URL path fragment
- `classify_attr` / `classify_value` — optional filter to distinguish subtypes (e.g., Config Options PDP vs. Part PDP both use `ProductDefinitionPart` but differ by `ConfigurableModule.Value`)

### Validation Checks

Rules are defined in [config/checks.json](config/checks.json) and executed by [checks.py](src/oneplm_ingestion/checks.py).

Each rule pairs objects of a `source_type` and `target_type` by a `match_on` attribute (usually `Number`), then runs one or more comparisons. Supported operators: `equals`, `not_equals`, `contains`, `not_contains`, `not_empty`, `is_empty`, `matches` (regex), `greater_than`, `less_than`, `greater_equal`, `less_equal`, `before`, `after`.

Comparisons support an optional `when` precondition evaluated against the source object.

### CLI Design

All CLI commands are in [cli.py](src/oneplm_ingestion/cli.py). Heavy imports (especially `docling`) are deferred inside command functions to keep startup fast.

Global options (`--db`, `--data-dir`, `-v`) are passed via `click.pass_context` and stored in `ctx.obj`.

## Key Conventions

- **No shell calls** — all file I/O uses `pathlib.Path`; no `subprocess` or `os.system`
- **Deferred imports** — import heavy modules inside Click command functions, not at module top level
- **Explicit connections** — callers open and close `sqlite3.Connection`; DB functions never open their own connections
- **Upsert pattern** — objects and PDFs use `INSERT ... ON CONFLICT DO UPDATE`; check results delete-then-insert per check name
- **Dot notation for attributes** — `get_nested(obj.attributes, "State.Value")` handles nested OData fields

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ONEPLM_BASE_URL` | (required) | Windchill OData base URL |
| `ONEPLM_DB_PATH` | `data/oneplm.db` | SQLite database path |
| `ONEPLM_DATA_DIR` | `data/` | Directory for downloaded files |
