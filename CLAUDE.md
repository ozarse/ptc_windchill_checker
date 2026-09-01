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
  sync.py               # Fetches objects by type from API and upserts into DB
  folders.py            # Recursive folder hierarchy sync (containers → folders → subfolders)
  relationships.py      # Fetches and stores per-object relationships (attachments, etc.)
  pdf.py                # PDF download and docling text extraction
  checks.py             # Loads checks.json, runs comparisons, saves results
  excel_compare.py      # Published-products export vs Windchill where-used comparison
  export.py             # CSV export for objects and check results
  lookup.py             # Interactive lookup by number, follows relationships
  models.py             # Dataclasses: WindchillObject, Folder, PDFContent, CheckResult
  dataframe.py          # Pandas helpers for notebook/exploration use
config/
  types.json            # Object type definitions (human names → Windchill types)
  checks.json           # Validation rule definitions
  containers.json       # Windchill container IDs to sync folders from
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
.venv/Scripts/oneplm --dry-run sync objects   # log API calls without making them
.venv/Scripts/oneplm -v <command>             # verbose: adds response status, timing, pagination
```

Common workflow:
```bash
oneplm auth login              # store credentials in Windows keyring
oneplm init                    # create DB tables
oneplm --dry-run sync objects  # preview the API calls sync would make
oneplm sync objects            # fetch all typed objects (documents, parts)
oneplm sync folder             # walk folder hierarchy recursively
oneplm check                   # run all validation rules
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

Seven tables in `data/oneplm.db`:

| Table | Purpose |
|---|---|
| `objects` | Windchill objects; full API response stored as `attributes_json`; `folder_id` FK to folders |
| `folders` | Windchill folder hierarchy; `parent_folder_id` self-FK; `location` stores full path |
| `relationships` | Per-object relationship metadata (attachments, DescribedBy links, DocUsageLinks, PartDocAssociations) |
| `versions` | Per-object version history from the Versions API (`sync versions`); one row per version with its State |
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
- `dry_run=True` — logs every request at INFO level and returns empty results without hitting the network

Every outgoing request is logged at INFO (URL + query params). Response status and elapsed time are logged at DEBUG (visible with `-v`).

Key endpoints:
- Documents: `v6/DocMgmt/Documents/PTC.DocMgmt.<Type>`
- Parts: `v6/ProdMgmt/Parts/PTC.ProdMgmt.ProductDefinitionPart`
- PDF content: `Documents('{id}')/PrimaryContent` and `/Attachments`
- Relationships: `Parts('{id}')/DescribedBy`, `Documents('{id}')/DocUsageLinks`, `Parts('{id}')/PartDocAssociations`
- Folders (full tree): `v6/DataAdmin/Containers('{id}')/Folders?$expand=Folders($levels=max)`
- Folder contents: `v6/DataAdmin/Containers('{id}')/Folders('{cabinet}')/Folders('{sub}')/…/FolderContents` — full ancestor chain required

### Object Types

Defined in [config/types.json](config/types.json). Each entry has:
- `human_name` — used in CLI flags and DB `type_name` column
- `windchill_type` — OData type string
- `api_endpoint` — URL path fragment
- `classify_attr` / `classify_value` — optional filter to distinguish subtypes (e.g., Config PDP vs. IFU PDP both use `ProductDefinitionPart` but differ by `ConfigurableModule.Value`; IFU Drawing additionally requires `DocTypeName == "IFU Drawing"`)

The three logical record types validated by checks are:

| Logical type | Windchill identity |
|---|---|
| `Config PDP` | `ProductDefinitionPart`, `ConfigurableModule.Display == "Yes"` |
| `IFU PDP` | `ProductDefinitionPart`, `ConfigurableModule.Display == "No"` |
| `IFU Drawing` | `PTC.DocMgmt.IFUDrawing` document, `DocTypeName == "IFU Drawing"` |

Enum attributes come back as `{"Value": <internal code>, "Display": <label>}` — e.g.
`ConfigurableModule` is `{"Value": "dynamic", "Display": "Yes"}` /
`{"Value": "standard", "Display": "No"}`, and `DefaultUnit` is
`{"Value": "as_needed", "Display": "As Needed"}` / `{"Value": "ea", "Display": "Piece"}`.
Classification and checks compare on `.Display` (the human label). Non-enum fields
like `Number`, `Name`, `DocTypeName` are plain strings.

### Folder Sync

Configured via [config/containers.json](config/containers.json) — a list of Windchill container OIDs (e.g. `"OR:wt.inf.library.WTLibrary:10115144708"`) and human labels. An optional `folder_paths` list restricts which folders have their contents fetched (all folders are still upserted):

```json
{ "id": "OR:...", "label": "My Library", "folder_paths": ["/Default/01 - Parts"] }
```

Run with `oneplm sync folder`. The sequence per container:

1. `GET /v6/DataAdmin/Containers('{id}')/Folders?$expand=Folders($levels=max)` — fetches the complete folder tree in a single call. The nested response is walked locally; each folder is upserted with `parent_folder_id` derived from its position in the tree.
2. If `folder_paths` is set, filter to folders whose full path (`location + "/" + name`) starts with a configured prefix. Note: the API `Location` field is the **parent** path, not the folder's own full path.
3. For each matching folder, `GET …/Folders('{cabinet}')/Folders('{sub}')/…/FolderContents` — the full ancestor chain of folder IDs is required in the URL. Each item is fetched in full from its type endpoint and upserted into `objects` with `folder_id` set.
4. The DB is committed after each folder, so a crash mid-run preserves progress.

Use `--containers-config <path>` to point at a different config file.

### Validation Checks

Checks are defined declaratively in [config/checks.json](config/checks.json) and executed by [checks.py](src/oneplm_ingestion/checks.py). [docs/CHECKS.md](docs/CHECKS.md) is the human-readable catalog of every check, its data prerequisites, and open items — keep it in sync when checks change. Each entry has a `kind`:

- **`attribute`** — validates attributes of individual records of one `type`. Carries a list of `assertions`, each an `attr` + `operator` (+ optional `value`, + optional `when`).
- **`relationship`** — validates a record against the records reached through a Windchill relationship. Names a source `type`, a `related_type`, and a `via` (one of `describes`, `described_by`, `uses`, `used_by`), then runs `comparisons` between source and related attributes (or against literals: `value` for the source attr, `target_value` for the related record's attr). `min_count`/`max_count` bound how many related records must exist. `on_missing` (`fail` | `skip`) controls behaviour when no related record exists; a link whose target isn't synced locally is reported distinctly from a missing link.
- **`python`** — delegates to a function registered in [registry.py](src/oneplm_ingestion/registry.py) via `@register_check("<name>")`, referenced by `function`. The escape hatch for logic that does not fit the declarative forms.
- **`excel_compare`** — compares the products that use Config PDPs (`used_by` targets; not synced as objects) against an external .xlsx export ([excel_compare.py](src/oneplm_ingestion/excel_compare.py)). Declares the file path, product/IFU column headers, and separator; missing file → skip row, not a failure. Reads the file at check time via a deferred `openpyxl` import.

The shared operator set lives in [operators.py](src/oneplm_ingestion/operators.py): `equals`, `not_equals`, `contains`, `not_contains`, `not_empty`, `is_empty`, `matches` (regex), `greater_than`, `less_than`, `greater_equal`, `less_equal`, `before`, `after`. Assertions and comparisons both support an optional `when` precondition evaluated against the source record.

Relationship checks join records **offline** from the `relationships` table. `sync relationships` resolves `described_by` / `uses` through their second navigation hop and `used_by` directly, storing each related object's real `target_id` and `target_number` — so a check like "IFU Drawing and IFU PDP share the same Number" pairs records by the actual link, not by matching Number. The `describes` direction is the stored `described_by` traversed in inverse. Re-run `sync relationships` after schema changes to populate these.

### CLI Design

All CLI commands are in [cli.py](src/oneplm_ingestion/cli.py). Heavy imports (especially `docling`) are deferred inside command functions to keep startup fast.

Global options (`--db`, `--data-dir`, `-v`, `--dry-run`) are passed via `click.pass_context` and stored in `ctx.obj`. Commands that construct `WindchillClient` read `ctx.obj["dry_run"]` and forward it.

`sync` is a Click group with two subcommands:

- `sync objects` — fetches typed objects (documents, parts) via type-specific endpoints
- `sync folder` — walks the folder hierarchy recursively and upserts folders into the DB

## Key Conventions

- **No shell calls** — all file I/O uses `pathlib.Path`; no `subprocess` or `os.system`
- **Deferred imports** — import heavy modules inside Click command functions, not at module top level
- **Explicit connections** — callers open and close `sqlite3.Connection`; DB functions never open their own connections
- **Upsert pattern** — objects, folders use `INSERT ... ON CONFLICT DO UPDATE`; check results and relationships delete-then-insert per (key, type)
- **Dot notation for attributes** — `get_nested(obj.attributes, "State.Value")` handles nested OData fields

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `ONEPLM_BASE_URL` | (required) | Windchill OData base URL |
| `ONEPLM_DB_PATH` | `data/oneplm.db` | SQLite database path |
| `ONEPLM_DATA_DIR` | `data/` | Directory for downloaded files |
| `ONEPLM_DRY_RUN` | `0` | Set to `1` to enable dry-run mode (same as `--dry-run`) |
