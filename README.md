# ptc_syncer_ingestion

CLI tool to ingest PTC Windchill PLM data, compare attributes across object types, extract PDFs, and export results.

## Installation

```bash
# Create and activate a virtual environment (required on Windows)
python -m venv .venv
.venv\Scripts\activate

# Install the package and its dependencies
pip install -e .
```

To also install notebook/exploration dependencies (pandas, Jupyter):

```bash
pip install -e ".[notebook]"
```

## Quick Start

```bash
# Set your Windchill base URL (a real shell variable — there is no .env auto-loading).
# It is read from the environment on each command, so set it in every new session
# or add it to your shell profile.
set PTC_SYNCER_BASE_URL=https://your-host/Windchill/servlet/odata     # Windows cmd
# $env:PTC_SYNCER_BASE_URL = "https://your-host/Windchill/servlet/odata"   # PowerShell
# export PTC_SYNCER_BASE_URL=https://your-host/Windchill/servlet/odata     # bash

# Store credentials (saved in the Windows keyring)
ptc_syncer auth login

# Initialize the local database
ptc_syncer init

# Sync typed objects (Config PDPs, IFU PDPs, IFU Drawings)
ptc_syncer sync objects

# Edit config/containers.json with your real container ID(s)
# then sync the folder hierarchy
ptc_syncer sync folder

# Resolve relationships (required before relationship checks)
ptc_syncer sync relationships

# Run validation checks
ptc_syncer check

# Export results
ptc_syncer export checks -o check_results.csv
```

## Commands

| Command | Description |
|---------|-------------|
| `ptc_syncer init` | Create/initialize the local SQLite database |
| `ptc_syncer status` | Show object counts, last sync times, check summaries |
| `ptc_syncer auth login` | Store Windchill credentials in the system keyring |
| `ptc_syncer auth logout` | Remove stored credentials |
| `ptc_syncer auth status` | Check if credentials are stored |
| `ptc_syncer sync objects` | Sync typed objects (documents, parts) from Windchill |
| `ptc_syncer sync relationships` | Resolve and store object relationships (needed for relationship/classification checks) |
| `ptc_syncer sync versions` | Fetch and store each object's version history (needed for the version check) |
| `ptc_syncer sync folder` | Sync folder hierarchy recursively from configured containers |
| `ptc_syncer lookup <number>` | Look up a document or part by number and show relationships |
| `ptc_syncer check` | Run validation checks (`--skip-pdf` to exclude PDF-dependent checks) |
| `ptc_syncer pdf download` | Download PDFs from Windchill (skips existing files unless changed; `--force`, `--primary-only`, `--metadata-only`) |
| `ptc_syncer pdf extract` | Extract text from downloaded PDFs using docling |
| `ptc_syncer pdf check` | Validate IFU Drawing filenames against object metadata |
| `ptc_syncer export objects` | Export synced objects to CSV |
| `ptc_syncer export checks` | Export check results to CSV |

### Global Options

These go before the subcommand:

- `--db <path>` -- Path to SQLite database (default: `data/ptc_syncer.db`)
- `--data-dir <path>` -- Directory for downloaded files (default: `data/`)
- `-v` / `--verbose` -- Enable debug logging (response status, timing, pagination)
- `--dry-run` -- Log every API call that would be made without sending any requests (also set via `PTC_SYNCER_DRY_RUN=1`)

```bash
# See exactly which API calls a sync would make, without touching Windchill
ptc_syncer --dry-run sync objects
ptc_syncer --dry-run sync folder

# Same for a lookup
ptc_syncer --dry-run lookup ABC-1234

# Add -v to also see pagination and timing details
ptc_syncer -v sync objects
```

### Sync Options

```bash
# Object sync
ptc_syncer sync objects                            # sync all typed objects
ptc_syncer sync objects --type "IFU Drawing"       # sync only one object type
ptc_syncer sync objects --full                     # ignore last_modified, re-fetch everything
ptc_syncer sync objects --types-config path/to/types.json

# Folder sync
ptc_syncer sync folder                             # sync folder hierarchy from containers.json
ptc_syncer sync folder --containers-config path/to/other.json
```

---

## Folder Sync

`ptc_syncer sync folder` walks the complete folder hierarchy for each container configured in `config/containers.json`.

### Container Configuration

Edit `config/containers.json` with your Windchill container OID(s):

```json
[
  {
    "id": "OR:wt.inf.library.WTLibrary:10115144708",
    "label": "My Library"
  }
]
```

The `id` is the Windchill OID as it appears in URLs. The `label` is only used in log output.

To limit which folders have their contents fetched, add an optional `folder_paths` list. Only folders whose full path starts with one of the listed prefixes (including all subfolders) will be synced:

```json
[
  {
    "id": "OR:wt.inf.library.WTLibrary:10115144708",
    "label": "My Library",
    "folder_paths": ["/Default/01 - Parts", "/Default/03 - Documents"]
  }
]
```

All folders are still stored in the `folders` table regardless of this filter — only content fetching is restricted.

### Traversal Strategy

The full folder tree is retrieved in a single API call using OData expansion:

1. `GET /v6/DataAdmin/Containers('{id}')/Folders?$expand=Folders($levels=max)` — returns the complete nested folder tree in one response. The tree is walked locally; each folder is upserted into the `folders` table with `parent_folder_id` set from its position in the tree.

2. For each folder (subject to `folder_paths` filtering): `GET /v6/DataAdmin/Containers('{id}')/Folders('{cabinet}')/Folders('{sub}')/…/FolderContents` — the **full ancestor chain** of folder IDs must be included in the URL. For each item returned, the full object is fetched from its type-specific endpoint and stored in the `objects` table with `folder_id` set.

Objects are committed to the database after each folder, so a crash mid-run preserves already-completed folders.

### What Gets Stored

**`folders` table** — each row has:

- `id` — Windchill folder ID
- `name`, `location` — the API `Location` field is the **parent** path (e.g. `/Default`); the folder's own full path is `location + "/" + name` (e.g. `/Default/01 - Parts`)
- `parent_folder_id` — self-referencing FK set during recursive traversal
- `container_id` — which container this folder belongs to

---

## PDF Download & Extraction

`ptc_syncer pdf download` fetches content for objects already in the local database
(sync them with `ptc_syncer sync objects` or `ptc_syncer sync folder` first). Files are
saved under the data directory in `pdfs/` (default `data/pdfs/`) and each file's
metadata is recorded in the `pdfs` table.

```bash
# Download PDFs for every object of a type
ptc_syncer pdf download --type "IFU Drawing"

# Download PDFs for a single object
ptc_syncer pdf download --object-id "OR:wt.doc.WTDocument:12345"

# Extract text from downloaded PDFs (docling)
ptc_syncer pdf extract --all
```

### Download Options

| Option | Behavior |
|---|---|
| `--type <name>` | Download content for every object of this type (a `human_name` from `types.json`). |
| `--object-id <id>` | Download content for a single object. |
| `--primary-only` | Fetch only the document's primary content and skip attachments. Skips the attachments request entirely, so it is also faster. |
| `--metadata-only` | Record each file's filename and download URL in the `pdfs` table **without** downloading the file. |
| `--force` | Re-download files even if they already exist on disk. |

Windchill object IDs contain characters that are illegal in Windows filenames
(e.g. `:`); these are sanitized to `_` when building the on-disk filename. The
original ID is still stored in the `pdfs.object_id` column.

### Skipping and Re-downloading

By default the download is **resumable**: any file that already exists on disk is
skipped (logged as `Skipping existing …`), so re-running after an interruption
only fetches what is missing.

A file that already exists is still re-downloaded automatically when the object
changed in Windchill since it was last pulled — that is, when the object's
`LastModified` is newer than the file's stored `downloaded_at`. When this
happens the file is refreshed and its stale extracted text is cleared so the next
`ptc_syncer pdf extract` re-processes it. Use `--force` to re-download unconditionally.

Because change detection compares against each object's `LastModified`, refresh
the objects first so the timestamps are current:

```bash
ptc_syncer sync objects                          # refreshes each object's LastModified
ptc_syncer pdf download --type "IFU Drawing"     # re-pulls only drawings that changed
```

`--metadata-only` never touches disk, so it ignores `--force` and change
detection; it simply refreshes the URL/filename rows.

### Filename Validation

`ptc_syncer pdf check` validates each IFU Drawing's primary-content filename against
its metadata (number, revision, and language code), storing results in
`check_results`. Populate PDF metadata first with either a full `pdf download` or
`pdf download --metadata-only`.

---

## Attribute Validation Checks

The check system validates the records ingested from Windchill. Checks are defined in `config/checks.json` and executed with `ptc_syncer check`. Results (pass/fail/skip) are saved to the database and can be exported to CSV. See [`docs/CHECKS.md`](docs/CHECKS.md) for the catalog of every check currently defined, its data prerequisites, and open items.

### Check Kinds

Every entry in `checks.json` has a `kind`. There are three:

| Kind | Validates | Add your own by |
|---|---|---|
| `attribute` | Attributes of individual records of one type | Editing JSON |
| `relationship` | A record against records reached through a Windchill relationship | Editing JSON |
| `python` | Anything — delegates to a registered Python function | Writing a function + JSON entry |

All three share the same operator set (see [Operators](#operators)) and the optional `when` precondition.

### `attribute` checks

Validate each record of one `type` against a list of `assertions`:

```json
{
  "name": "config_pdp_attributes",
  "kind": "attribute",
  "type": "Config PDP",
  "description": "Config PDP records must carry the core required attributes.",
  "assertions": [
    { "attr": "Number", "operator": "not_empty" },
    {
      "attr": "ApprovalDate",
      "operator": "not_empty",
      "when": { "attr": "State.Value", "operator": "equals", "value": "Released" }
    }
  ]
}
```

| Field | Required | Description |
|---|---|---|
| `type` | Yes | Logical record type. Must match a `human_name` in `config/types.json` (e.g. `Config PDP`, `IFU PDP`, `IFU Drawing`). |
| `assertions` | Yes | List of `{ attr, operator, value?, when? }`. `attr` supports dot notation (`State.Value`). |

### `relationship` checks

Validate a record against the records reached through a Windchill relationship. The relationship is resolved **offline** from the local `relationships` table (run `ptc_syncer sync relationships` first):

```json
{
  "name": "ifu_drawing_matches_ifu_pdp",
  "kind": "relationship",
  "type": "IFU Drawing",
  "related_type": "IFU PDP",
  "via": "describes",
  "on_missing": "fail",
  "comparisons": [
    { "source_attr": "Name", "target_attr": "Name", "operator": "equals" },
    { "source_attr": "Number", "target_attr": "Number", "operator": "equals" }
  ]
}
```

| Field | Required | Description |
|---|---|---|
| `type` | Yes | The source record type. |
| `related_type` | Yes | The type of the related records to compare against. |
| `via` | Yes | Relationship to traverse from the source: `describes`, `described_by`, `uses`, or `used_by`. |
| `on_missing` | No | `fail` (default) records a failure when no related record exists; `skip` ignores it. When the link exists in Windchill but the target was never synced locally, the failure message says so explicitly. |
| `comparisons` | Yes* | List of `{ source_attr?, target_attr?, operator, value?, target_value?, when? }`. *Optional when `min_count`/`max_count` is set. |
| `min_count` / `max_count` | No | Bounds on how many related records must exist (e.g. `"min_count": 1` = "at least one related record"). Emits one pass/fail row per source record. |

The relationship directions for the three record types:

- IFU Drawing `describes` → IFU PDP &nbsp;&nbsp;(and IFU PDP `described_by` → IFU Drawing)
- IFU PDP `used_by` → Config PDP &nbsp;&nbsp;(and Config PDP `uses` → IFU PDPs)

Records are paired by the actual Windchill link, **not** by matching Number — so comparing `Number` for equality is meaningful.

### `python` checks

For logic that does not fit the declarative forms, register a function and reference it by name:

```python
# in any module imported by registry.py's load_builtin_checks
from ptc_syncer_ingestion.registry import register_check
from ptc_syncer_ingestion.models import CheckResult

@register_check("my_custom_check")
def my_custom_check(conn) -> list[CheckResult]:
    ...  # query the DB, return CheckResult rows
```

```json
{ "name": "My Custom Check", "kind": "python", "function": "my_custom_check" }
```

### `excel_compare` checks

Compare the products that use Config PDPs (their `used_by` targets — the products
themselves don't need to be synced as objects) against an external Excel export,
e.g. the bulk export from the IFU publishing website:

```json
{
  "name": "published_products_match",
  "kind": "excel_compare",
  "file": "data/published_products.xlsx",
  "product_column": "Reforcatalognumber",
  "ifu_column": "Product groups",
  "ifu_separator": "|"
}
```

| Field | Required | Description |
|---|---|---|
| `file` | Yes | Path to the .xlsx export. If the file is missing the check emits a `skip` row instead of failing. |
| `product_column` | Yes | Header of the column whose values equal Windchill part Numbers of the products. |
| `ifu_column` | Yes | Header of the column listing the product's IFU numbers. |
| `ifu_separator` | No | Separator between IFU numbers within the cell (default `\|`). |
| `sheet` | No | Worksheet name (default: first sheet). |
| `type` | No | Whose `used_by` targets are the products (default `Config PDP`). |
| `ifu_type` | No | The type whose Numbers the export lists (default `IFU PDP`). |

The check fails a row for: a Windchill product missing from the export, a product
whose exported IFU set differs from the IFU PDP numbers reachable in Windchill
(product → its Config PDPs → `uses`), and an exported product no Config PDP is
used by. Requires `ptc_syncer sync relationships` to have populated `used_by`/`uses`.

### Comparison Fields

Each entry in an `attribute` check's `assertions` or a `relationship` check's `comparisons` has:

| Field | Required | Description |
|-------|----------|-------------|
| `attr` / `source_attr` | Yes* | Attribute to read from the (source) record. Supports dot notation (e.g., `State.Value`). *In a relationship comparison, may be omitted when asserting only on the related record via `target_attr` + `target_value`. |
| `operator` | Yes | The comparison operator. See the operator table below. |
| `target_attr` | No | (relationship only) Attribute to read from the related record. Required for cross-record comparisons like `equals`. |
| `value` | No | A literal value to compare against the **source** attribute. Required for `matches`, numeric, and date operators. When both `target_attr` and `value` are present, `value` takes precedence. |
| `target_value` | No | (relationship only) A literal value to compare against the **related** record's `target_attr` — e.g. `{ "target_attr": "State.Display", "operator": "equals", "target_value": "Released" }` asserts on the related record directly. |
| `when` | No | A precondition evaluated against the source record. If not met, the comparison is recorded with status `skip` (never counts as a failure). |

Note: attribute values are read as strings — booleans arrive as `"True"`/`"False"`, so
compare them with `{ "operator": "equals", "value": "True" }`. Enum attributes are
`{"Value", "Display"}` objects; compare on `.Display` (the human label) per project convention.

Every result row carries a `status` of `pass`, `fail`, or `skip` (in the DB, CSV export,
and CLI summaries). `checks.json` is validated when loaded — unknown operators, a bad
`via`, malformed comparisons, or duplicate check names fail fast with a list of problems.

### Operators

#### String

| Operator | Description | Needs `target_attr` or `value`? |
|----------|-------------|------|
| `equals` | Source equals target (or literal value) | Yes |
| `not_equals` | Source does not equal target (or literal value) | Yes |
| `contains` | Source string contains target/value as a substring | Yes |
| `not_contains` | Source string does NOT contain target/value | Yes |

#### Presence

| Operator | Description | Needs `target_attr` or `value`? |
|----------|-------------|------|
| `not_empty` | Source attribute is present and non-blank | No |
| `is_empty` | Source attribute is absent or blank | No |

#### Regex

| Operator | Description | Needs `value`? |
|----------|-------------|------|
| `matches` | Source matches the regex pattern in `value` (full match) | Yes (regex pattern) |

#### Numeric

| Operator | Description | Needs `value`? |
|----------|-------------|------|
| `greater_than` | Source (as number) > value | Yes (number) |
| `less_than` | Source (as number) < value | Yes (number) |
| `greater_equal` | Source (as number) >= value | Yes (number) |
| `less_equal` | Source (as number) <= value | Yes (number) |

#### Date

| Operator | Description | Needs `value`? |
|----------|-------------|------|
| `before` | Source date is before value | Yes (ISO date, e.g., `2024-01-01`) |
| `after` | Source date is after value | Yes (ISO date) |

### Conditional Checks with `when`

Add a `when` block to any comparison to make it conditional. The comparison only runs if the precondition is met. If it is not met, the result is SKIP (counts as pass).

The `when` block evaluates against the **source** object and supports the same operators.

```json
{
  "source_attr": "ApprovalDate",
  "operator": "not_empty",
  "when": {
    "attr": "State.Value",
    "operator": "equals",
    "value": "Released"
  }
}
```

This reads: *"Only check that ApprovalDate is not empty **when** State.Value is Released."*

### Attribute Access

Attributes are accessed from the full Windchill API response stored for each object. Use dot notation to access nested fields:

- `Number` -- top-level field
- `State.Value` -- nested field (OData enum type)
- `ConfigurableModule.Value` -- nested field

To discover what attributes are available, use the exploration notebook or export objects to CSV:

```bash
ptc_syncer export objects -o objects.csv
```

### Available Types

Types are defined in `config/types.json`. The default types are:

| Type Name | Windchill Type |
|-----------|---------------|
| Config PDP | `PTC.ProdMgmt.ProductDefinitionPart` (ConfigurableModule = Yes) |
| IFU PDP | `PTC.ProdMgmt.ProductDefinitionPart` (ConfigurableModule = No) |
| IFU Drawing | `PTC.DocMgmt.IFUDrawing` (DocTypeName = "IFU Drawing") |
| Product Design | `PTC.DocMgmt.ProductDesign` |

### Examples

**Relationship equality** — an IFU Drawing and the IFU PDP it describes must share Name and Number:

```json
{
  "name": "ifu_drawing_matches_ifu_pdp",
  "kind": "relationship",
  "type": "IFU Drawing",
  "related_type": "IFU PDP",
  "via": "describes",
  "on_missing": "fail",
  "comparisons": [
    { "source_attr": "Name", "target_attr": "Name", "operator": "equals" },
    { "source_attr": "Number", "target_attr": "Number", "operator": "equals" }
  ]
}
```

**Conditional presence** — Released Config PDPs must have an ApprovalDate:

```json
{
  "name": "released_config_needs_approval_date",
  "kind": "attribute",
  "type": "Config PDP",
  "assertions": [
    {
      "attr": "ApprovalDate",
      "operator": "not_empty",
      "when": { "attr": "State.Value", "operator": "equals", "value": "Released" }
    }
  ]
}
```

**Regex pattern** — IFU PDP number must follow a standard format:

```json
{
  "name": "ifu_pdp_number_format",
  "kind": "attribute",
  "type": "IFU PDP",
  "assertions": [
    { "attr": "Number", "operator": "matches", "value": "^[A-Z]{2,4}-\\d{4,6}$" }
  ]
}
```

**Relationship coverage** — every IFU PDP must be used by a Config PDP:

```json
{
  "name": "ifu_pdp_used_by_config_pdp",
  "kind": "relationship",
  "type": "IFU PDP",
  "related_type": "Config PDP",
  "via": "used_by",
  "on_missing": "fail",
  "comparisons": [
    { "source_attr": "Number", "target_attr": "Number", "operator": "not_empty" }
  ]
}
```

### Running Checks

```bash
# Run all checks
ptc_syncer check

# Run a specific check by name
ptc_syncer check --check ifu_drawing_matches_ifu_pdp

# Run multiple specific checks
ptc_syncer check --check config_pdp_attributes --check ifu_pdp_attributes

# Skip checks that need PDF data (marked "requires_pdf") when PDFs aren't downloaded
ptc_syncer check --skip-pdf

# Wipe all previous results (removes stale/orphan rows) before running
ptc_syncer check --clear

# Use a different config file
ptc_syncer check --checks-config path/to/my_checks.json

# Export results to CSV
ptc_syncer export checks -o check_results.csv

# Export only failures
ptc_syncer export checks --failed-only -o failures.csv
```

Checks that read the `pdfs` table are marked `"requires_pdf": true` in
`config/checks.json`. `--skip-pdf` excludes them, so you can run the attribute,
relationship, and classification checks before any PDFs are downloaded. See
[`docs/CHECKS.md`](docs/CHECKS.md) for the full catalog and each check's data
prerequisites.

### Check Results

Each comparison produces a result with:

- **check_name** -- Which rule produced this result
- **source_object_id / target_object_id** -- The objects compared
- **source_attr / target_attr** -- The attributes compared
- **source_value / target_value** -- The actual values
- **passed** -- `true` or `false`
- **message** -- Human-readable result (e.g., `PASS: 'ClassA' == 'ClassA'`, `FAIL: value is empty`, `SKIP: precondition not met`)

---

## Interactive Exploration (Jupyter Notebook)

A starter notebook is included at `notebooks/exploration.ipynb` for interactively exploring your data and prototyping rules before formalizing them into `checks.json`.

### Setup

```bash
pip install -e ".[notebook]"
jupyter notebook notebooks/exploration.ipynb
```

### What the Notebook Covers

1. **Database status** -- See what has been synced and when
2. **Explore objects by type** -- Load objects as DataFrames, inspect available columns
3. **Find missing values** -- Identify attributes with nulls/blanks
4. **Prototype checks** -- Test conditional logic and cross-type comparisons with pandas
5. **Review check results** -- Inspect pass/fail summaries after running `ptc_syncer check`
6. **Formalize rules** -- Template for converting notebook findings into JSON rules

### DataFrame Helpers

The `ptc_syncer_ingestion.dataframe` module provides reusable functions for loading data:

```python
from ptc_syncer_ingestion.dataframe import load_objects, load_check_results, load_sync_log, load_pdfs

# Load all objects with attributes expanded into columns
df = load_objects("data/ptc_syncer.db")

# Load a specific type
parts = load_objects("data/ptc_syncer.db", type_name="IFU PDP")

# Load without expanding the JSON attributes column
raw = load_objects("data/ptc_syncer.db", expand_attributes=False)

# Load check results
results = load_check_results("data/ptc_syncer.db")
failures = load_check_results("data/ptc_syncer.db", failed_only=True)
```

### Typical Workflow

1. **Sync** data from Windchill: `ptc_syncer sync`
2. **Explore** in the notebook -- find patterns, missing values, mismatches
3. **Write a rule** in `config/checks.json` based on what you found
4. **Run** the check: `ptc_syncer check --check your_rule_name`
5. **Export** results: `ptc_syncer export checks -o results.csv`
6. Repeat
