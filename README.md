# oneplm_ingestion

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
# Set your Windchill base URL
set ONEPLM_BASE_URL=https://your-host/Windchill/servlet/odata

# Store credentials (saved in the Windows keyring)
oneplm auth login

# Initialize the local database
oneplm init

# Sync typed objects (documents, parts)
oneplm sync objects

# Edit config/containers.json with your real container ID(s)
# then sync the folder hierarchy
oneplm sync folder

# Run validation checks
oneplm check

# Export results
oneplm export checks -o check_results.csv
```

## Commands

| Command | Description |
|---------|-------------|
| `oneplm init` | Create/initialize the local SQLite database |
| `oneplm status` | Show object counts, last sync times, check summaries |
| `oneplm auth login` | Store Windchill credentials in the system keyring |
| `oneplm auth logout` | Remove stored credentials |
| `oneplm auth status` | Check if credentials are stored |
| `oneplm sync objects` | Sync typed objects (documents, parts) from Windchill |
| `oneplm sync folder` | Sync folder hierarchy recursively from configured containers |
| `oneplm lookup <number>` | Look up a document or part by number and show relationships |
| `oneplm check` | Run validation checks against local data |
| `oneplm pdf download` | Download PDFs from Windchill |
| `oneplm pdf extract` | Extract text from downloaded PDFs using docling |
| `oneplm export objects` | Export synced objects to CSV |
| `oneplm export checks` | Export check results to CSV |

### Global Options

These go before the subcommand:

- `--db <path>` -- Path to SQLite database (default: `data/oneplm.db`)
- `--data-dir <path>` -- Directory for downloaded files (default: `data/`)
- `-v` / `--verbose` -- Enable debug logging (response status, timing, pagination)
- `--dry-run` -- Log every API call that would be made without sending any requests (also set via `ONEPLM_DRY_RUN=1`)

```bash
# See exactly which API calls a sync would make, without touching Windchill
oneplm --dry-run sync objects
oneplm --dry-run sync folder

# Same for a lookup
oneplm --dry-run lookup ABC-1234

# Add -v to also see pagination and timing details
oneplm -v sync objects
```

### Sync Options

```bash
# Object sync
oneplm sync objects                            # sync all typed objects
oneplm sync objects --type "IFU Document"      # sync only one object type
oneplm sync objects --full                     # ignore last_modified, re-fetch everything
oneplm sync objects --types-config path/to/types.json

# Folder sync
oneplm sync folder                             # sync folder hierarchy from containers.json
oneplm sync folder --containers-config path/to/other.json
```

---

## Folder Sync

`oneplm sync folder` walks the complete folder hierarchy for each container configured in `config/containers.json`.

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

### Traversal Strategy

The folder tree is walked recursively via two API endpoints:

1. `GET /v6/DataAdmin/Containers('{id}')/Folders` — fetches the top-level folders in the container
2. `GET /v6/DataAdmin/Containers('{id}')/Folders('{fid}')/Folders` — fetches subfolders of a given folder

Each folder is upserted into the `folders` table as it is discovered. `parent_folder_id` is set directly from the recursion, so hierarchy is always accurate regardless of how `Location` paths are formatted.

### What Gets Stored

**`folders` table** — each row has:

- `id` — Windchill folder ID
- `name`, `location` (full path from the API, e.g. `/Default/SubA/SubB`)
- `parent_folder_id` — self-referencing FK set during recursive traversal
- `container_id` — which container this folder belongs to

---

## Attribute Validation Checks

The check system lets you define rules that validate attributes on Windchill objects. Rules are defined in `config/checks.json` and executed with `oneplm check`.

### How It Works

1. Each rule specifies a **source type** and a **target type** (can be the same type).
2. Objects are **paired** by a shared attribute (the `match_on` field -- usually `Number`).
3. For each pair, one or more **comparisons** are run against their attributes.
4. Results (pass/fail/skip) are saved to the database and can be exported to CSV.

### Rule Structure

Every rule in `checks.json` follows this structure:

```json
{
  "name": "unique_rule_name",
  "description": "Human-readable explanation of what this rule checks",
  "source_type": "Part PDP",
  "target_type": "IFU Document",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "RegulatoryClass",
      "target_attr": "RegulatoryClass",
      "operator": "equals"
    }
  ]
}
```

**Fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `name` | Yes | Unique identifier for the rule. Used in CLI output and exports. |
| `description` | Yes | Human-readable description of the rule's purpose. |
| `source_type` | Yes | The type of object to check. Must match a `human_name` in `config/types.json`. |
| `target_type` | Yes | The type to compare against. Can be the same as `source_type` for self-checks. |
| `match_on` | Yes | The attribute used to pair source and target objects (e.g., `Number`). |
| `comparisons` | Yes | A list of one or more comparisons to run on each paired object. |

### Comparison Fields

Each entry in `comparisons` has:

| Field | Required | Description |
|-------|----------|-------------|
| `source_attr` | Yes | The attribute to read from the source object. Supports dot notation (e.g., `State.Value`). |
| `operator` | Yes | The comparison operator. See the operator table below. |
| `target_attr` | No | The attribute to read from the target object. Required for cross-object comparisons like `equals`. |
| `value` | No | A literal value to compare against. Required for `matches`, numeric, and date operators. When both `target_attr` and `value` are present, `value` takes precedence. |
| `when` | No | A precondition. If specified, the comparison is only run when the condition is met. If the condition is not met, the comparison is skipped (counts as pass). |

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
oneplm export objects -o objects.csv
```

### Available Types

Types are defined in `config/types.json`. The default types are:

| Type Name | Windchill Type |
|-----------|---------------|
| Config Options PDP | `PTC.ProdMgmt.ProductDefinitionPart` (ConfigurableModule = Yes) |
| Part PDP | `PTC.ProdMgmt.ProductDefinitionPart` (ConfigurableModule = No) |
| IFU Document | `PTC.DocMgmt.IFUDrawing` |
| Product Design | `PTC.DocMgmt.ProductDesign` |

### Examples

**Cross-type equality** -- Part PDP and IFU Document must share the same RegulatoryClass:

```json
{
  "name": "pdp_ifu_regulatory_class_match",
  "description": "Part PDP and IFU Document must have the same RegulatoryClass",
  "source_type": "Part PDP",
  "target_type": "IFU Document",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "RegulatoryClass",
      "target_attr": "RegulatoryClass",
      "operator": "equals"
    }
  ]
}
```

**Conditional presence** -- Released parts must have an ApprovalDate:

```json
{
  "name": "released_parts_need_approval_date",
  "description": "When a Part PDP is Released, its ApprovalDate must not be empty",
  "source_type": "Part PDP",
  "target_type": "Part PDP",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "ApprovalDate",
      "operator": "not_empty",
      "when": {
        "attr": "State.Value",
        "operator": "equals",
        "value": "Released"
      }
    }
  ]
}
```

**Regex pattern** -- Part number must follow a standard format:

```json
{
  "name": "part_number_format",
  "description": "Part PDP Number must match standard format",
  "source_type": "Part PDP",
  "target_type": "Part PDP",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "Number",
      "operator": "matches",
      "value": "^[A-Z]{2,4}-\\d{4,6}$"
    }
  ]
}
```

**Numeric threshold** -- Version must be greater than 0:

```json
{
  "name": "config_options_version_positive",
  "description": "Config Options PDP VersionNumber must be greater than 0",
  "source_type": "Config Options PDP",
  "target_type": "Config Options PDP",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "VersionNumber",
      "operator": "greater_than",
      "value": "0"
    }
  ]
}
```

**Date boundary** -- Documents must have been modified after a baseline date:

```json
{
  "name": "ifu_modified_after_baseline",
  "description": "IFU Documents must have been modified after the 2024-01-01 baseline",
  "source_type": "IFU Document",
  "target_type": "IFU Document",
  "match_on": "Number",
  "comparisons": [
    {
      "source_attr": "LastModified",
      "operator": "after",
      "value": "2024-01-01"
    }
  ]
}
```

### Running Checks

```bash
# Run all checks
oneplm check

# Run a specific check by name
oneplm check --check released_parts_need_approval_date

# Run multiple specific checks
oneplm check --check part_number_format --check ifu_modified_after_baseline

# Use a different config file
oneplm check --checks-config path/to/my_checks.json

# Export results to CSV
oneplm export checks -o check_results.csv

# Export only failures
oneplm export checks --failed-only -o failures.csv
```

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
5. **Review check results** -- Inspect pass/fail summaries after running `oneplm check`
6. **Formalize rules** -- Template for converting notebook findings into JSON rules

### DataFrame Helpers

The `oneplm_ingestion.dataframe` module provides reusable functions for loading data:

```python
from oneplm_ingestion.dataframe import load_objects, load_check_results, load_sync_log, load_pdfs

# Load all objects with attributes expanded into columns
df = load_objects("data/oneplm.db")

# Load a specific type
parts = load_objects("data/oneplm.db", type_name="Part PDP")

# Load without expanding the JSON attributes column
raw = load_objects("data/oneplm.db", expand_attributes=False)

# Load check results
results = load_check_results("data/oneplm.db")
failures = load_check_results("data/oneplm.db", failed_only=True)
```

### Typical Workflow

1. **Sync** data from Windchill: `oneplm sync`
2. **Explore** in the notebook -- find patterns, missing values, mismatches
3. **Write a rule** in `config/checks.json` based on what you found
4. **Run** the check: `oneplm check --check your_rule_name`
5. **Export** results: `oneplm export checks -o results.csv`
6. Repeat
