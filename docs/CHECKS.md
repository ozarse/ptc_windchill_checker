# Checks Catalog

Tracking document for every validation check in oneplm_ingestion. Checks are
defined in [`config/checks.json`](../config/checks.json) and run with
`oneplm check` (or `oneplm check --check <name>`). Results are written to the
`check_results` table and exported with `oneplm export checks`.

There are three kinds:

- **attribute** — validates one record's own fields (declared in JSON).
- **relationship** — compares a record against related records reached through a
  Windchill link, joined offline from the `relationships` table (declared in JSON).
- **python** — a registered function for logic the JSON can't express.

**Legend:** ✅ complete · 🟡 works but has open items to confirm · ⬜ planned

---

## Summary

| # | Check name | Kind | Applies to | Status |
|---|---|---|---|---|
| 1 | `config_pdp_attributes` | attribute | Config PDP | ✅ |
| 2 | `ifu_pdp_attributes` | attribute | IFU PDP | ✅ |
| 3 | `ifu_drawing_attributes` | attribute | IFU Drawing | ✅ |
| 4 | `ifu_drawing_matches_ifu_pdp` | relationship | IFU Drawing → IFU PDP | ✅ |
| 5 | `ifu_pdp_used_by_config_pdp` | relationship | IFU PDP → Config PDP | ✅ |
| 6 | `config_pdp_uses_match` | relationship | Config PDP → IFU PDP | ✅ |
| 7 | `ifu_drawing_pdf_filename` | python | IFU Drawing (PDF) | ✅ |
| 8 | `config_pdp_ifu_classification` | python | Config PDP + IFU PDPs | 🟡 |
| 9 | `ifu_drawing_pdf_language` | python | IFU Drawing (PDF) | 🟡 |

**Data prerequisites**

| To run… | You must first run… |
|---|---|
| Relationship checks (4, 5, 6) and classification (8) | `oneplm sync relationships` |
| PDF filename / language checks (7, 9) | `oneplm pdf download` (filename needs `--metadata-only` minimum) |
| Last-page portion of language check (9) | `oneplm pdf extract` |

Checks that read PDF data are marked `"requires_pdf": true` in `config/checks.json`
(currently 7 and 9). Run `oneplm check --skip-pdf` to exclude them when PDFs
haven't been downloaded. Add `"requires_pdf": true` to any check you author that
depends on the `pdfs` table so it participates in `--skip-pdf`.

---

## Attribute checks

### 1. `config_pdp_attributes` ✅
Config PDP records carry the core required attributes.
- `Number` not empty
- `Name` not empty
- `ApprovalDate` not empty **when** `State.Value == Released`

### 2. `ifu_pdp_attributes` ✅
IFU PDP records carry the core required attributes.
- `Number` not empty
- `Name` not empty
- `ConfigurableModule.Value == "No"`

### 3. `ifu_drawing_attributes` ✅
IFU Drawing documents are the correct type and identified.
- `DocTypeName == "IFU Drawing"`
- `Number` not empty

---

## Relationship checks

### 4. `ifu_drawing_matches_ifu_pdp` ✅
An IFU Drawing and the IFU PDP it **describes** must share identity.
- Traversal: IFU Drawing → `describes` → IFU PDP
- `Name` equal; `Number` equal
- `on_missing: fail` (a drawing with no linked IFU PDP fails)

### 5. `ifu_pdp_used_by_config_pdp` ✅
Every IFU PDP is **used by** a Config PDP.
- Traversal: IFU PDP → `used_by` → Config PDP
- Related Config PDP `Number` not empty
- `on_missing: fail`

### 6. `config_pdp_uses_match` ✅
Each IFU PDP a Config PDP **uses** shares its lifecycle state.
- Traversal: Config PDP → `uses` → IFU PDP
- `State.Value` equal
- `on_missing: skip`

---

## Python checks

### 7. `ifu_drawing_pdf_filename` ✅ · `requires_pdf`
Source: [`content_checks.py`](../src/oneplm_ingestion/content_checks.py) ·
function `ifu_drawing_pdf_filename`

Parses each IFU Drawing's primary-PDF filename
(`{number}_{revision}_{doc_type}_{language}.pdf`) and compares against metadata:
- Number match (drawing number, language suffix stripped, == filename number)
- Revision match (drawing `Revision` == filename revision)
- Language code is a valid ISO 639-1 code
- Language code == the `-XX` suffix on the drawing number

Skips drawings with no primary-PDF metadata.

### 8. `config_pdp_ifu_classification` 🟡
Source: [`ifu_classification.py`](../src/oneplm_ingestion/ifu_classification.py) ·
function `config_pdp_ifu_classification`

For each Config PDP, inspects the IFU PDPs it **uses** and classifies by whether
their part numbers end in a valid `-XX` language suffix:

| Related IFU PDP numbers | Class | Result |
|---|---|---|
| All language-suffixed | **eIFU** | pass (if attributes OK) |
| Exactly one not suffixed | **Hybrid (eIFU + print)** | pass (if attributes OK) |
| None suffixed | **Print** | fail — needs review |
| Mixed / none related | **Needs Review** | fail |

Then per-IFU-PDP attribute compliance:
- **eIFU** parts: `eIFU Only = Yes`, `Default Unit = As Needed`, `Quantity = 0`
- **Hybrid**, electronic (`-XX`) parts: `eIFU Flag = Yes`, `Default Unit = As Needed`, `Quantity = 0`
- **Hybrid**, print (non-`-XX`) part: `eIFU Flag = No`, `Default Unit = Piece`

Emits one classification row per Config PDP plus one row per IFU PDP per attribute.
The classification row passes only when the class is confident **and** all
attribute checks pass.

> 🟡 **Open — confirm attribute keys/values** (top of `ifu_classification.py`).
> These are placeholders and will fail every part until matched to real data:
> `ATTR_EIFU_ONLY = "eIFUOnly.Value"`, `ATTR_EIFU_FLAG = "eIFUFlag.Value"`,
> `ATTR_DEFAULT_UNIT = "DefaultUnit.Value"`, `ATTR_QUANTITY = "Quantity"`.
> Also confirm whether "eIFU Only" and "eIFU Flag" are one attribute or two.
> Easiest source of truth: `oneplm export objects --type "IFU PDP"` and read the
> column headers.

### 9. `ifu_drawing_pdf_language` 🟡 · `requires_pdf`
Source: [`content_checks.py`](../src/oneplm_ingestion/content_checks.py) ·
function `ifu_drawing_pdf_language`

Uses the language code parsed from the primary-PDF **filename** as the reference
and verifies it matches, for each IFU Drawing:
- **Title** — code appears as a standalone token in the drawing `Name`
  (handles space/underscore separation, e.g. `Widget IFU_EN`)
- **Number** — code == the `-XX` suffix on the drawing `Number`
- **Last page** — code appears as a standalone token in the tail
  (`TAIL_CHARS = 1200`) of the extracted PDF text

If a drawing has no extracted text, the last-page row is a SKIP (pass) prompting
`oneplm pdf extract`.

> 🟡 **Open — last-page reliability.** The stored text is one whole-document
> markdown blob with no page boundaries, so "last page" is the last 1200 chars.
> If IFU last pages list *all* languages, every code (incl. the reference) will
> appear and this passes regardless of the true language — revisit with a
> tighter footer pattern or accurate per-page extraction if so.

---

## Adding a check

- **attribute / relationship:** add an entry to `config/checks.json`. See the
  README "Attribute Validation Checks" section for the full field reference.
- **python:** write a function, decorate it with
  `@register_check("<name>")`, ensure its module is in `_BUILTIN_MODULES`
  ([`registry.py`](../src/oneplm_ingestion/registry.py)), and add a
  `{"kind": "python", "function": "<name>"}` entry to `config/checks.json`.

When you add or change a check, update the summary table and add/adjust its
section here.
