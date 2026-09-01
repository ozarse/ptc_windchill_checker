# Checks Catalog

Tracking document for every validation check in ptc_syncer_ingestion. Checks are
defined in [`config/checks.json`](../config/checks.json) and run with
`ptc_syncer check` (or `ptc_syncer check --check <name>`). Results are written to the
`check_results` table and exported with `ptc_syncer export checks`.

There are three kinds:

- **attribute** — validates one record's own fields (declared in JSON).
- **relationship** — compares a record against related records reached through a
  Windchill link, joined offline from the `relationships` table (declared in JSON).
  Supports `min_count`/`max_count` cardinality bounds and `target_value` literal
  assertions on the related record — see the README field reference.
- **python** — a registered function for logic the JSON can't express.
- **excel_compare** — compares Windchill "where used" products against an
  external Excel export (declared in JSON; reads the .xlsx at check time).

Every result row carries a `status` of `pass`, `fail`, or `skip`; skipped rows
(unmet `when` preconditions, missing prerequisite data) never count as failures
and are reported separately in summaries and exports.

**Legend:** ✅ complete · 🟡 works but has open items to confirm · ⬜ planned

---

## Summary

| # | Check name | Kind | Applies to | Status |
| --- | --- | --- | --- | --- |
| 1 | `config_pdp_attributes` | attribute | Config PDP | ✅ |
| 2 | `ifu_pdp_attributes` | attribute | IFU PDP | ✅ |
| 3 | `ifu_drawing_attributes` | attribute | IFU Drawing | ✅ |
| 4 | `ifu_drawing_matches_ifu_pdp` | relationship | IFU Drawing → IFU PDP | ✅ |
| 5 | `ifu_pdp_used_by_config_pdp` | relationship | IFU PDP → Config PDP | ✅ |
| 6 | `config_pdp_uses_match` | relationship | Config PDP → IFU PDP | 🟡 |
| 7 | `ifu_drawing_pdf_filename` | python | IFU Drawing (PDF) | ✅ |
| 8 | `config_pdp_ifu_classification` | python | Config PDP + IFU PDPs | 🟡 |
| 9 | `ifu_drawing_pdf_language` | python | IFU Drawing (PDF) | 🟡 |
| 10 | `previous_versions_not_in_concept` | python | All objects (version history) | 🟡 |
| 11 | `published_products_match` | excel_compare | Config PDP products ↔ published export | 🟡 |

**Data prerequisites**

| To run… | You must first run… |
| --- | --- |
| Relationship checks (4, 5, 6) and classification (8) | `ptc_syncer sync relationships` |
| PDF filename / language checks (7, 9) | `ptc_syncer pdf download` (filename needs `--metadata-only` minimum) |
| Last-page portion of language check (9) | `ptc_syncer pdf extract` |
| Version check (10) | `ptc_syncer sync versions` |
| Published-products check (11) | `ptc_syncer sync relationships` **and** the publishing website's bulk export saved to `data/published_products.xlsx` |

Checks that read PDF data are marked `"requires_pdf": true` in `config/checks.json`
(currently 7 and 9). Run `ptc_syncer check --skip-pdf` to exclude them when PDFs
haven't been downloaded. Add `"requires_pdf": true` to any check you author that
depends on the `pdfs` table so it participates in `--skip-pdf`.

---

## Attribute checks

### 1. `config_pdp_attributes` ✅

Config PDP records carry the core required attributes.

- `Number` not empty
- `Name` not empty

> The former `ConfigurableModule.Display == "Yes"` assertion was removed as a
> tautology: type classification during sync already guarantees it for every
> record stored as `Config PDP`. Likewise the once-documented `ApprovalDate`
> assertion was deliberately dropped in commit `b14717e`.

### 2. `ifu_pdp_attributes` ✅

IFU PDP records carry the core required attributes.

- `Number` not empty
- `Name` not empty

> The former `ConfigurableModule.Display == "No"` assertion was removed as a
> tautology (see check 1).

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

Every IFU PDP is **used by** at least one Config PDP.

- Traversal: IFU PDP → `used_by` → Config PDP
- `min_count: 1` — one pass/fail row per IFU PDP counting its related Config PDPs

### 6. `config_pdp_uses_match` 🟡

Each IFU PDP a Config PDP **uses** shares the Config PDP's lifecycle state.

- Traversal: Config PDP → `uses` → IFU PDP
- `State.Value` equal
- `on_missing: skip`

> 🟡 **Open — confirm the intended rule.** An earlier description said the IFU
> PDP "must be in the Released state", but the implementation has always
> compared source and target states for *equality* (two "In Work" records
> pass). If the absolute rule is intended, replace the comparison with
> `{ "target_attr": "State.Display", "operator": "equals", "target_value": "<released label>" }`
> once the released-state label is confirmed (sample payloads show
> `State.Value == "PRODUCTIONRELEASED"`).

**Note on `used_by` data:** `sync relationships` fetches `used_by` for **every**
part, Config PDPs included — the parents that use each Config PDP are stored in
the `relationships` table with their real ID/Number. No check consumes that
direction yet; relationship checks can only see related records whose type is
also synced into `objects`, so a check over Config PDP parents would first need
those parent types added to `config/types.json` and synced.

---

## Python checks

### 7. `ifu_drawing_pdf_filename` ✅ · `requires_pdf`

Source: [`content_checks.py`](../src/ptc_syncer_ingestion/content_checks.py) ·
function `ifu_drawing_pdf_filename`

Parses each IFU Drawing's primary-PDF filename
(`{number}_{revision}_{doc_type}_{language}.pdf`) and compares against metadata:

- Number match (drawing number, language suffix stripped, == filename number)
- Revision match (drawing `Revision` == filename revision)
- Language code is a valid ISO 639-1 code
- Language code == the `-XX` suffix on the drawing number

Skips drawings with no primary-PDF metadata.

### 8. `config_pdp_ifu_classification` 🟡

Source: [`ifu_classification.py`](../src/ptc_syncer_ingestion/ifu_classification.py) ·
function `config_pdp_ifu_classification`

For each Config PDP, inspects the IFU PDPs it **uses** and classifies by whether
their part numbers end in a valid `-XX` language suffix:

| Related IFU PDP numbers | Class | Result |
| --- | --- | --- |
| All language-suffixed | **eIFU** | pass (if attributes OK) |
| Exactly one not suffixed | **Hybrid (eIFU + print)** | pass (if attributes OK) |
| None suffixed | **Print** | fail — needs review |
| Mixed / none related | **Needs Review** | fail |

Then per-IFU-PDP attribute compliance (`StrykercorpeIFUFlag` is a boolean;
`DefaultUnit` is an enum compared on `.Display`):

- **eIFU** parts: `StrykercorpeIFUFlag == True`, `DefaultUnit.Display == "As Needed"`
- **Hybrid**, electronic (`-XX`) parts: `StrykercorpeIFUFlag == True`, `DefaultUnit.Display == "As Needed"`
- **Hybrid**, print (non-`-XX`) part: `StrykercorpeIFUFlag == False`, `DefaultUnit.Display == "Piece"`

Emits one classification row per Config PDP plus one row per IFU PDP per attribute.
The classification row passes only when the class is confident **and** all
attribute checks pass.

> 🟡 **Open — the "Quantity = 0" check is not implemented.** The eIFU flag and
> DefaultUnit are confirmed from the sample payloads and active. `Quantity` is
> **not a Part attribute** — it is the PartUse usage-link quantity
> (Config PDP `uses` IFU PDP). `sync relationships` now preserves the PartUse
> link on each stored `uses` row under `UsesLink` in `attributes_json`
> (re-run `ptc_syncer sync relationships` to populate it), so the data is
> available at `UsesLink.Quantity`; what remains is confirming the expected
> quantity rule per classification and wiring it into this check.

### 9. `ifu_drawing_pdf_language` 🟡 · `requires_pdf`

Source: [`content_checks.py`](../src/ptc_syncer_ingestion/content_checks.py) ·
function `ifu_drawing_pdf_language`

Uses the language code parsed from the primary-PDF **filename** as the reference
and verifies it matches, for each IFU Drawing:

- **Title** — code appears as a standalone token in the drawing `Name`
  (handles space/underscore separation, e.g. `Widget IFU_EN`)
- **Number** — code == the `-XX` suffix on the drawing `Number`
- **Last page** — code appears as a standalone token in the tail
  (`TAIL_CHARS = 1200`) of the extracted PDF text

If a drawing has no extracted text, the last-page row is a SKIP (pass) prompting
`ptc_syncer pdf extract`.

> 🟡 **Open — last-page reliability.** The stored text is one whole-document
> markdown blob with no page boundaries, so "last page" is the last 1200 chars.
> If IFU last pages list *all* languages, every code (incl. the reference) will
> appear and this passes regardless of the true language — revisit with a
> tighter footer pattern or accurate per-page extraction if so.

### 10. `previous_versions_not_in_concept` 🟡

Source: [`versions.py`](../src/ptc_syncer_ingestion/versions.py) ·
function `previous_versions_not_in_concept`

For each object with stored version history, the **previous** (non-latest)
versions must not be in a concept-phase lifecycle state. Emits one row per object
that has previous versions: PASS if none are in concept, FAIL listing the
offending version(s) otherwise. Objects with no previous versions are skipped.

Version history is fetched by `ptc_syncer sync versions` (calls the Windchill
`.../Versions` API for each object) and stored in the `versions` table, so the
check runs offline. A version is judged "concept" if its `State.Value` **or**
`State.Display` is in `CONCEPT_STATES` (matched case-insensitively).

> 🟡 **Open — confirm the concept-phase state string.** `CONCEPT_STATES` at the
> top of `versions.py` currently holds `{"concept"}` as a placeholder. Set it to
> the real lifecycle state value(s)/label(s) for the concept phase in your
> "Stryker Three Phase Development" lifecycle (e.g. the `State.Value` seen on a
> concept-phase version).

---

## Excel-compare checks

### 11. `published_products_match` 🟡

Source: [`excel_compare.py`](../src/ptc_syncer_ingestion/excel_compare.py) ·
declared in `config/checks.json` (`kind: excel_compare`)

Compares the products that use Config PDPs against the bulk export from the
IFU publishing website. The Windchill side is built entirely from data already
synced: products are the `used_by` targets of each Config PDP (identified by
`target_number` — they do **not** need to be synced as objects), and each
product's expected IFU set is the union of the full, language-suffixed IFU PDP
numbers its Config PDPs `use`.

The export is read at check time from `data/published_products.xlsx`
(configurable via `file`): one row per product, product number in
`Reforcatalognumber`, IFU PDP numbers in `Product groups` separated by `|`.
A product spread over several rows gets its IFU sets merged.

Three validations, all in one result set:

| Case | Result |
| --- | --- |
| Windchill product absent from the export | FAIL, names the Config PDP(s) it came from |
| Product in both, IFU sets differ | FAIL, lists "in Windchill but not published" and "published but not in Windchill" numbers |
| Export product no Config PDP is used by | FAIL (orphan in the published list) |

If the export file is missing, or no `used_by` relationships are stored, the
check emits a single `skip` row saying what to do — it never fails for absent
prerequisites.

> 🟡 **Open — verify against the first real export.** Column headers
> (`Reforcatalognumber`, `Product groups`), the `|` separator, and the
> assumption that `Product groups` holds full language-suffixed IFU PDP numbers
> (e.g. `50-0060-EN`) all come from the export's described layout; confirm and
> adjust `config/checks.json` once the first real file is in hand. Also note
> the IFU set only counts `uses` children that are synced as IFU PDP objects.

---

## Adding a check

- **attribute / relationship:** add an entry to `config/checks.json`. See the
  README "Attribute Validation Checks" section for the full field reference.
- **python:** write a function, decorate it with
  `@register_check("<name>")`, ensure its module is in `_BUILTIN_MODULES`
  ([`registry.py`](../src/ptc_syncer_ingestion/registry.py)), and add a
  `{"kind": "python", "function": "<name>"}` entry to `config/checks.json`.

When you add or change a check, update the summary table and add/adjust its
section here.
