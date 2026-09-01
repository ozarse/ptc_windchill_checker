"""Record validation engine.

Checks are defined declaratively in checks.json and come in three kinds:

  - ``attribute``     — validate attributes of individual records of one type.
  - ``relationship``  — validate a record against records reached through a
                        Windchill relationship (describes / described_by / uses /
                        used_by), resolved offline from the relationships table.
  - ``python``        — delegate to a function registered in registry.py for
                        logic that does not fit the declarative forms.

Each check produces CheckResult rows that are saved to the database.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from ptc_syncer_ingestion.db import (
    get_object_by_id,
    get_objects_by_type,
    get_relationship_source_ids,
    get_relationship_targets,
    save_check_results,
)
from ptc_syncer_ingestion.models import (
    Assertion,
    AttributeCheck,
    CheckResult,
    ExcelCompareCheck,
    PythonCheck,
    RelationshipCheck,
    RelationshipComparison,
    WhenCondition,
)
from ptc_syncer_ingestion.operators import (
    UNARY_OPERATORS,
    VALID_OPERATORS,
    compare,
    evaluate_when,
    get_attr_value,
)
from ptc_syncer_ingestion.registry import get_check_function

log = logging.getLogger(__name__)

Check = AttributeCheck | RelationshipCheck | PythonCheck | ExcelCompareCheck

# Logical relationship name (from the source record's perspective) -> the stored
# rel_type and the direction to traverse it. "forward" looks up rows where the
# source record is the relationship source; "inverse" looks up rows where the
# source record is the relationship target.
_VIA: dict[str, tuple[str, str]] = {
    "described_by": ("described_by", "forward"),
    "used_by": ("used_by", "forward"),
    "uses": ("uses", "forward"),
    "describes": ("described_by", "inverse"),
}


def _parse_when(raw: dict | None) -> WhenCondition | None:
    return WhenCondition(**raw) if raw else None


def _validate_operator(operator: str, where: str, errors: list[str]) -> None:
    if operator not in VALID_OPERATORS:
        errors.append(f"{where}: unknown operator '{operator}' (expected one of {sorted(VALID_OPERATORS)})")


def _validate_when(when: WhenCondition | None, where: str, errors: list[str]) -> None:
    if when is not None:
        _validate_operator(when.operator, f"{where} 'when'", errors)


def _validate_assertion(a: Assertion, where: str, errors: list[str]) -> None:
    _validate_operator(a.operator, where, errors)
    if a.operator in VALID_OPERATORS and a.operator not in UNARY_OPERATORS and a.value is None:
        errors.append(f"{where}: operator '{a.operator}' requires a 'value'")
    _validate_when(a.when, where, errors)


def _validate_comparison(c: RelationshipComparison, where: str, errors: list[str]) -> None:
    _validate_operator(c.operator, where, errors)
    if c.source_attr is None and c.target_attr is None:
        errors.append(f"{where}: needs 'source_attr' and/or 'target_attr'")
    if c.target_value is not None and c.target_attr is None:
        errors.append(f"{where}: 'target_value' requires a 'target_attr'")
    if (
        c.operator in VALID_OPERATORS
        and c.operator not in UNARY_OPERATORS
        and c.target_attr is None
        and c.value is None
    ):
        errors.append(f"{where}: operator '{c.operator}' needs a 'target_attr' or a literal 'value'")
    _validate_when(c.when, where, errors)


def _validate_relationship_check(check: RelationshipCheck, where: str, errors: list[str]) -> None:
    if check.via not in _VIA:
        errors.append(f"{where}: unknown 'via' '{check.via}' (expected one of {sorted(_VIA)})")
    if check.on_missing not in ("fail", "skip"):
        errors.append(f"{where}: 'on_missing' must be 'fail' or 'skip', got '{check.on_missing}'")
    for bound_name in ("min_count", "max_count"):
        bound = getattr(check, bound_name)
        if bound is not None and (not isinstance(bound, int) or bound < 0):
            errors.append(f"{where}: '{bound_name}' must be a non-negative integer")
    if (
        isinstance(check.min_count, int)
        and isinstance(check.max_count, int)
        and check.min_count > check.max_count
    ):
        errors.append(f"{where}: 'min_count' ({check.min_count}) exceeds 'max_count' ({check.max_count})")
    if not check.comparisons and check.min_count is None and check.max_count is None:
        errors.append(f"{where}: needs 'comparisons' and/or 'min_count'/'max_count'")
    for i, comp in enumerate(check.comparisons):
        _validate_comparison(comp, f"{where} comparison {i + 1}", errors)


def load_check_configs(config_path: Path) -> list[Check]:
    """Load checks.json and parse each entry into its kind-specific dataclass.

    Raises ValueError listing every problem found (unknown operators, bad 'via',
    malformed comparisons, duplicate names) so config typos surface immediately
    instead of as per-record failures at run time.
    """
    with open(config_path) as f:
        raw = json.load(f)

    checks: list[Check] = []
    errors: list[str] = []
    for entry in raw:
        kind = entry.get("kind", "attribute")
        name = entry.get("name", "<unnamed>")
        where = f"check '{name}'"
        requires_pdf = entry.get("requires_pdf", False)
        if kind == "attribute":
            assertions = [
                Assertion(
                    attr=a["attr"],
                    operator=a.get("operator", "not_empty"),
                    value=a.get("value"),
                    when=_parse_when(a.get("when")),
                )
                for a in entry.get("assertions", [])
            ]
            for i, assertion in enumerate(assertions):
                _validate_assertion(assertion, f"{where} assertion {i + 1}", errors)
            checks.append(AttributeCheck(
                name=entry["name"],
                type=entry["type"],
                description=entry.get("description", ""),
                assertions=assertions,
                requires_pdf=requires_pdf,
            ))
        elif kind == "relationship":
            comparisons = [
                RelationshipComparison(
                    source_attr=c.get("source_attr"),
                    target_attr=c.get("target_attr"),
                    operator=c.get("operator", "equals"),
                    value=c.get("value"),
                    target_value=c.get("target_value"),
                    when=_parse_when(c.get("when")),
                )
                for c in entry.get("comparisons", [])
            ]
            check = RelationshipCheck(
                name=entry["name"],
                type=entry["type"],
                related_type=entry["related_type"],
                via=entry["via"],
                description=entry.get("description", ""),
                comparisons=comparisons,
                on_missing=entry.get("on_missing", "fail"),
                min_count=entry.get("min_count"),
                max_count=entry.get("max_count"),
                requires_pdf=requires_pdf,
            )
            _validate_relationship_check(check, where, errors)
            checks.append(check)
        elif kind == "python":
            checks.append(PythonCheck(
                name=entry["name"],
                function=entry["function"],
                description=entry.get("description", ""),
                requires_pdf=requires_pdf,
            ))
        elif kind == "excel_compare":
            check = ExcelCompareCheck(
                name=entry["name"],
                file=entry.get("file", ""),
                product_column=entry.get("product_column", ""),
                ifu_column=entry.get("ifu_column", ""),
                description=entry.get("description", ""),
                sheet=entry.get("sheet"),
                ifu_separator=entry.get("ifu_separator", "|"),
                type=entry.get("type", "Config PDP"),
                ifu_type=entry.get("ifu_type", "IFU PDP"),
                requires_pdf=requires_pdf,
            )
            for field in ("file", "product_column", "ifu_column", "ifu_separator"):
                if not getattr(check, field):
                    errors.append(f"{where}: '{field}' is required and must be non-empty")
            checks.append(check)
        else:
            errors.append(f"{where}: unknown kind '{kind}'")

    seen: set[str] = set()
    for chk in checks:
        if chk.name in seen:
            errors.append(f"duplicate check name '{chk.name}'")
        seen.add(chk.name)

    if errors:
        raise ValueError(
            f"Invalid checks config {config_path}:\n  " + "\n  ".join(errors)
        )
    return checks


def run_attribute_check(conn, check: AttributeCheck) -> list[CheckResult]:
    """Validate each record of ``check.type`` against its assertions."""
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    for obj in get_objects_by_type(conn, check.type):
        for assertion in check.assertions:
            if assertion.when and not evaluate_when(obj.attributes, assertion.when):
                results.append(CheckResult(
                    check_name=check.name,
                    source_object_id=obj.id,
                    target_object_id=obj.id,
                    source_attr=assertion.attr,
                    target_attr="",
                    source_value=get_attr_value(obj.attributes, assertion.attr),
                    target_value=assertion.value,
                    passed=True,
                    status="skip",
                    message=(
                        f"SKIP: precondition not met "
                        f"({assertion.when.attr} {assertion.when.operator} {assertion.when.value})"
                    ),
                    checked_at=now,
                ))
                continue

            src_val = get_attr_value(obj.attributes, assertion.attr)
            passed, msg = compare(src_val, None, assertion.operator, literal_value=assertion.value)
            results.append(CheckResult(
                check_name=check.name,
                source_object_id=obj.id,
                target_object_id=obj.id,
                source_attr=assertion.attr,
                target_attr="",
                source_value=src_val,
                target_value=assertion.value,
                passed=passed,
                message=msg,
                checked_at=now,
            ))
    return results


def _related_objects(conn, source_id: str, via: str, related_type: str) -> tuple[list, list[str], int]:
    """Resolve related records of ``related_type`` reachable from ``source_id``.

    Returns ``(related, unsynced, other_type)``:
      - ``related`` — related objects found in the DB with the expected type.
      - ``unsynced`` — labels (number or ID) of linked targets NOT in the local
        DB, so "no link in Windchill" can be told apart from "link exists but
        the target was never synced".
      - ``other_type`` — count of linked objects in the DB of a different type
        (legitimately excluded, e.g. non-IFU children under a Config PDP).
    """
    if via not in _VIA:
        raise ValueError(
            f"Unknown relationship 'via': '{via}'. Expected one of {sorted(_VIA)}"
        )
    rel_type, direction = _VIA[via]
    if direction == "forward":
        pairs = get_relationship_targets(conn, source_id, rel_type)
    else:
        pairs = [(sid, None) for sid in get_relationship_source_ids(conn, source_id, rel_type)]

    related, unsynced, other_type = [], [], 0
    for rid, number in pairs:
        obj = get_object_by_id(conn, rid)
        if obj is None:
            unsynced.append(number or rid)
        elif obj.type_name == related_type:
            related.append(obj)
        else:
            other_type += 1
    return related, unsynced, other_type


def _unsynced_note(unsynced: list[str], max_listed: int = 5) -> str:
    """Human-readable note about linked targets missing from the local DB."""
    if not unsynced:
        return ""
    listed = ", ".join(unsynced[:max_listed])
    more = f" (+{len(unsynced) - max_listed} more)" if len(unsynced) > max_listed else ""
    return f"; {len(unsynced)} linked target(s) not in local DB: {listed}{more}"


def _cardinality_result(check: RelationshipCheck, source, related: list, unsynced: list[str], now: str) -> CheckResult:
    """Build the one-per-source result for a min_count/max_count constraint."""
    count = len(related)
    passed = (check.min_count is None or count >= check.min_count) and (
        check.max_count is None or count <= check.max_count
    )
    if check.min_count is not None and check.max_count is not None:
        expected = f"between {check.min_count} and {check.max_count}"
    elif check.min_count is not None:
        expected = f"at least {check.min_count}"
    else:
        expected = f"at most {check.max_count}"
    return CheckResult(
        check_name=check.name,
        source_object_id=source.id,
        target_object_id=related[0].id if count == 1 else ("MISSING" if count == 0 else "MULTIPLE"),
        source_attr="count",
        target_attr="",
        source_value=str(count),
        target_value=expected,
        passed=passed,
        message=(
            f"{'PASS' if passed else 'FAIL'}: {count} related {check.related_type} "
            f"via '{check.via}' for {source.number or source.id} (expected {expected})"
            + _unsynced_note(unsynced)
        ),
        checked_at=now,
    )


def _comparison_result(check: RelationshipCheck, source, target, comp, now: str) -> CheckResult:
    """Evaluate one comparison between a source record and one related record."""
    src_val = get_attr_value(source.attributes, comp.source_attr) if comp.source_attr else None
    tgt_val = get_attr_value(target.attributes, comp.target_attr) if comp.target_attr else None

    if comp.target_value is not None:
        # Assert directly on the related record's attribute against a literal.
        passed, msg = compare(tgt_val, None, comp.operator, literal_value=comp.target_value)
    elif comp.source_attr and comp.target_attr and comp.value is None:
        passed, msg = compare(src_val, tgt_val, comp.operator)
    else:
        passed, msg = compare(src_val if comp.source_attr else tgt_val, None,
                              comp.operator, literal_value=comp.value)
    return CheckResult(
        check_name=check.name,
        source_object_id=source.id,
        target_object_id=target.id,
        source_attr=comp.source_attr or "",
        target_attr=comp.target_attr or "",
        source_value=src_val,
        target_value=tgt_val if comp.target_attr else comp.value,
        passed=passed,
        message=msg,
        checked_at=now,
    )


def run_relationship_check(conn, check: RelationshipCheck) -> list[CheckResult]:
    """Validate each ``check.type`` record against its related ``related_type`` records."""
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    for source in get_objects_by_type(conn, check.type):
        related, unsynced, _ = _related_objects(conn, source.id, check.via, check.related_type)

        if check.min_count is not None or check.max_count is not None:
            results.append(_cardinality_result(check, source, related, unsynced, now))

        if not related:
            if check.on_missing == "skip":
                continue
            if unsynced:
                # The link exists in Windchill; our local DB just lacks the target.
                # Report a data gap rather than a misleading "no related record".
                missing_msg = (
                    f"FAIL: related {check.related_type} via '{check.via}' for "
                    f"{source.number or source.id} not synced locally"
                    + _unsynced_note(unsynced)
                )
            else:
                missing_msg = (
                    f"No related {check.related_type} found via '{check.via}' "
                    f"for {source.number or source.id}"
                )
            for comp in check.comparisons:
                results.append(CheckResult(
                    check_name=check.name,
                    source_object_id=source.id,
                    target_object_id="MISSING",
                    source_attr=comp.source_attr or "",
                    target_attr=comp.target_attr or "",
                    source_value=(
                        get_attr_value(source.attributes, comp.source_attr)
                        if comp.source_attr else None
                    ),
                    target_value=None,
                    passed=False,
                    message=missing_msg,
                    checked_at=now,
                ))
            continue

        for target in related:
            for comp in check.comparisons:
                if comp.when and not evaluate_when(source.attributes, comp.when):
                    results.append(CheckResult(
                        check_name=check.name,
                        source_object_id=source.id,
                        target_object_id=target.id,
                        source_attr=comp.source_attr or "",
                        target_attr=comp.target_attr or "",
                        source_value=(
                            get_attr_value(source.attributes, comp.source_attr)
                            if comp.source_attr else None
                        ),
                        target_value=None,
                        passed=True,
                        status="skip",
                        message=(
                            f"SKIP: precondition not met "
                            f"({comp.when.attr} {comp.when.operator} {comp.when.value})"
                        ),
                        checked_at=now,
                    ))
                    continue
                results.append(_comparison_result(check, source, target, comp, now))
    return results


def run_python_check(conn, check: PythonCheck) -> list[CheckResult]:
    """Dispatch to a registered Python check function."""
    fn = get_check_function(check.function)
    if fn is None:
        raise ValueError(
            f"Python check '{check.name}' references unknown function '{check.function}'"
        )
    results = fn(conn)
    # Stamp the configured check name so results group under it.
    for r in results:
        r.check_name = check.name
    return results


def run_check(conn, check: Check) -> list[CheckResult]:
    """Run a single check, dispatching by kind."""
    if isinstance(check, AttributeCheck):
        return run_attribute_check(conn, check)
    if isinstance(check, RelationshipCheck):
        return run_relationship_check(conn, check)
    if isinstance(check, PythonCheck):
        return run_python_check(conn, check)
    if isinstance(check, ExcelCompareCheck):
        from ptc_syncer_ingestion.excel_compare import run_excel_compare_check

        return run_excel_compare_check(conn, check)
    raise TypeError(f"Unsupported check type: {type(check).__name__}")


def run_all_checks(
    conn,
    config_path: Path,
    check_names: list[str] | None = None,
    skip_pdf: bool = False,
) -> dict[str, list[CheckResult]]:
    """Run all (or named) checks, save results to the DB, and return them.

    When ``skip_pdf`` is True, checks marked ``requires_pdf`` are excluded (useful
    when PDFs have not been downloaded/extracted).
    """
    checks = load_check_configs(config_path)
    if check_names:
        checks = [c for c in checks if c.name in check_names]
    if skip_pdf:
        skipped = [c.name for c in checks if c.requires_pdf]
        if skipped:
            log.info("Skipping %d PDF check(s): %s", len(skipped), ", ".join(skipped))
        checks = [c for c in checks if not c.requires_pdf]

    all_results: dict[str, list[CheckResult]] = {}
    for chk in checks:
        log.info("Running check: %s", chk.name)
        results = run_check(conn, chk)
        save_check_results(conn, results)
        all_results[chk.name] = results

        passed = sum(1 for r in results if r.status == "pass")
        failed = sum(1 for r in results if r.status == "fail")
        skipped = sum(1 for r in results if r.status == "skip")
        log.info("  %s: %d passed, %d failed, %d skipped", chk.name, passed, failed, skipped)

    conn.commit()
    return all_results
