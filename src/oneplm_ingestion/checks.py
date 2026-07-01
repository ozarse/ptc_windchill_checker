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

from oneplm_ingestion.db import (
    get_object_by_id,
    get_objects_by_type,
    get_relationship_source_ids,
    get_relationship_target_ids,
    save_check_results,
)
from oneplm_ingestion.models import (
    Assertion,
    AttributeCheck,
    CheckResult,
    PythonCheck,
    RelationshipCheck,
    RelationshipComparison,
    WhenCondition,
)
from oneplm_ingestion.operators import compare, evaluate_when, get_attr_value
from oneplm_ingestion.registry import get_check_function

log = logging.getLogger(__name__)

Check = AttributeCheck | RelationshipCheck | PythonCheck

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


def load_check_configs(config_path: Path) -> list[Check]:
    """Load checks.json and parse each entry into its kind-specific dataclass."""
    with open(config_path) as f:
        raw = json.load(f)

    checks: list[Check] = []
    for entry in raw:
        kind = entry.get("kind", "attribute")
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
                    source_attr=c["source_attr"],
                    target_attr=c.get("target_attr"),
                    operator=c.get("operator", "equals"),
                    value=c.get("value"),
                    when=_parse_when(c.get("when")),
                )
                for c in entry.get("comparisons", [])
            ]
            checks.append(RelationshipCheck(
                name=entry["name"],
                type=entry["type"],
                related_type=entry["related_type"],
                via=entry["via"],
                description=entry.get("description", ""),
                comparisons=comparisons,
                on_missing=entry.get("on_missing", "fail"),
                requires_pdf=requires_pdf,
            ))
        elif kind == "python":
            checks.append(PythonCheck(
                name=entry["name"],
                function=entry["function"],
                description=entry.get("description", ""),
                requires_pdf=requires_pdf,
            ))
        else:
            raise ValueError(f"Unknown check kind '{kind}' in check '{entry.get('name')}'")
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


def _related_objects(conn, source_id: str, via: str, related_type: str) -> list:
    """Resolve related records of ``related_type`` reachable from ``source_id``."""
    if via not in _VIA:
        raise ValueError(
            f"Unknown relationship 'via': '{via}'. Expected one of {sorted(_VIA)}"
        )
    rel_type, direction = _VIA[via]
    if direction == "forward":
        ids = get_relationship_target_ids(conn, source_id, rel_type)
    else:
        ids = get_relationship_source_ids(conn, source_id, rel_type)

    related = []
    for rid in ids:
        obj = get_object_by_id(conn, rid)
        if obj is not None and obj.type_name == related_type:
            related.append(obj)
    return related


def run_relationship_check(conn, check: RelationshipCheck) -> list[CheckResult]:
    """Validate each ``check.type`` record against its related ``related_type`` records."""
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    for source in get_objects_by_type(conn, check.type):
        related = _related_objects(conn, source.id, check.via, check.related_type)

        if not related:
            if check.on_missing == "skip":
                continue
            for comp in check.comparisons:
                results.append(CheckResult(
                    check_name=check.name,
                    source_object_id=source.id,
                    target_object_id="MISSING",
                    source_attr=comp.source_attr,
                    target_attr=comp.target_attr or "",
                    source_value=get_attr_value(source.attributes, comp.source_attr),
                    target_value=None,
                    passed=False,
                    message=(
                        f"No related {check.related_type} found via '{check.via}' "
                        f"for {source.number or source.id}"
                    ),
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
                        source_attr=comp.source_attr,
                        target_attr=comp.target_attr or "",
                        source_value=get_attr_value(source.attributes, comp.source_attr),
                        target_value=None,
                        passed=True,
                        message=(
                            f"SKIP: precondition not met "
                            f"({comp.when.attr} {comp.when.operator} {comp.when.value})"
                        ),
                        checked_at=now,
                    ))
                    continue

                src_val = get_attr_value(source.attributes, comp.source_attr)
                tgt_val = (
                    get_attr_value(target.attributes, comp.target_attr)
                    if comp.target_attr else None
                )
                passed, msg = compare(src_val, tgt_val, comp.operator, literal_value=comp.value)
                results.append(CheckResult(
                    check_name=check.name,
                    source_object_id=source.id,
                    target_object_id=target.id,
                    source_attr=comp.source_attr,
                    target_attr=comp.target_attr or "",
                    source_value=src_val,
                    target_value=tgt_val if comp.target_attr else comp.value,
                    passed=passed,
                    message=msg,
                    checked_at=now,
                ))
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

        passed = sum(1 for r in results if r.passed)
        failed = sum(1 for r in results if not r.passed)
        log.info("  %s: %d passed, %d failed", chk.name, passed, failed)

    conn.commit()
    return all_results
