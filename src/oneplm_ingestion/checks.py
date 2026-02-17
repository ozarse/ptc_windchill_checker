"""Configurable attribute comparison engine."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from oneplm_ingestion.db import get_objects_by_type, save_check_results
from oneplm_ingestion.models import CheckConfig, CheckResult, Comparison, WhenCondition

log = logging.getLogger(__name__)

VALID_OPERATORS = frozenset({
    "equals", "not_equals",
    "contains", "not_contains",
    "not_empty", "is_empty",
    "matches",
    "greater_than", "less_than", "greater_equal", "less_equal",
    "before", "after",
})


def load_check_configs(config_path: Path) -> list[CheckConfig]:
    """Load checks.json and parse into CheckConfig objects."""
    with open(config_path) as f:
        raw = json.load(f)

    configs = []
    for entry in raw:
        comparisons = []
        for comp in entry.pop("comparisons", []):
            when_raw = comp.pop("when", None)
            when = WhenCondition(**when_raw) if when_raw else None
            comparisons.append(Comparison(**comp, when=when))
        configs.append(CheckConfig(**entry, comparisons=comparisons))
    return configs


def _get_attr_value(attributes: dict, attr_name: str) -> str | None:
    """Extract an attribute value from the attributes dict.

    Supports dot notation for nested keys (e.g., "State.Value").
    """
    keys = attr_name.split(".")
    current = attributes
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return str(current) if current is not None else None


def _parse_date(value: str) -> datetime | None:
    """Try to parse a date/datetime string. Returns None on failure."""
    try:
        cleaned = value.replace("Z", "+00:00") if value.endswith("Z") else value
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


def _compare(
    source_val: str | None,
    target_val: str | None,
    operator: str,
    literal_value: str | None = None,
) -> tuple[bool, str]:
    """Run a single comparison. Returns (passed, message).

    When literal_value is provided, it is used as the comparison target
    instead of target_val.
    """
    compare_to = literal_value if literal_value is not None else target_val

    # --- Unary operators (only inspect source_val) ---

    if operator == "not_empty":
        passed = source_val is not None and source_val.strip() != ""
        msg = f"{'PASS' if passed else 'FAIL'}: value is {'not ' if passed else ''}empty"
        return passed, msg

    if operator == "is_empty":
        passed = source_val is None or source_val.strip() == ""
        msg = f"{'PASS' if passed else 'FAIL'}: value is {'empty' if passed else 'not empty'}"
        return passed, msg

    # --- Regex ---

    if operator == "matches":
        if literal_value is None:
            return False, "FAIL: 'matches' operator requires a 'value' (regex pattern)"
        if source_val is None:
            return False, "FAIL: source value is null, cannot match pattern"
        try:
            passed = re.fullmatch(literal_value, source_val) is not None
        except re.error as e:
            return False, f"FAIL: invalid regex pattern: {e}"
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' {'matches' if passed else 'does not match'} /{literal_value}/"
        return passed, msg

    # --- String comparison operators ---

    if operator == "equals":
        passed = source_val == compare_to
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' == '{compare_to}'"
        return passed, msg

    if operator == "not_equals":
        passed = source_val != compare_to
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' != '{compare_to}'"
        return passed, msg

    if operator == "contains":
        if source_val is None or compare_to is None:
            return False, f"FAIL: cannot check contains with null values"
        passed = compare_to in source_val
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' {'contains' if passed else 'does not contain'} '{compare_to}'"
        return passed, msg

    if operator == "not_contains":
        if source_val is None:
            passed = True
            msg = "PASS: source is null, trivially does not contain value"
            return passed, msg
        if compare_to is None:
            return False, "FAIL: cannot check not_contains with null comparison value"
        passed = compare_to not in source_val
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' {'does not contain' if passed else 'contains'} '{compare_to}'"
        return passed, msg

    # --- Numeric operators ---

    if operator in ("greater_than", "less_than", "greater_equal", "less_equal"):
        if source_val is None or compare_to is None:
            return False, f"FAIL: cannot compare null values numerically"
        try:
            src_num = float(source_val)
            cmp_num = float(compare_to)
        except (ValueError, TypeError):
            return False, f"FAIL: cannot parse as numbers: '{source_val}', '{compare_to}'"

        if operator == "greater_than":
            passed = src_num > cmp_num
        elif operator == "less_than":
            passed = src_num < cmp_num
        elif operator == "greater_equal":
            passed = src_num >= cmp_num
        else:
            passed = src_num <= cmp_num
        msg = f"{'PASS' if passed else 'FAIL'}: {src_num} {operator} {cmp_num}"
        return passed, msg

    # --- Date operators ---

    if operator in ("before", "after"):
        if source_val is None or compare_to is None:
            return False, f"FAIL: cannot compare null values as dates"
        src_date = _parse_date(source_val)
        cmp_date = _parse_date(compare_to)
        if src_date is None or cmp_date is None:
            return False, f"FAIL: cannot parse as dates: '{source_val}', '{compare_to}'"

        if operator == "before":
            passed = src_date < cmp_date
        else:
            passed = src_date > cmp_date
        msg = f"{'PASS' if passed else 'FAIL'}: '{source_val}' {operator} '{compare_to}'"
        return passed, msg

    return False, f"Unknown operator: {operator}"


def _evaluate_when(attributes: dict, when: WhenCondition) -> bool:
    """Evaluate a 'when' precondition against the source object's attributes.

    Returns True if the condition is met (the check should run),
    False if the condition is not met (the check should be skipped).
    """
    attr_val = _get_attr_value(attributes, when.attr)
    passed, _ = _compare(attr_val, None, when.operator, literal_value=when.value)
    return passed


def run_check(conn, check: CheckConfig) -> list[CheckResult]:
    """Run a single check configuration against the local database."""
    source_objects = get_objects_by_type(conn, check.source_type)
    target_objects = get_objects_by_type(conn, check.target_type)

    # Build lookup: match_on attribute value -> list of target objects
    target_lookup: dict[str, list] = {}
    for t in target_objects:
        key = _get_attr_value(t.attributes, check.match_on)
        if key:
            target_lookup.setdefault(key, []).append(t)

    results = []
    now = datetime.now(timezone.utc).isoformat()

    for source in source_objects:
        source_key = _get_attr_value(source.attributes, check.match_on)
        if not source_key:
            continue

        targets = target_lookup.get(source_key, [])
        if not targets:
            for comp in check.comparisons:
                results.append(CheckResult(
                    check_name=check.name,
                    source_object_id=source.id,
                    target_object_id="MISSING",
                    source_attr=comp.source_attr,
                    target_attr=comp.target_attr or "",
                    source_value=_get_attr_value(source.attributes, comp.source_attr),
                    target_value=None,
                    passed=False,
                    message=f"No matching {check.target_type} found for {check.match_on}={source_key}",
                    checked_at=now,
                ))
            continue

        for target in targets:
            for comp in check.comparisons:
                # Evaluate precondition
                if comp.when and not _evaluate_when(source.attributes, comp.when):
                    results.append(CheckResult(
                        check_name=check.name,
                        source_object_id=source.id,
                        target_object_id=target.id,
                        source_attr=comp.source_attr,
                        target_attr=comp.target_attr or "",
                        source_value=_get_attr_value(source.attributes, comp.source_attr),
                        target_value=None,
                        passed=True,
                        message=f"SKIP: precondition not met ({comp.when.attr} {comp.when.operator} {comp.when.value})",
                        checked_at=now,
                    ))
                    continue

                src_val = _get_attr_value(source.attributes, comp.source_attr)
                tgt_val = _get_attr_value(target.attributes, comp.target_attr) if comp.target_attr else None
                passed, msg = _compare(src_val, tgt_val, comp.operator, literal_value=comp.value)
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


def run_all_checks(
    conn,
    config_path: Path,
    check_names: list[str] | None = None,
) -> dict[str, list[CheckResult]]:
    """Run all (or specified) checks, save results to DB, return them."""
    checks = load_check_configs(config_path)
    if check_names:
        checks = [c for c in checks if c.name in check_names]

    all_results = {}
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
