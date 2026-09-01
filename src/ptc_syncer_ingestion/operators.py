"""Shared comparison primitives used by the checks engine.

A single ``compare`` function evaluates one operator against a source value and
either a target value (another object's attribute) or a literal value from the
check definition. ``get_attr_value`` and ``evaluate_when`` support dot-notation
attribute access and optional preconditions.
"""

from __future__ import annotations

import re
from datetime import datetime

from ptc_syncer_ingestion.models import WhenCondition

VALID_OPERATORS = frozenset({
    "equals", "not_equals",
    "contains", "not_contains",
    "not_empty", "is_empty",
    "matches",
    "greater_than", "less_than", "greater_equal", "less_equal",
    "before", "after",
})

# Operators that only inspect the source value (no target/literal required).
UNARY_OPERATORS = frozenset({"not_empty", "is_empty"})


def get_attr_value(attributes: dict, attr_name: str) -> str | None:
    """Extract an attribute value, supporting dot notation (e.g. "State.Value")."""
    current = attributes
    for key in attr_name.split("."):
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return str(current) if current is not None else None


def _parse_date(value: str) -> datetime | None:
    """Parse a date/datetime string. Returns None on failure."""
    try:
        cleaned = value.replace("Z", "+00:00") if value.endswith("Z") else value
        return datetime.fromisoformat(cleaned)
    except (ValueError, TypeError):
        return None


def compare(
    source_val: str | None,
    target_val: str | None,
    operator: str,
    literal_value: str | None = None,
) -> tuple[bool, str]:
    """Run a single comparison. Returns (passed, message).

    When ``literal_value`` is provided it is the comparison target instead of
    ``target_val``.
    """
    compare_to = literal_value if literal_value is not None else target_val

    # --- Unary operators (only inspect source_val) ---

    if operator == "not_empty":
        passed = source_val is not None and source_val.strip() != ""
        return passed, f"{'PASS' if passed else 'FAIL'}: value is {'not ' if passed else ''}empty"

    if operator == "is_empty":
        passed = source_val is None or source_val.strip() == ""
        return passed, f"{'PASS' if passed else 'FAIL'}: value is {'empty' if passed else 'not empty'}"

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
        verb = "matches" if passed else "does not match"
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' {verb} /{literal_value}/"

    # --- String comparison operators ---

    if operator == "equals":
        passed = source_val == compare_to
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' == '{compare_to}'"

    if operator == "not_equals":
        passed = source_val != compare_to
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' != '{compare_to}'"

    if operator == "contains":
        if source_val is None or compare_to is None:
            return False, "FAIL: cannot check contains with null values"
        passed = compare_to in source_val
        verb = "contains" if passed else "does not contain"
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' {verb} '{compare_to}'"

    if operator == "not_contains":
        if source_val is None:
            return True, "PASS: source is null, trivially does not contain value"
        if compare_to is None:
            return False, "FAIL: cannot check not_contains with null comparison value"
        passed = compare_to not in source_val
        verb = "does not contain" if passed else "contains"
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' {verb} '{compare_to}'"

    # --- Numeric operators ---

    if operator in ("greater_than", "less_than", "greater_equal", "less_equal"):
        if source_val is None or compare_to is None:
            return False, "FAIL: cannot compare null values numerically"
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
        return passed, f"{'PASS' if passed else 'FAIL'}: {src_num} {operator} {cmp_num}"

    # --- Date operators ---

    if operator in ("before", "after"):
        if source_val is None or compare_to is None:
            return False, "FAIL: cannot compare null values as dates"
        src_date = _parse_date(source_val)
        cmp_date = _parse_date(compare_to)
        if src_date is None or cmp_date is None:
            return False, f"FAIL: cannot parse as dates: '{source_val}', '{compare_to}'"
        passed = src_date < cmp_date if operator == "before" else src_date > cmp_date
        return passed, f"{'PASS' if passed else 'FAIL'}: '{source_val}' {operator} '{compare_to}'"

    return False, f"Unknown operator: {operator}"


def evaluate_when(attributes: dict, when: WhenCondition) -> bool:
    """Evaluate a precondition against an object's attributes.

    Returns True if the condition is met (the check should run).
    """
    attr_val = get_attr_value(attributes, when.attr)
    passed, _ = compare(attr_val, None, when.operator, literal_value=when.value)
    return passed
