"""Classify each Config PDP as eIFU / Hybrid / Print based on its IFU PDPs.

A Config PDP `uses` one or more IFU PDPs. Whether the product ships an electronic
IFU (eIFU), a printed IFU, or both is inferred from the IFU PDP part numbers: an
electronic language variant's number ends in a two-letter language suffix
(``-XX``, e.g. ``...-EN``, ``...-FR``), while a printed insert's number does not.

Classification for a Config PDP's related IFU PDPs:

  - all numbers language-suffixed          -> eIFU
  - exactly one not suffixed (rest are)    -> Hybrid (eIFU + print)
  - none suffixed                          -> Print, flagged "needs review"
  - anything else (mixed, or no IFU PDPs)  -> Needs Review

Once classified, each IFU PDP is attribute-checked according to its class (see
the EIFU_EXPECTATIONS / HYBRID_* tables): eIFU parts and the electronic parts of
a hybrid must be flagged electronic with unit "As Needed" and quantity 0; the
printed part of a hybrid must be flagged non-electronic with unit "Piece".

Each Config PDP yields a classification CheckResult (``source_value`` is the
label) followed by one CheckResult per IFU PDP per attribute. The classification
row passes only when the class is confident (eIFU/Hybrid) and every attribute
check passed; review cases and attribute failures fail, so they surface in
``export checks --failed-only``.

Registered as the Python check ``config_pdp_ifu_classification``. The related
IFU PDPs are read offline from the relationships table, so ``sync relationships``
must have run first.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from oneplm_ingestion.content_checks import ISO_639_1_CODES
from oneplm_ingestion.db import (
    get_object_by_id,
    get_objects_by_type,
    get_relationship_target_ids,
    save_check_results,
)
from oneplm_ingestion.models import CheckResult
from oneplm_ingestion.operators import get_attr_value
from oneplm_ingestion.registry import register_check

log = logging.getLogger(__name__)

CHECK_NAME = "Config PDP IFU Classification"

# Config PDP --uses--> IFU PDP. Change to "used_by" if the intended direction is
# "the Config PDPs that use a given IFU PDP" instead.
_RELATIONSHIP = "uses"

# Cap how many part numbers are listed in a message.
_MAX_LISTED = 8

# ---------------------------------------------------------------------------
# Attribute compliance configuration — CONFIRM THESE AGAINST YOUR WINDCHILL DATA.
#
# These are the attribute keys (dot notation into the stored JSON payload; a
# trailing ".Value" reads an OData enum) and the expected values used once a
# Config PDP is classified. Best-guess placeholders — edit to match reality.
# Note "eIFU Only" (eIFU case) and "eIFU Flag" (hybrid case) are kept as two
# separate keys; set them to the same key if they are in fact one attribute.
# ---------------------------------------------------------------------------
ATTR_EIFU_ONLY = "eIFUOnly.Value"        # eIFU case, expected "Yes"
ATTR_EIFU_FLAG = "eIFUFlag.Value"        # hybrid per-part flag, "Yes"/"No"
ATTR_DEFAULT_UNIT = "DefaultUnit.Value"  # expected "As Needed" or "Piece"
ATTR_QUANTITY = "Quantity"               # expected "0"

VAL_YES = "Yes"
VAL_NO = "No"
VAL_UNIT_AS_NEEDED = "As Needed"
VAL_UNIT_PIECE = "Piece"
VAL_QUANTITY_ZERO = "0"

# (attribute key, expected value, human label) tuples per classification.
EIFU_EXPECTATIONS = [
    (ATTR_EIFU_ONLY, VAL_YES, "eIFU Only"),
    (ATTR_DEFAULT_UNIT, VAL_UNIT_AS_NEEDED, "Default Unit"),
    (ATTR_QUANTITY, VAL_QUANTITY_ZERO, "Quantity"),
]
HYBRID_ELECTRONIC_EXPECTATIONS = [
    (ATTR_EIFU_FLAG, VAL_YES, "eIFU Flag"),
    (ATTR_DEFAULT_UNIT, VAL_UNIT_AS_NEEDED, "Default Unit"),
    (ATTR_QUANTITY, VAL_QUANTITY_ZERO, "Quantity"),
]
HYBRID_PRINT_EXPECTATIONS = [
    (ATTR_EIFU_FLAG, VAL_NO, "eIFU Flag"),
    (ATTR_DEFAULT_UNIT, VAL_UNIT_PIECE, "Default Unit"),
]


def has_language_suffix(number: str | None) -> bool:
    """True if ``number`` ends in ``-XX`` where XX is a valid ISO 639-1 code."""
    if not number or len(number) < 3 or number[-3] != "-":
        return False
    return number[-2:].lower() in ISO_639_1_CODES


def _listed(numbers: list[str]) -> str:
    shown = numbers[:_MAX_LISTED]
    suffix = ", …" if len(numbers) > _MAX_LISTED else ""
    return ", ".join(shown) + suffix


def classify(matched: list[str], unmatched: list[str]) -> tuple[str, bool, str]:
    """Return (label, passed, message) for one Config PDP's IFU PDP numbers."""
    total = len(matched) + len(unmatched)

    if total == 0:
        return (
            "Needs Review", False,
            "No related IFU PDPs found via 'uses' — has 'sync relationships' been run?",
        )
    if not unmatched:
        return (
            "eIFU", True,
            f"All {total} IFU PDP number(s) carry a -XX language suffix",
        )
    if len(unmatched) == 1 and matched:
        return (
            "Hybrid (eIFU + print)", True,
            f"{len(matched)} language-coded + 1 non-coded ({unmatched[0]})",
        )
    if not matched:
        return (
            "Print", False,
            f"No IFU PDP numbers are language-coded ({_listed(unmatched)}); "
            "likely print — needs review",
        )
    return (
        "Needs Review", False,
        f"{len(matched)} language-coded, {len(unmatched)} not "
        f"({_listed(unmatched)}) — ambiguous mix",
    )


def _related_ifu_pdps(conn, config_id: str) -> list:
    """Load the IFU PDP records related to a Config PDP via the relationship table."""
    ifu_pdps = []
    for rid in get_relationship_target_ids(conn, config_id, _RELATIONSHIP):
        obj = get_object_by_id(conn, rid)
        if obj is not None and obj.type_name == "IFU PDP":
            ifu_pdps.append(obj)
    return ifu_pdps


def _validate_attrs(ifu_pdp, config_id: str, expectations, now: str) -> list[CheckResult]:
    """Check one IFU PDP's attributes against a set of (key, expected, label) rules."""
    out = []
    for attr, expected, label in expectations:
        actual = get_attr_value(ifu_pdp.attributes, attr)
        passed = actual == expected
        out.append(CheckResult(
            check_name=CHECK_NAME,
            source_object_id=ifu_pdp.id,
            target_object_id=config_id,
            source_attr=attr,
            target_attr=label,
            source_value=actual,
            target_value=expected,
            passed=passed,
            message=(
                f"{'PASS' if passed else 'FAIL'}: {ifu_pdp.number or ifu_pdp.id} "
                f"{label} is '{actual}', expected '{expected}'"
            ),
            checked_at=now,
        ))
    return out


def _attribute_results(label, coded, uncoded, config_id, now) -> list[CheckResult]:
    """Per-IFU-PDP attribute checks appropriate to the classification."""
    out: list[CheckResult] = []
    if label == "eIFU":
        for o in coded + uncoded:
            out += _validate_attrs(o, config_id, EIFU_EXPECTATIONS, now)
    elif label.startswith("Hybrid"):
        for o in coded:
            out += _validate_attrs(o, config_id, HYBRID_ELECTRONIC_EXPECTATIONS, now)
        for o in uncoded:
            out += _validate_attrs(o, config_id, HYBRID_PRINT_EXPECTATIONS, now)
    # Print / Needs Review: classification row already flags for review, no attr checks.
    return out


@register_check("config_pdp_ifu_classification")
def classify_config_pdps(conn) -> list[CheckResult]:
    """Classify every Config PDP by its IFU PDPs, then attribute-check per class."""
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    configs = get_objects_by_type(conn, "Config PDP")
    log.info("Classifying %d Config PDPs by IFU type", len(configs))

    for config in configs:
        ifu_pdps = _related_ifu_pdps(conn, config.id)
        coded = [o for o in ifu_pdps if has_language_suffix(o.number)]
        uncoded = [o for o in ifu_pdps if not has_language_suffix(o.number)]
        label, classified_ok, msg = classify(
            [o.number or "" for o in coded], [o.number or "" for o in uncoded]
        )

        attr_results = _attribute_results(label, coded, uncoded, config.id, now)
        attr_failed = sum(1 for r in attr_results if not r.passed)

        summary = msg
        if attr_failed:
            summary += f" — {attr_failed} attribute check(s) failed"

        results.append(CheckResult(
            check_name=CHECK_NAME,
            source_object_id=config.id,
            target_object_id="CLASSIFICATION",
            source_attr="IFU Type",
            target_attr="",
            source_value=label,
            target_value=f"{len(coded)}/{len(ifu_pdps)} language-coded",
            passed=classified_ok and attr_failed == 0,
            message=summary,
            checked_at=now,
        ))
        results += attr_results

    return results


def run_and_save(conn) -> list[CheckResult]:
    """Run the classification and persist results (for direct/standalone use)."""
    results = classify_config_pdps(conn)
    save_check_results(conn, results)
    conn.commit()
    return results
