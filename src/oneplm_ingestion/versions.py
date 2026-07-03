"""Version history sync and the previous-version lifecycle check.

``fetch_and_store_versions`` calls the Windchill Versions API for an object and
stores every version in the ``versions`` table so checks can run offline.

The registered check ``previous_versions_not_in_concept`` flags any object whose
*previous* (non-latest) versions are still in a concept-phase lifecycle state.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from oneplm_ingestion.db import (
    get_all_objects,
    get_versions_for_object,
    save_versions,
)
from oneplm_ingestion.models import CheckResult
from oneplm_ingestion.registry import register_check
from oneplm_ingestion.relationships import collection_for_type, domain_for_type

log = logging.getLogger(__name__)

CHECK_NAME = "Previous Versions Not In Concept"

# Lifecycle state(s) that count as "concept phase". Matched case-insensitively
# against each version's State.Value AND State.Display, so either the internal
# code or the human label may be listed.
# CONFIRM these against your Windchill lifecycle ("Stryker Three Phase Development").
CONCEPT_STATES: frozenset[str] = frozenset({"concept"})


def fetch_and_store_versions(client, conn, obj, now: str) -> int:
    """Fetch and store the version history for one object. Returns count stored."""
    domain = domain_for_type(obj.windchill_type)
    collection = collection_for_type(obj.windchill_type)
    if not domain or not collection:
        return 0
    versions = client.get_versions(domain, collection, obj.id)
    save_versions(conn, obj.id, versions, now)
    log.debug("  %s -> %d versions", obj.id, len(versions))
    return len(versions)


def _is_concept(version: dict) -> bool:
    """Whether a stored version row is in a concept-phase state."""
    for key in ("state_value", "state_display"):
        val = version.get(key)
        if val and val.strip().lower() in CONCEPT_STATES:
            return True
    return False


@register_check("previous_versions_not_in_concept")
def check_previous_versions(conn) -> list[CheckResult]:
    """Flag objects whose previous (non-latest) versions are still in concept phase."""
    now = datetime.now(timezone.utc).isoformat()
    results: list[CheckResult] = []

    for obj in get_all_objects(conn):
        versions = get_versions_for_object(conn, obj.id)
        if not versions:
            continue  # no version history synced for this object
        previous = [v for v in versions if not v["is_latest"]]
        if not previous:
            continue  # the check only applies when previous versions exist

        offenders = [v for v in previous if _is_concept(v)]
        passed = not offenders
        if passed:
            message = f"PASS: {len(previous)} previous version(s), none in concept phase"
        else:
            labels = ", ".join(
                f"{v['version'] or v['revision'] or v['version_oid']}"
                f"={v['state_display'] or v['state_value']}"
                for v in offenders
            )
            message = f"FAIL: previous version(s) in concept phase: {labels}"

        results.append(CheckResult(
            check_name=CHECK_NAME,
            source_object_id=obj.id,
            target_object_id="VERSIONS",
            source_attr="State",
            target_attr="Previous Versions",
            source_value=obj.number,
            target_value=str(len(previous)),
            passed=passed,
            message=message,
            checked_at=now,
        ))

    return results
