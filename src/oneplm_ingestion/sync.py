"""Incremental sync orchestration — fetches from Windchill, stores in SQLite."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from oneplm_ingestion.api import WindchillClient
from oneplm_ingestion.db import get_last_sync, update_sync_log, upsert_object
from oneplm_ingestion.models import TypeConfig, WindchillObject

log = logging.getLogger(__name__)


def load_type_configs(config_path: Path) -> list[TypeConfig]:
    """Load types.json and return TypeConfig list."""
    with open(config_path) as f:
        raw = json.load(f)
    return [TypeConfig(**entry) for entry in raw]


def parse_windchill_object(raw: dict, type_config: TypeConfig) -> WindchillObject:
    """Convert a raw API response dict into a WindchillObject.

    API property names from the PTC OData specs:
      ID, Number, Name, State (EnumType with Value), Revision, Version,
      VersionID, LastModified, ObjectType, CabinetName, FolderLocation, etc.
    """
    return WindchillObject(
        id=str(raw.get("ID", "")),
        type_name=type_config.human_name,
        windchill_type=raw.get("ObjectType", type_config.windchill_type),
        number=raw.get("Number"),
        name=raw.get("Name"),
        state=_extract_state(raw),
        revision=raw.get("Revision"),
        last_modified=raw.get("LastModified", ""),
        attributes=raw,  # Store full API payload
    )


def _extract_state(raw: dict) -> str | None:
    """Extract lifecycle state — the State field is an EnumType with a Value key."""
    state = raw.get("State")
    if isinstance(state, dict):
        return state.get("Value")
    return state


def _get_nested_value(data: dict, dotted_key: str) -> str | None:
    """Get a value from a dict using dot notation (e.g., 'ConfigurableModule.Value')."""
    keys = dotted_key.split(".")
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return str(current) if current is not None else None


def _classify_object(raw: dict, type_configs: list[TypeConfig]) -> TypeConfig | None:
    """Determine which TypeConfig a raw object belongs to based on classify_attr/classify_value.

    If no config has classify_attr set, returns the first config.
    If the object doesn't match any classifier, returns None.
    """
    classifying = [tc for tc in type_configs if tc.classify_attr]
    if not classifying:
        # No classification needed — all configs are equivalent, use the first
        return type_configs[0]

    for tc in classifying:
        actual = _get_nested_value(raw, tc.classify_attr)
        if actual == tc.classify_value:
            return tc

    # Object doesn't match any classifier — skip it
    return None


def sync_endpoint(
    client: WindchillClient,
    conn,
    api_endpoint: str,
    type_configs: list[TypeConfig],
    full: bool = False,
) -> dict[str, int]:
    """Sync a single API endpoint, distributing objects across type configs by classification.

    This avoids duplicate API calls when multiple types share the same endpoint
    (e.g., Config Options PDP and Part PDP are both ProductDefinitionPart).
    """
    # Use the earliest last_sync across all types sharing this endpoint
    last_sync = None
    if not full:
        syncs = [get_last_sync(conn, tc.human_name) for tc in type_configs]
        valid_syncs = [s for s in syncs if s is not None]
        last_sync = min(valid_syncs) if valid_syncs else None

    log.info(
        "Fetching %s (types: %s, last_sync=%s, full=%s)",
        api_endpoint,
        [tc.human_name for tc in type_configs],
        last_sync,
        full,
    )

    raw_objects = client.get_objects_by_type(api_endpoint, modified_after=last_sync)
    now = datetime.now(timezone.utc).isoformat()

    counts: dict[str, int] = {tc.human_name: 0 for tc in type_configs}

    for raw in raw_objects:
        tc = _classify_object(raw, type_configs)
        if tc is None:
            continue
        obj = parse_windchill_object(raw, tc)
        obj.synced_at = now
        upsert_object(conn, obj)
        counts[tc.human_name] += 1

    for tc in type_configs:
        update_sync_log(conn, tc.human_name, now, counts[tc.human_name])

    conn.commit()
    for name, count in counts.items():
        log.info("Synced %d objects for %s", count, name)

    return counts


def sync_all(
    client: WindchillClient,
    conn,
    config_path: Path,
    containers_config_path: Path | None = None,
    types: list[str] | None = None,
    full: bool = False,
) -> dict[str, int]:
    """Sync all (or specified) types, then folders if containers config exists.

    Returns {type_name: count} plus {folders/<label>: count} entries.
    Groups type configs by api_endpoint to avoid duplicate API calls.
    """
    all_type_configs = load_type_configs(config_path)
    active_configs = (
        [tc for tc in all_type_configs if tc.human_name in types] if types else all_type_configs
    )

    # Group by endpoint to deduplicate API calls
    by_endpoint: dict[str, list[TypeConfig]] = defaultdict(list)
    for tc in active_configs:
        by_endpoint[tc.api_endpoint].append(tc)

    results: dict[str, int] = {}
    for endpoint, configs in by_endpoint.items():
        counts = sync_endpoint(client, conn, endpoint, configs, full=full)
        results.update(counts)

    if containers_config_path and containers_config_path.exists():
        from oneplm_ingestion.folders import sync_folders
        # Pass the full (unfiltered) type configs so folder sync can fetch any object type
        folder_results = sync_folders(client, conn, containers_config_path, all_type_configs)
        for label, count in folder_results.items():
            log.info("Synced %d folders for container '%s'", count, label)
        results.update({f"folders/{k}": v for k, v in folder_results.items()})

    return results
