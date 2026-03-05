"""Folder sync — fetches the complete folder hierarchy and all contained objects.

Step 1 (folders): A single API call per container retrieves the full tree:
  GET /v6/DataAdmin/Containers('{id}')/Folders?$expand=Folders($levels=max)
  The nested response is walked locally and every folder is upserted.

Step 2 (objects): For each folder, FolderContents is fetched:
  GET /v6/DataAdmin/Containers('{id}')/Folders('{fid}')/FolderContents
  Each item is fetched in full from its type-specific endpoint and stored in the
  objects table with folder_id set.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from oneplm_ingestion.db import update_object_folder, upsert_folder, upsert_object
from oneplm_ingestion.models import Folder, TypeConfig, WindchillObject

log = logging.getLogger(__name__)


def load_containers_config(path: Path) -> list[dict]:
    """Load containers.json and return a list of container config dicts."""
    with open(path) as f:
        return json.load(f)


def sync_folders(
    client, conn, containers_config_path: Path, type_configs: list[TypeConfig]
) -> dict[str, tuple[int, int]]:
    """Sync folder hierarchy and all contained objects for each configured container.

    Returns a dict mapping container label to (folder_count, object_count).
    """
    containers = load_containers_config(containers_config_path)

    by_windchill_type: dict[str, list[TypeConfig]] = defaultdict(list)
    for tc in type_configs:
        by_windchill_type[tc.windchill_type].append(tc)

    results: dict[str, tuple[int, int]] = {}
    for cfg in containers:
        container_id = cfg["id"]
        label = cfg.get("label", container_id)
        folder_paths = cfg.get("folder_paths") or None
        log.info("Syncing folders for container '%s' (%s)", label, container_id)
        if folder_paths:
            log.info("  Filtering to folder_paths: %s", folder_paths)
        folder_count, object_count = _sync_container(
            client, conn, container_id, label, by_windchill_type, folder_paths=folder_paths
        )
        results[label] = (folder_count, object_count)
    return results


def _sync_container(
    client, conn, container_id: str, label: str,
    by_windchill_type: dict[str, list[TypeConfig]],
    folder_paths: list[str] | None = None,
) -> tuple[int, int]:
    """Fetch the full folder tree and all folder contents for one container.

    If folder_paths is provided, only folders whose location starts with one of
    those prefixes will have their contents fetched (all folders are still upserted).
    """
    now = datetime.now(timezone.utc).isoformat()

    top_level = client.get_folders(container_id)
    if not top_level:
        log.info("  No folders returned for container '%s'", label)
        return 0, 0

    # Walk the tree, upsert all folders, collect (id, full_path, location) tuples
    folder_entries = _walk_folder_tree(conn, container_id, top_level, parent_folder_id=None, now=now)
    conn.commit()
    log.info("  Upserted %d folders for container '%s'", len(folder_entries), label)

    # Apply location-prefix filter if configured
    content_entries = folder_entries
    if folder_paths:
        content_entries = [
            (fid, fpath, loc) for fid, fpath, loc in folder_entries
            if loc and any(loc.startswith(fp) for fp in folder_paths)
        ]
        log.info("  %d folders match folder_paths filter", len(content_entries))

    # Fetch and store contents of every folder, committing after each
    total_objects = 0
    for folder_id, folder_path, _ in content_entries:
        total_objects += _sync_folder_contents(
            client, conn, container_id, folder_id, folder_path, by_windchill_type, now
        )
        conn.commit()
    log.info(
        "  Stored %d objects across %d folders for container '%s'",
        total_objects, len(content_entries), label,
    )

    return len(folder_entries), total_objects


def _walk_folder_tree(
    conn, container_id: str, folders: list[dict], parent_folder_id: str | None, now: str,
    ancestor_path: list[str] | None = None,
) -> list[tuple[str, list[str], str | None]]:
    """Recursively walk the $expand=Folders($levels=max) response, upsert each folder.

    Returns a flat list of (folder_id, full_path, full_location) tuples where:
      - full_path: ordered list of ancestor IDs from cabinet down to this folder (inclusive)
      - full_location: the folder's own full path (API Location + "/" + Name, e.g. "/Default/01 - Parts")
    """
    if ancestor_path is None:
        ancestor_path = []
    entries: list[tuple[str, list[str], str | None]] = []
    for raw in folders:
        folder_id = str(raw.get("ID", ""))
        if not folder_id:
            continue
        folder = _make_folder(raw, container_id, parent_folder_id=parent_folder_id, now=now)
        upsert_folder(conn, folder)
        current_path = ancestor_path + [folder_id]
        # Build the folder's own full path: Location is the parent path, so append the name
        parent_loc = folder.location or ""
        full_location = f"{parent_loc}/{folder.name}" if parent_loc else f"/{folder.name}"
        entries.append((folder_id, current_path, full_location))
        log.debug("  folder: %r  full_location: %r", folder.name, full_location)
        children = raw.get("Folders") or []
        if children:
            entries.extend(
                _walk_folder_tree(
                    conn, container_id, children,
                    parent_folder_id=folder_id, now=now,
                    ancestor_path=current_path,
                )
            )
    return entries


def _sync_folder_contents(
    client, conn, container_id: str, folder_id: str, folder_path: list[str],
    by_windchill_type: dict[str, list[TypeConfig]], now: str,
) -> int:
    """Fetch FolderContents, retrieve each item in full, and store in the objects table.

    Returns the number of objects stored.
    """
    # Deferred to avoid circular import
    from oneplm_ingestion.sync import _classify_object, parse_windchill_object

    contents = client.get_folder_contents(container_id, folder_path)
    count = 0

    for item in contents:
        item_id = str(item.get("ID", ""))
        if not item_id:
            continue

        windchill_type = item.get("@odata.type", "").lstrip("#")
        domain_collection = _domain_and_collection(windchill_type, by_windchill_type)
        if domain_collection is None:
            log.debug("  Unknown type '%s' for item %s — skipping", windchill_type, item_id)
            continue

        domain, collection = domain_collection

        try:
            raw = client.get_object(f"{domain}/{collection}", item_id)
        except Exception as exc:
            log.warning("  Failed to fetch %s (%s): %s", item_id, windchill_type, exc)
            continue

        if not raw:
            continue

        configs = by_windchill_type.get(windchill_type, [])
        if configs:
            tc = _classify_object(raw, configs)
            obj = parse_windchill_object(raw, tc) if tc else _fallback_object(raw, windchill_type, now)
        else:
            obj = _fallback_object(raw, windchill_type, now)

        obj.synced_at = now
        upsert_object(conn, obj)
        update_object_folder(conn, item_id, folder_id)
        count += 1

    return count


def _domain_and_collection(
    windchill_type: str, by_windchill_type: dict[str, list[TypeConfig]]
) -> tuple[str, str] | None:
    """Return (domain, collection) for a Windchill type string.

    Uses types.json config first; falls back to namespace pattern matching.
    Returns None if the type is completely unrecognised.
    """
    configs = by_windchill_type.get(windchill_type)
    if configs:
        return configs[0].domain, configs[0].collection
    if "DocMgmt" in windchill_type:
        return "v6/DocMgmt", "Documents"
    if "ProdMgmt" in windchill_type:
        return "v6/ProdMgmt", "Parts"
    return None


def _fallback_object(raw: dict, windchill_type: str, now: str) -> WindchillObject:
    """Create a WindchillObject for a type not listed in types.json."""
    state = raw.get("State")
    return WindchillObject(
        id=str(raw.get("ID", "")),
        type_name=windchill_type,
        windchill_type=windchill_type,
        number=raw.get("Number"),
        name=raw.get("Name"),
        state=state.get("Value") if isinstance(state, dict) else state,
        revision=raw.get("Revision"),
        last_modified=raw.get("LastModified", ""),
        attributes=raw,
        synced_at=now,
    )


def _make_folder(
    raw: dict, container_id: str, parent_folder_id: str | None, now: str
) -> Folder:
    return Folder(
        id=str(raw.get("ID", "")),
        container_id=container_id,
        name=raw.get("Name", ""),
        location=raw.get("Location"),
        parent_folder_id=parent_folder_id,
        description=raw.get("Description"),
        created_on=raw.get("CreatedOn"),
        last_modified=raw.get("LastModified"),
        synced_at=now,
    )
