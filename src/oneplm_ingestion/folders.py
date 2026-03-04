"""Folder sync — fetches the folder hierarchy from Windchill containers and stores it in SQLite."""

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
) -> dict[str, int]:
    """Sync folder hierarchy for each configured container, fetching any missing objects.

    Returns a dict mapping container label to number of folders synced.
    """
    containers = load_containers_config(containers_config_path)

    # Build windchill_type → [TypeConfig] index for fast lookup during content fetch
    by_windchill_type: dict[str, list[TypeConfig]] = defaultdict(list)
    for tc in type_configs:
        by_windchill_type[tc.windchill_type].append(tc)

    results: dict[str, int] = {}
    for cfg in containers:
        container_id = cfg["id"]
        label = cfg.get("label", container_id)
        log.info("Syncing folders for container '%s' (%s)", label, container_id)
        count = _sync_container(client, conn, container_id, label, by_windchill_type)
        results[label] = count
    return results


def _sync_container(
    client, conn, container_id: str, label: str,
    by_windchill_type: dict[str, list[TypeConfig]],
) -> int:
    """Sync all folders in one container, fetch/store missing objects, link all to folders."""
    raw_folders = client.get_folders(container_id)
    if not raw_folders:
        log.info("  No folders returned for container '%s'", label)
        return 0

    now = datetime.now(timezone.utc).isoformat()

    # Build location → folder_id map so we can resolve parent_folder_id
    loc_to_id: dict[str, str] = {}
    for raw in raw_folders:
        folder_id = str(raw.get("ID", ""))
        location = raw.get("Location")
        if folder_id and location:
            loc_to_id[location.rstrip("/")] = folder_id

    folders: list[Folder] = []
    for raw in raw_folders:
        folder_id = str(raw.get("ID", ""))
        if not folder_id:
            continue
        location = raw.get("Location")
        parent_folder_id = _resolve_parent(location, loc_to_id)
        folder = Folder(
            id=folder_id,
            container_id=container_id,
            name=raw.get("Name", ""),
            location=location,
            parent_folder_id=parent_folder_id,
            description=raw.get("Description"),
            created_on=raw.get("CreatedOn"),
            last_modified=raw.get("LastModified"),
            synced_at=now,
        )
        upsert_folder(conn, folder)
        folders.append(folder)

    conn.commit()
    log.info("  Upserted %d folders for container '%s'", len(folders), label)

    total_linked = 0
    total_fetched = 0
    for folder in folders:
        linked, fetched = _sync_folder_contents(
            client, conn, container_id, folder, by_windchill_type, now
        )
        total_linked += linked
        total_fetched += fetched

    conn.commit()
    log.info(
        "  Linked %d objects (%d newly fetched) to folders for container '%s'",
        total_linked, total_fetched, label,
    )

    return len(folders)


def _resolve_parent(location: str | None, loc_to_id: dict[str, str]) -> str | None:
    """Derive a folder's parent_folder_id from its location path.

    e.g. location="/Default/SubA/SubB" → parent_path="/Default/SubA"
    """
    if not location:
        return None
    stripped = location.rstrip("/")
    parts = stripped.rsplit("/", 1)
    if len(parts) < 2 or not parts[0]:
        return None  # top-level folder
    parent_path = parts[0]
    return loc_to_id.get(parent_path)


def _sync_folder_contents(
    client, conn, container_id: str, folder: Folder,
    by_windchill_type: dict[str, list[TypeConfig]], now: str,
) -> tuple[int, int]:
    """Fetch FolderContents, store missing objects, fetch relationships, link all to this folder.

    For each content item:
    - If already in the objects table: just ensure folder_id is set.
    - If not in the table and type is known (types.json): fetch the full object
      from its type-specific endpoint and store it.
    - If not in the table and type is unknown: store basic metadata from the
      FolderContent response as a fallback.
    Then fetch relationships (attachments, described_by, etc.) for every item.

    Returns (total_linked, newly_fetched).
    """
    # Deferred to avoid circular import: sync.py imports folders.py lazily
    from oneplm_ingestion.relationships import (
        collection_for_type, domain_for_type, fetch_and_store_relationships,
    )
    from oneplm_ingestion.sync import _classify_object, parse_windchill_object

    contents = client.get_folder_contents(container_id, folder.id)
    linked = 0
    fetched = 0

    for item in contents:
        item_id = str(item.get("ID", ""))
        if not item_id:
            continue

        windchill_type = ""
        domain = collection = None

        row = conn.execute(
            "SELECT id, windchill_type FROM objects WHERE id = ?", (item_id,)
        ).fetchone()

        if not row:
            windchill_type = _extract_windchill_type(item)
            configs = by_windchill_type.get(windchill_type, [])

            if configs:
                # Fetch full object from the type-specific collection endpoint
                tc = configs[0]
                domain, collection = tc.domain, tc.collection
                try:
                    raw = client.get_object(f"{domain}/{collection}", item_id)
                except Exception as exc:
                    log.warning("  Failed to fetch object %s: %s", item_id, exc)
                    continue

                if raw:
                    matched_tc = _classify_object(raw, configs)
                    if matched_tc is None:
                        log.debug(
                            "  Object %s ('%s') didn't match any classifier — skipping",
                            item_id, item.get("Name", ""),
                        )
                        continue
                    obj = parse_windchill_object(raw, matched_tc)
                    obj.synced_at = now
                    upsert_object(conn, obj)
                    fetched += 1
            else:
                # Unknown type — store basic FolderContent metadata as a fallback
                obj = WindchillObject(
                    id=item_id,
                    type_name=windchill_type or "Unknown",
                    windchill_type=windchill_type,
                    number=None,
                    name=item.get("Name"),
                    state=None,
                    revision=None,
                    last_modified=item.get("LastModified", ""),
                    attributes=item,
                    synced_at=now,
                )
                upsert_object(conn, obj)
                fetched += 1
                domain = domain_for_type(windchill_type)
                collection = collection_for_type(windchill_type)
        else:
            # Already in DB — derive domain/collection from stored windchill_type
            windchill_type = row["windchill_type"]
            configs = by_windchill_type.get(windchill_type, [])
            if configs:
                tc = configs[0]
                domain, collection = tc.domain, tc.collection
            else:
                domain = domain_for_type(windchill_type)
                collection = collection_for_type(windchill_type)

        update_object_folder(conn, item_id, folder.id)
        linked += 1

        # Fetch and store relationships for this object
        if domain and collection:
            fetch_and_store_relationships(client, conn, item_id, domain, collection, now)

    return linked, fetched


def _extract_windchill_type(item: dict) -> str:
    """Extract the Windchill type string from an OData FolderContent item.

    OData uses '@odata.type' (e.g. '#PTC.DocMgmt.IFUDrawing') for polymorphic items.
    Windchill may also expose 'ObjectType' directly.
    """
    odata_type = item.get("@odata.type", "")
    if odata_type:
        return odata_type.lstrip("#")
    return item.get("ObjectType", "")
