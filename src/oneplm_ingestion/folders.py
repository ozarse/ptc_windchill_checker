"""Folder sync — fetches the folder hierarchy from Windchill containers and stores it in SQLite.

The hierarchy is walked recursively:
  1. GET /Containers('{id}')/Folders               → top-level folders in the container
  2. GET /Containers('{id}')/Folders('{fid}')/Folders → subfolders of a given folder (repeat)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from oneplm_ingestion.db import upsert_folder
from oneplm_ingestion.models import Folder

log = logging.getLogger(__name__)


def load_containers_config(path: Path) -> list[dict]:
    """Load containers.json and return a list of container config dicts."""
    with open(path) as f:
        return json.load(f)


def sync_folders(client, conn, containers_config_path: Path) -> dict[str, int]:
    """Sync folder hierarchy for each configured container.

    Returns a dict mapping container label to total number of folders upserted.
    """
    containers = load_containers_config(containers_config_path)
    results: dict[str, int] = {}
    for cfg in containers:
        container_id = cfg["id"]
        label = cfg.get("label", container_id)
        log.info("Syncing folders for container '%s' (%s)", label, container_id)
        count = _sync_container(client, conn, container_id, label)
        results[label] = count
    return results


def _sync_container(client, conn, container_id: str, label: str) -> int:
    """Walk the full folder tree for one container and upsert every folder found."""
    now = datetime.now(timezone.utc).isoformat()

    top_level = client.get_folders(container_id)
    if not top_level:
        log.info("  No folders returned for container '%s'", label)
        return 0

    total = 0
    for raw in top_level:
        folder_id = str(raw.get("ID", ""))
        if not folder_id:
            continue
        folder = _make_folder(raw, container_id, parent_folder_id=None, now=now)
        upsert_folder(conn, folder)
        total += 1
        total += _fetch_subfolders_recursive(client, conn, container_id, folder_id, now)

    conn.commit()
    log.info("  Upserted %d folders for container '%s'", total, label)
    return total


def _fetch_subfolders_recursive(
    client, conn, container_id: str, parent_folder_id: str, now: str
) -> int:
    """Recursively fetch and upsert subfolders of a given folder.

    Returns the total number of folders inserted at this level and below.
    """
    subfolders = client.get_subfolders(container_id, parent_folder_id)
    if not subfolders:
        log.debug("    No subfolders for folder %s", parent_folder_id)
        return 0

    count = 0
    for raw in subfolders:
        folder_id = str(raw.get("ID", ""))
        if not folder_id:
            continue
        folder = _make_folder(raw, container_id, parent_folder_id=parent_folder_id, now=now)
        upsert_folder(conn, folder)
        count += 1
        count += _fetch_subfolders_recursive(client, conn, container_id, folder_id, now)

    return count


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
