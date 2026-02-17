"""CSV export logic for objects and check results."""

from __future__ import annotations

import csv
import json
from pathlib import Path

from oneplm_ingestion.db import get_all_objects, get_check_results, get_objects_by_type


def export_objects(conn, type_name: str | None, output_path: Path) -> int:
    """Export objects to CSV. If type_name is None, export all types."""
    if type_name:
        objects = get_objects_by_type(conn, type_name)
    else:
        objects = get_all_objects(conn)

    if not objects:
        return 0

    # Collect all unique attribute keys across all objects
    all_keys: set[str] = set()
    for obj in objects:
        all_keys.update(obj.attributes.keys())

    # Put common columns first, then sorted remaining attributes
    priority_cols = ["type_name", "number", "name", "state", "revision", "last_modified"]
    attr_cols = sorted(all_keys - set(priority_cols))
    fieldnames = priority_cols + attr_cols

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for obj in objects:
            row = {
                "type_name": obj.type_name,
                "number": obj.number,
                "name": obj.name,
                "state": obj.state,
                "revision": obj.revision,
                "last_modified": obj.last_modified,
            }
            for key in attr_cols:
                val = obj.attributes.get(key)
                if isinstance(val, (dict, list)):
                    row[key] = json.dumps(val)
                else:
                    row[key] = val
            writer.writerow(row)

    return len(objects)


def export_check_results(
    conn,
    output_path: Path,
    check_name: str | None = None,
    failed_only: bool = False,
) -> int:
    """Export check results to CSV."""
    results = get_check_results(conn, check_name=check_name, failed_only=failed_only)
    if not results:
        return 0

    fieldnames = [
        "check_name", "source_object_id", "target_object_id",
        "source_attr", "target_attr", "source_value", "target_value",
        "passed", "message", "checked_at",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "check_name": r.check_name,
                "source_object_id": r.source_object_id,
                "target_object_id": r.target_object_id,
                "source_attr": r.source_attr,
                "target_attr": r.target_attr,
                "source_value": r.source_value,
                "target_value": r.target_value,
                "passed": r.passed,
                "message": r.message,
                "checked_at": r.checked_at,
            })

    return len(results)
