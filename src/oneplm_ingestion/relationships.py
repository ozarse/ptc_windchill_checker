"""Relationship sync — fetches and stores relationship metadata for Windchill objects.

For each object discovered during folder sync, this module fetches:
  - Documents:  attachments, DocUsageLinks
  - Parts:      attachments, DescribedBy links, PartDocAssociations

Results are stored in the `relationships` table (delete + insert per source/type).
"""

from __future__ import annotations

import logging

from oneplm_ingestion.db import save_relationships

log = logging.getLogger(__name__)

# Domain → relationship types to fetch for that domain
_REL_TYPES: dict[str, list[str]] = {
    "v6/DocMgmt": ["attachment", "doc_usage_link"],
    "v6/ProdMgmt": ["attachment", "described_by", "part_doc_assoc"],
}


def fetch_and_store_relationships(
    client, conn, object_id: str, domain: str, collection: str, now: str
) -> int:
    """Fetch all applicable relationships for an object and persist them.

    Returns the total number of relationship items stored across all types.
    """
    rel_types = _REL_TYPES.get(domain, [])
    total = 0
    for rel_type in rel_types:
        items = _fetch(client, object_id, domain, collection, rel_type)
        save_relationships(conn, object_id, rel_type, items, now)
        total += len(items)
        log.debug("  %s → %d %s items", object_id, len(items), rel_type)
    return total


def _fetch(
    client, object_id: str, domain: str, collection: str, rel_type: str
) -> list[dict]:
    """Call the appropriate API method for a single relationship type."""
    try:
        if rel_type == "attachment":
            return client.get_attachments(domain, collection, object_id)
        if rel_type == "doc_usage_link":
            return client.get_doc_usage_links(object_id)
        if rel_type == "described_by":
            return client.get_part_described_by(object_id)
        if rel_type == "part_doc_assoc":
            return client.get_part_doc_associations(object_id)
        log.warning("Unknown relationship type: %s", rel_type)
        return []
    except Exception as exc:
        log.warning("  Failed to fetch %s for object %s: %s", rel_type, object_id, exc)
        return []


def domain_for_type(windchill_type: str) -> str | None:
    """Infer the OData domain from a Windchill type string."""
    if windchill_type.startswith("PTC.DocMgmt."):
        return "v6/DocMgmt"
    if windchill_type.startswith("PTC.ProdMgmt."):
        return "v6/ProdMgmt"
    return None


def collection_for_type(windchill_type: str) -> str | None:
    """Infer the OData collection name from a Windchill type string."""
    if windchill_type.startswith("PTC.DocMgmt."):
        return "Documents"
    if windchill_type.startswith("PTC.ProdMgmt."):
        return "Parts"
    return None
