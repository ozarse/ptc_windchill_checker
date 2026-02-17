"""Lookup: given a document/part number, find associated docs, parts, and containers."""

from __future__ import annotations

import logging

from oneplm_ingestion.api import DOCMGMT, PRODMGMT, WindchillClient

log = logging.getLogger(__name__)


def lookup_by_number(client: WindchillClient, number: str) -> dict:
    """Search for a number across Documents and Parts, then resolve relationships.

    Returns a dict with:
      - "documents": list of matching document dicts
      - "parts": list of matching part dicts
      - "related_documents": list of documents found via part->DescribedBy links
      - "related_parts": list of parts found via document->DocUsageLinks (if available)
      - "containers": list of container dicts for all found objects
    """
    result = {
        "documents": [],
        "parts": [],
        "related_documents": [],
        "related_parts": [],
        "containers": [],
    }

    seen_containers: set[str] = set()

    # Search Documents
    log.info("Searching Documents for Number='%s'", number)
    docs = client.find_by_number(f"{DOCMGMT}/Documents", number)
    result["documents"] = docs
    for doc in docs:
        _resolve_document_context(client, doc, result, seen_containers)

    # Search Parts
    log.info("Searching Parts for Number='%s'", number)
    parts = client.find_by_number(f"{PRODMGMT}/Parts", number)
    result["parts"] = parts
    for part in parts:
        _resolve_part_relationships(client, part, result, seen_containers)

    return result


def _resolve_document_context(
    client: WindchillClient, doc: dict, result: dict, seen_containers: set[str]
) -> None:
    """Resolve container for a document."""
    doc_id = doc.get("ID")
    if not doc_id:
        return

    # Get container
    try:
        ctx = client.get_document_context(doc_id)
        container_id = ctx.get("ID", "")
        if container_id and container_id not in seen_containers:
            seen_containers.add(container_id)
            result["containers"].append(ctx)
    except Exception:
        log.debug("Could not fetch context for document %s", doc_id, exc_info=True)


def _resolve_part_relationships(
    client: WindchillClient, part: dict, result: dict, seen_containers: set[str]
) -> None:
    """Resolve documents described by a part, and its container."""
    part_id = part.get("ID")
    if not part_id:
        return

    # Get container
    try:
        ctx = client.get_part_context(part_id)
        container_id = ctx.get("ID", "")
        if container_id and container_id not in seen_containers:
            seen_containers.add(container_id)
            result["containers"].append(ctx)
    except Exception:
        log.debug("Could not fetch context for part %s", part_id, exc_info=True)

    # Get documents that describe this part
    try:
        describe_links = client.get_part_described_by(part_id)
        for link in describe_links:
            link_id = link.get("ID")
            if not link_id:
                continue
            try:
                doc = client.get_described_by_document(part_id, link_id)
                result["related_documents"].append(doc)
                _resolve_document_context(client, doc, result, seen_containers)
            except Exception:
                log.debug("Could not follow DescribedBy link %s", link_id, exc_info=True)
    except Exception:
        log.debug("Could not fetch DescribedBy for part %s", part_id, exc_info=True)

    # Get PartDocAssociations (CAD docs, etc.)
    try:
        associations = client.get_part_doc_associations(part_id)
        for assoc in associations:
            result["related_documents"].append(assoc)
    except Exception:
        log.debug("Could not fetch PartDocAssociations for part %s", part_id, exc_info=True)


def format_lookup_result(result: dict) -> str:
    """Format a lookup result for CLI display."""
    lines = []

    if result["documents"]:
        lines.append(f"Documents ({len(result['documents'])}):")
        for doc in result["documents"]:
            state = doc.get("State", {})
            state_val = state.get("Value") if isinstance(state, dict) else state
            lines.append(
                f"  {doc.get('Number', '?'):20s}  {doc.get('Name', ''):40s}  "
                f"Rev {doc.get('Revision', '?'):5s}  {state_val or ''}"
            )

    if result["parts"]:
        lines.append(f"\nParts ({len(result['parts'])}):")
        for part in result["parts"]:
            state = part.get("State", {})
            state_val = state.get("Value") if isinstance(state, dict) else state
            lines.append(
                f"  {part.get('Number', '?'):20s}  {part.get('Name', ''):40s}  "
                f"Rev {part.get('Revision', '?'):5s}  {state_val or ''}"
            )

    if result["related_documents"]:
        lines.append(f"\nRelated Documents ({len(result['related_documents'])}):")
        for doc in result["related_documents"]:
            lines.append(
                f"  {doc.get('Number', '?'):20s}  {doc.get('Name', ''):40s}  "
                f"Type: {doc.get('ObjectType', doc.get('DocTypeName', '?'))}"
            )

    if result["containers"]:
        lines.append(f"\nContainers ({len(result['containers'])}):")
        for ctx in result["containers"]:
            lines.append(f"  {ctx.get('Name', ctx.get('ID', '?'))}")

    if not any(result.values()):
        lines.append(f"No documents or parts found with number matching the query.")

    return "\n".join(lines)
