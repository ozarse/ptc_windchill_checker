"""Domain dataclasses for Windchill objects, PDFs, checks, and configuration."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class WindchillObject:
    """A single object fetched from Windchill."""

    id: str
    type_name: str  # Human name: "Config PDP", "IFU PDP", "IFU Drawing", etc.
    windchill_type: str  # Windchill internal type ID
    number: str | None
    name: str | None
    state: str | None
    revision: str | None
    last_modified: str  # ISO 8601 from Windchill
    attributes: dict  # Full attribute dictionary from API
    synced_at: str = ""  # Populated on save


@dataclass
class PDFContent:
    """A PDF file associated with a Windchill object."""

    object_id: str
    content_role: str  # "primary" or "attachment"
    filename: str
    local_path: str | None = None
    extracted_text: str | None = None
    download_url: str | None = None
    downloaded_at: str | None = None
    extracted_at: str | None = None
    id: int | None = None  # DB-assigned


@dataclass
class CheckResult:
    """Result of a single attribute comparison check."""

    check_name: str
    source_object_id: str
    target_object_id: str
    source_attr: str
    target_attr: str
    source_value: str | None
    target_value: str | None
    passed: bool
    message: str
    checked_at: str = ""
    id: int | None = None


@dataclass
class TypeConfig:
    """Mapping of a human-readable type name to Windchill type info."""

    human_name: str
    windchill_type: str
    api_endpoint: str  # Relative URL path for this type's collection
    domain: str = "v6/DocMgmt"  # OData domain base path
    collection: str = "Documents"  # "Documents" or "Parts"
    classify_attr: str = ""  # If set, classify objects by this attribute's value
    classify_value: str = ""  # The value that assigns an object to this type
    attributes: list[str] = field(default_factory=list)  # Empty = track all


@dataclass
class Folder:
    """A folder in the Windchill container/folder hierarchy."""

    id: str
    container_id: str
    name: str
    location: str | None  # Full path, e.g. "/Default/SubA"
    parent_folder_id: str | None = None
    description: str | None = None
    created_on: str | None = None
    last_modified: str | None = None
    synced_at: str = ""


@dataclass
class WhenCondition:
    """Precondition that must be met for a comparison to run."""

    attr: str
    operator: str
    value: str | None = None


@dataclass
class Assertion:
    """A single-record attribute assertion within an attribute check."""

    attr: str
    operator: str = "not_empty"
    value: str | None = None
    when: WhenCondition | None = None


@dataclass
class RelationshipComparison:
    """Compares a source record's attribute against a related record's attribute."""

    source_attr: str
    target_attr: str | None = None
    operator: str = "equals"
    value: str | None = None
    when: WhenCondition | None = None


@dataclass
class AttributeCheck:
    """Validates attributes of individual records of a single type."""

    name: str
    type: str  # Logical type name, e.g. "Config PDP"
    description: str = ""
    assertions: list[Assertion] = field(default_factory=list)
    kind: str = "attribute"


@dataclass
class RelationshipCheck:
    """Validates a record against records reached through a relationship.

    ``via`` names the relationship traversed from the source ``type`` to the
    ``related_type`` (one of: describes, described_by, uses, used_by).
    ``on_missing`` is "fail" or "skip" when no related record is found.
    """

    name: str
    type: str
    related_type: str
    via: str
    description: str = ""
    comparisons: list[RelationshipComparison] = field(default_factory=list)
    on_missing: str = "fail"
    kind: str = "relationship"


@dataclass
class PythonCheck:
    """Delegates to a registered Python function for complex one-off logic."""

    name: str
    function: str  # Key in checks.registry.CHECK_REGISTRY
    description: str = ""
    kind: str = "python"
