"""Domain dataclasses for Windchill objects, PDFs, checks, and configuration."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class WindchillObject:
    """A single object fetched from Windchill."""

    id: str
    type_name: str  # Human name: "Config Options PDP", "Part PDP", etc.
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
class WhenCondition:
    """Precondition that must be met for a comparison to run."""

    attr: str
    operator: str
    value: str | None = None


@dataclass
class Comparison:
    """A single attribute comparison within a check rule."""

    source_attr: str
    operator: str = "equals"
    target_attr: str | None = None
    value: str | None = None
    when: WhenCondition | None = None


@dataclass
class CheckConfig:
    """Definition of an attribute comparison rule."""

    name: str
    description: str
    source_type: str  # Human type name of source object
    target_type: str  # Human type name of target object
    match_on: str  # Attribute used to pair source <-> target objects
    comparisons: list[Comparison] = field(default_factory=list)
