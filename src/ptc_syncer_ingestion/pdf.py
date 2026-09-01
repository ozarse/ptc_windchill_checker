"""PDF download and text extraction via docling."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from pathlib import Path

from ptc_syncer_ingestion.api import WindchillClient
from ptc_syncer_ingestion.db import get_pdfs_for_object, upsert_pdf
from ptc_syncer_ingestion.models import PDFContent

log = logging.getLogger(__name__)

# Characters that are illegal in Windows filenames (colon appears in Windchill IDs).
_INVALID_FILENAME_CHARS = re.compile(r'[<>:"/\\|?*]')


def safe_filename(name: str) -> str:
    """Replace characters that are invalid in filenames (notably ':' on Windows)."""
    return _INVALID_FILENAME_CHARS.sub("_", name)


def _parse_ts(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp (handles trailing 'Z'), assuming UTC if naive."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def _changed_since_download(last_modified: str | None, downloaded_at: str | None) -> bool:
    """True if the object was modified in Windchill after we last downloaded its content."""
    lm = _parse_ts(last_modified)
    dl = _parse_ts(downloaded_at)
    if lm is None or dl is None:
        return False
    return lm > dl


def ensure_pdf_dir(data_dir: Path) -> Path:
    """Create the pdfs subdirectory if needed."""
    pdf_dir = data_dir / "pdfs"
    pdf_dir.mkdir(parents=True, exist_ok=True)
    return pdf_dir


def fetch_pdf_metadata_for_object(
    client: WindchillClient,
    conn,
    object_id: str,
    domain: str = "v6/DocMgmt",
    collection: str = "Documents",
    include_attachments: bool = True,
) -> list[PDFContent]:
    """Fetch PDF content URLs and store metadata in DB without downloading files."""
    pdf_infos = client.get_pdf_content_urls(
        object_id, domain=domain, collection=collection, include_attachments=include_attachments
    )

    results = []
    for info in pdf_infos:
        pdf = PDFContent(
            object_id=object_id,
            content_role=info.get("role", "primary"),
            filename=info["filename"],
            download_url=info["url"],
        )
        pdf.id = upsert_pdf(conn, pdf)
        results.append(pdf)
    conn.commit()
    return results


def download_pdfs_for_object(
    client: WindchillClient,
    conn,
    object_id: str,
    data_dir: Path,
    domain: str = "v6/DocMgmt",
    collection: str = "Documents",
    include_attachments: bool = True,
    force: bool = False,
    last_modified: str | None = None,
) -> list[PDFContent]:
    """Download all PDFs for an object, save to disk, record in DB.

    Existing files on disk are skipped unless ``force`` is set or the object's
    ``last_modified`` timestamp is newer than the stored ``downloaded_at`` (i.e.
    the drawing changed in Windchill since we last pulled it), in which case the
    file is re-downloaded and any stale extracted text is cleared.
    """
    pdf_dir = ensure_pdf_dir(data_dir)
    pdf_infos = client.get_pdf_content_urls(
        object_id, domain=domain, collection=collection, include_attachments=include_attachments
    )

    # Prior DB records, keyed by (role, filename), to reuse row IDs and download times.
    prior_by_key = {
        (p.content_role, p.filename): p for p in get_pdfs_for_object(conn, object_id)
    }

    results = []
    for info in pdf_infos:
        filename = info["filename"]
        role = info.get("role", "primary")
        local_path = str(pdf_dir / safe_filename(f"{object_id}_{filename}"))
        prior = prior_by_key.get((role, filename))

        changed = _changed_since_download(last_modified, prior.downloaded_at if prior else None)
        if force or not Path(local_path).exists() or changed:
            client.download_file(info["url"], local_path)
            downloaded_at = datetime.now(timezone.utc).isoformat()
            # Content changed on disk — drop stale extracted text so it re-extracts.
            extracted_text = None if changed else (prior.extracted_text if prior else None)
            extracted_at = None if changed else (prior.extracted_at if prior else None)
        else:
            log.info("  Skipping existing %s", local_path)
            downloaded_at = prior.downloaded_at if prior else None
            extracted_text = prior.extracted_text if prior else None
            extracted_at = prior.extracted_at if prior else None

        pdf = PDFContent(
            object_id=object_id,
            content_role=role,
            filename=filename,
            local_path=local_path,
            download_url=info["url"],
            downloaded_at=downloaded_at,
            extracted_text=extracted_text,
            extracted_at=extracted_at,
            id=prior.id if prior else None,
        )
        pdf.id = upsert_pdf(conn, pdf)
        results.append(pdf)
    conn.commit()
    return results


def extract_text_from_pdf(pdf: PDFContent) -> str:
    """Extract text from a local PDF file using docling.

    Import is deferred because docling is heavy and loads ML models.
    """
    from docling.document_converter import DocumentConverter

    converter = DocumentConverter()
    result = converter.convert(pdf.local_path)
    return result.document.export_to_markdown()


def extract_and_save(conn, pdf: PDFContent) -> PDFContent:
    """Extract text from a PDF and update the DB record."""
    try:
        pdf.extracted_text = extract_text_from_pdf(pdf)
        pdf.extracted_at = datetime.now(timezone.utc).isoformat()
        upsert_pdf(conn, pdf)
        conn.commit()
    except Exception:
        log.exception("Failed to extract text from %s", pdf.local_path)
    return pdf
