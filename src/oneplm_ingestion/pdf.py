"""PDF download and text extraction via docling."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

from oneplm_ingestion.api import WindchillClient
from oneplm_ingestion.db import upsert_pdf
from oneplm_ingestion.models import PDFContent

log = logging.getLogger(__name__)


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
) -> list[PDFContent]:
    """Fetch PDF content URLs and store metadata in DB without downloading files."""
    pdf_infos = client.get_pdf_content_urls(object_id, domain=domain, collection=collection)

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
) -> list[PDFContent]:
    """Download all PDFs for an object, save to disk, record in DB."""
    pdf_dir = ensure_pdf_dir(data_dir)
    pdf_infos = client.get_pdf_content_urls(object_id, domain=domain, collection=collection)

    results = []
    for info in pdf_infos:
        filename = info["filename"]
        local_path = str(pdf_dir / f"{object_id}_{filename}")
        client.download_file(info["url"], local_path)
        pdf = PDFContent(
            object_id=object_id,
            content_role=info.get("role", "primary"),
            filename=filename,
            local_path=local_path,
            download_url=info["url"],
            downloaded_at=datetime.now(timezone.utc).isoformat(),
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
