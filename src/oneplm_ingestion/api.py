"""Windchill HTTP client — makes requests to the PTC Windchill OData API.

API structure (from specs):
  - Document Management: /Windchill/servlet/odata/v6/DocMgmt/
  - Product Management:  /Windchill/servlet/odata/v6/ProdMgmt/
  - Common:              /Windchill/servlet/odata/v4/PTC/

Set ONEPLM_BASE_URL to the root, e.g. https://windchill.company.com/Windchill/servlet/odata
"""

from __future__ import annotations

import logging
import os
import time

import requests
from requests.auth import HTTPBasicAuth

from oneplm_ingestion.auth import get_basic_auth

log = logging.getLogger(__name__)

# Domain base paths (relative to ONEPLM_BASE_URL)
DOCMGMT = "v6/DocMgmt"
PRODMGMT = "v6/ProdMgmt"
DATAADMIN = "v6/DataAdmin"
COMMON = "v4/PTC"


class WindchillClient:
    """HTTP client for the Windchill OData API."""

    def __init__(
        self,
        base_url: str | None = None,
        auth: HTTPBasicAuth | None = None,
        dry_run: bool = False,
    ):
        self.base_url = (base_url or os.environ.get("ONEPLM_BASE_URL", "")).rstrip("/")
        if not self.base_url:
            raise ValueError("ONEPLM_BASE_URL env variable not set and no base_url provided")
        self.dry_run = dry_run
        self.auth = auth or get_basic_auth()
        self.session = requests.Session()
        self.session.auth = self.auth
        self.session.headers.update({"Accept": "application/json"})
        # VPN is slow — generous timeouts (connect=30s, read=120s)
        self.timeout = (30, 120)

    def _log_request(self, method: str, url: str, params: dict | None) -> None:
        """Log an outgoing request at INFO level."""
        if params:
            param_str = "&".join(f"{k}={v}" for k, v in params.items())
            log.info("%s %s?%s", method, url, param_str)
        else:
            log.info("%s %s", method, url)

    def get(self, endpoint: str, params: dict | None = None) -> dict:
        """GET request, return parsed JSON."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        self._log_request("GET", url, params)
        if self.dry_run:
            log.info("  [dry-run] skipping request")
            return {}
        t0 = time.monotonic()
        resp = self.session.get(url, params=params, timeout=self.timeout)
        elapsed = time.monotonic() - t0
        log.debug("  -> %s in %.2fs", resp.status_code, elapsed)
        resp.raise_for_status()
        return resp.json()

    def get_collection(self, endpoint: str, params: dict | None = None) -> list[dict]:
        """GET a paginated OData collection, follow @odata.nextLink, return all items."""
        all_items = []
        current_params = dict(params or {})
        page = 1
        while True:
            data = self.get(endpoint, current_params)
            items = data.get("value", [])
            all_items.extend(items)
            next_link = data.get("@odata.nextLink")
            if not next_link:
                break
            page += 1
            log.debug("  fetching page %d (%d items so far)...", page, len(all_items))
            # nextLink can be absolute or relative
            if next_link.startswith("http"):
                endpoint = next_link.replace(self.base_url, "").lstrip("/")
            else:
                endpoint = next_link
            current_params = {}  # params are baked into nextLink
        log.debug("  collection done: %d total items", len(all_items))
        return all_items

    # ------------------------------------------------------------------
    # Object queries
    # ------------------------------------------------------------------

    def get_objects_by_type(self, api_endpoint: str, modified_after: str | None = None) -> list[dict]:
        """Fetch objects of a given type, optionally filtering by LastModified."""
        params = {}
        if modified_after:
            params["$filter"] = f"LastModified gt {modified_after}"
        return self.get_collection(api_endpoint, params)

    def find_by_number(self, api_endpoint: str, number: str) -> list[dict]:
        """Find objects by exact Number match."""
        params = {"$filter": f"Number eq '{number}'"}
        return self.get_collection(api_endpoint, params)

    def get_object(self, api_endpoint: str, object_id: str, expand: str | None = None) -> dict:
        """Get a single object by ID, optionally expanding navigation properties."""
        endpoint = f"{api_endpoint}('{object_id}')"
        params = {}
        if expand:
            params["$expand"] = expand
        return self.get(endpoint, params)

    # ------------------------------------------------------------------
    # Navigation: related objects
    # ------------------------------------------------------------------

    def get_document_context(self, document_id: str) -> dict:
        """Get the container/library for a document."""
        return self.get(f"{DOCMGMT}/Documents('{document_id}')/Context")

    def get_part_context(self, part_id: str) -> dict:
        """Get the container/library for a part."""
        return self.get(f"{PRODMGMT}/Parts('{part_id}')/Context")

    def get_doc_usage_links(self, document_id: str) -> list[dict]:
        """Get usage links from a document (related objects)."""
        return self.get_collection(f"{DOCMGMT}/Documents('{document_id}')/DocUsageLinks")

    def get_part_described_by(self, part_id: str) -> list[dict]:
        """Get PartDescribeLinks — documents that describe this part."""
        return self.get_collection(f"{PRODMGMT}/Parts('{part_id}')/DescribedBy")

    def get_described_by_document(self, part_id: str, link_id: str) -> dict:
        """Follow a PartDescribeLink to get the actual document."""
        return self.get(
            f"{PRODMGMT}/Parts('{part_id}')/DescribedBy('{link_id}')/DescribedBy"
        )

    def get_part_doc_associations(self, part_id: str) -> list[dict]:
        """Get PartDocAssociation links for a part."""
        return self.get_collection(f"{PRODMGMT}/Parts('{part_id}')/PartDocAssociations")

    # ------------------------------------------------------------------
    # Content / PDF
    # ------------------------------------------------------------------

    def get_attachments(self, domain: str, collection: str, object_id: str) -> list[dict]:
        """Get all attachments (ContentItem) for an object.

        Args:
            domain: e.g. "v6/DocMgmt"
            collection: e.g. "Documents" or "Parts"
            object_id: the object's ID

        Returns list of ContentItem dicts with FileName, MimeType, Content.URL, etc.
        """
        return self.get_collection(f"{domain}/{collection}('{object_id}')/Attachments")

    def get_primary_content(self, document_id: str) -> dict | None:
        """Get the primary content item for a document."""
        try:
            return self.get(f"{DOCMGMT}/Documents('{document_id}')/PrimaryContent")
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

    def get_pdf_content_urls(self, object_id: str, domain: str = DOCMGMT, collection: str = "Documents") -> list[dict]:
        """Get download info for content attached to an object.

        Returns list of dicts: {"url": ..., "filename": ..., "role": "primary"|"attachment"}
        """
        results = []

        # Try primary content first (documents only)
        if collection == "Documents":
            primary = self.get_primary_content(object_id)
            if primary and primary.get("Content", {}).get("URL"):
                results.append({
                    "url": primary["Content"]["URL"],
                    "filename": primary.get("FileName", "primary_content.pdf"),
                    "role": "primary",
                })

        # Get all attachments
        attachments = self.get_attachments(domain, collection, object_id)
        for att in attachments:
            url = att.get("Content", {}).get("URL")
            if url:
                results.append({
                    "url": url,
                    "filename": att.get("FileName", f"attachment_{att.get('ID', 'unknown')}"),
                    "role": "attachment",
                })

        return results

    # ------------------------------------------------------------------
    # Folders
    # ------------------------------------------------------------------

    def get_folders(self, container_id: str) -> list[dict]:
        """List all folders in a container with the full hierarchy expanded in one call."""
        return self.get_collection(
            f"{DATAADMIN}/Containers('{container_id}')/Folders",
            params={"$expand": "Folders($levels=max)"},
        )

    def get_folder_contents(self, container_id: str, folder_path: list[str]) -> list[dict]:
        """List contents (documents/parts) of a folder using the full ancestor chain.

        folder_path is the ordered list of folder IDs from cabinet to leaf, e.g.:
          ["OR:wt.folder.Cabinet:11111", "OR:wt.folder.SubFolder:22222"]
        """
        chain = "".join(f"/Folders('{fid}')" for fid in folder_path)
        return self.get_collection(
            f"{DATAADMIN}/Containers('{container_id}'){chain}/FolderContents"
        )

    def download_file(self, url: str, dest_path: str) -> str:
        """Download a file to dest_path. Returns dest_path."""
        self._log_request("GET (download)", url, None)
        log.info("  -> saving to %s", dest_path)
        if self.dry_run:
            log.info("  [dry-run] skipping download")
            return dest_path
        resp = self.session.get(url, stream=True, timeout=self.timeout)
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        return dest_path
