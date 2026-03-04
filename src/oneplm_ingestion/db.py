"""SQLite database layer — schema, connection, and CRUD helpers."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from oneplm_ingestion.models import CheckResult, Folder, PDFContent, WindchillObject

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS objects (
    id              TEXT PRIMARY KEY,
    type_name       TEXT NOT NULL,
    windchill_type  TEXT NOT NULL,
    number          TEXT,
    name            TEXT,
    state           TEXT,
    revision        TEXT,
    last_modified   TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    synced_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_objects_type ON objects(type_name);
CREATE INDEX IF NOT EXISTS idx_objects_number ON objects(number);
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(last_modified);

CREATE TABLE IF NOT EXISTS pdfs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    object_id       TEXT NOT NULL REFERENCES objects(id),
    content_role    TEXT NOT NULL,
    filename        TEXT NOT NULL,
    local_path      TEXT,
    extracted_text  TEXT,
    download_url    TEXT,
    downloaded_at   TEXT,
    extracted_at    TEXT
);

CREATE INDEX IF NOT EXISTS idx_pdfs_object_id ON pdfs(object_id);

CREATE TABLE IF NOT EXISTS check_results (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    check_name       TEXT NOT NULL,
    source_object_id TEXT NOT NULL,
    target_object_id TEXT NOT NULL,
    source_attr      TEXT NOT NULL,
    target_attr      TEXT NOT NULL,
    source_value     TEXT,
    target_value     TEXT,
    passed           INTEGER NOT NULL,
    message          TEXT,
    checked_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_check_results_check_name ON check_results(check_name);
CREATE INDEX IF NOT EXISTS idx_check_results_passed ON check_results(passed);

CREATE TABLE IF NOT EXISTS sync_log (
    type_name       TEXT PRIMARY KEY,
    last_sync_at    TEXT NOT NULL,
    objects_fetched  INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS folders (
    id               TEXT PRIMARY KEY,
    container_id     TEXT NOT NULL,
    parent_folder_id TEXT REFERENCES folders(id),
    name             TEXT NOT NULL,
    location         TEXT,
    description      TEXT,
    created_on       TEXT,
    last_modified    TEXT,
    synced_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_folders_container ON folders(container_id);
CREATE INDEX IF NOT EXISTS idx_folders_parent    ON folders(parent_folder_id);

CREATE TABLE IF NOT EXISTS relationships (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id       TEXT NOT NULL,
    target_id       TEXT,
    rel_type        TEXT NOT NULL,
    attributes_json TEXT NOT NULL,
    synced_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_id);
CREATE INDEX IF NOT EXISTS idx_relationships_type   ON relationships(rel_type, source_id);
"""


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    """Open (or create) the SQLite database."""
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables and indexes if they don't exist."""
    conn.executescript(SCHEMA_SQL)
    _run_migrations(conn)


def _run_migrations(conn: sqlite3.Connection) -> None:
    """Apply incremental schema changes to existing databases."""
    try:
        conn.execute("ALTER TABLE objects ADD COLUMN folder_id TEXT REFERENCES folders(id)")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # column already exists


# --- Objects ---


def upsert_object(conn: sqlite3.Connection, obj: WindchillObject) -> None:
    """Insert or update a Windchill object."""
    conn.execute(
        """INSERT INTO objects (id, type_name, windchill_type, number, name, state,
                                revision, last_modified, attributes_json, synced_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               type_name=excluded.type_name, windchill_type=excluded.windchill_type,
               number=excluded.number, name=excluded.name, state=excluded.state,
               revision=excluded.revision, last_modified=excluded.last_modified,
               attributes_json=excluded.attributes_json, synced_at=excluded.synced_at""",
        (
            obj.id, obj.type_name, obj.windchill_type, obj.number, obj.name,
            obj.state, obj.revision, obj.last_modified,
            json.dumps(obj.attributes), obj.synced_at,
        ),
    )


def get_objects_by_type(conn: sqlite3.Connection, type_name: str) -> list[WindchillObject]:
    """Load all objects of a given type from the DB."""
    rows = conn.execute(
        "SELECT * FROM objects WHERE type_name = ? ORDER BY number", (type_name,)
    ).fetchall()
    return [_row_to_object(row) for row in rows]


def get_all_objects(conn: sqlite3.Connection) -> list[WindchillObject]:
    """Load all objects from the DB."""
    rows = conn.execute("SELECT * FROM objects ORDER BY type_name, number").fetchall()
    return [_row_to_object(row) for row in rows]


def get_object_by_id(conn: sqlite3.Connection, object_id: str) -> WindchillObject | None:
    """Load a single object by ID."""
    row = conn.execute("SELECT * FROM objects WHERE id = ?", (object_id,)).fetchone()
    return _row_to_object(row) if row else None


def _row_to_object(row: sqlite3.Row) -> WindchillObject:
    return WindchillObject(
        id=row["id"],
        type_name=row["type_name"],
        windchill_type=row["windchill_type"],
        number=row["number"],
        name=row["name"],
        state=row["state"],
        revision=row["revision"],
        last_modified=row["last_modified"],
        attributes=json.loads(row["attributes_json"]),
        synced_at=row["synced_at"],
    )


# --- Sync log ---


def get_last_sync(conn: sqlite3.Connection, type_name: str) -> str | None:
    """Return the last sync timestamp for a type, or None if never synced."""
    row = conn.execute(
        "SELECT last_sync_at FROM sync_log WHERE type_name = ?", (type_name,)
    ).fetchone()
    return row["last_sync_at"] if row else None


def update_sync_log(conn: sqlite3.Connection, type_name: str, sync_at: str, count: int) -> None:
    """Record a sync event."""
    conn.execute(
        """INSERT INTO sync_log (type_name, last_sync_at, objects_fetched) VALUES (?, ?, ?)
           ON CONFLICT(type_name) DO UPDATE SET
               last_sync_at=excluded.last_sync_at,
               objects_fetched=excluded.objects_fetched""",
        (type_name, sync_at, count),
    )


# --- PDFs ---


def upsert_pdf(conn: sqlite3.Connection, pdf: PDFContent) -> int:
    """Insert or update a PDF record. Returns the row ID."""
    if pdf.id is not None:
        conn.execute(
            """UPDATE pdfs SET object_id=?, content_role=?, filename=?, local_path=?,
                   extracted_text=?, download_url=?, downloaded_at=?, extracted_at=?
               WHERE id=?""",
            (
                pdf.object_id, pdf.content_role, pdf.filename, pdf.local_path,
                pdf.extracted_text, pdf.download_url, pdf.downloaded_at,
                pdf.extracted_at, pdf.id,
            ),
        )
        return pdf.id

    cursor = conn.execute(
        """INSERT INTO pdfs (object_id, content_role, filename, local_path,
                              extracted_text, download_url, downloaded_at, extracted_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            pdf.object_id, pdf.content_role, pdf.filename, pdf.local_path,
            pdf.extracted_text, pdf.download_url, pdf.downloaded_at,
            pdf.extracted_at,
        ),
    )
    return cursor.lastrowid


def get_pdfs_for_object(conn: sqlite3.Connection, object_id: str) -> list[PDFContent]:
    """Get all PDF records for an object."""
    rows = conn.execute(
        "SELECT * FROM pdfs WHERE object_id = ?", (object_id,)
    ).fetchall()
    return [_row_to_pdf(row) for row in rows]


def get_pdfs_pending_extraction(conn: sqlite3.Connection) -> list[PDFContent]:
    """Get PDFs that have been downloaded but not yet extracted."""
    rows = conn.execute(
        "SELECT * FROM pdfs WHERE local_path IS NOT NULL AND extracted_text IS NULL"
    ).fetchall()
    return [_row_to_pdf(row) for row in rows]


def _row_to_pdf(row: sqlite3.Row) -> PDFContent:
    return PDFContent(
        id=row["id"],
        object_id=row["object_id"],
        content_role=row["content_role"],
        filename=row["filename"],
        local_path=row["local_path"],
        extracted_text=row["extracted_text"],
        download_url=row["download_url"],
        downloaded_at=row["downloaded_at"],
        extracted_at=row["extracted_at"],
    )


# --- Check results ---


def save_check_results(conn: sqlite3.Connection, results: list[CheckResult]) -> None:
    """Bulk insert check results, replacing previous results for the same check_name."""
    if not results:
        return
    check_name = results[0].check_name
    conn.execute("DELETE FROM check_results WHERE check_name = ?", (check_name,))
    conn.executemany(
        """INSERT INTO check_results
           (check_name, source_object_id, target_object_id, source_attr, target_attr,
            source_value, target_value, passed, message, checked_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        [
            (
                r.check_name, r.source_object_id, r.target_object_id,
                r.source_attr, r.target_attr, r.source_value, r.target_value,
                int(r.passed), r.message, r.checked_at,
            )
            for r in results
        ],
    )


# --- Folders ---


def upsert_folder(conn: sqlite3.Connection, folder: Folder) -> None:
    """Insert or update a folder."""
    conn.execute(
        """INSERT INTO folders
               (id, container_id, parent_folder_id, name, location, description,
                created_on, last_modified, synced_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
               container_id=excluded.container_id,
               parent_folder_id=excluded.parent_folder_id,
               name=excluded.name,
               location=excluded.location,
               description=excluded.description,
               created_on=excluded.created_on,
               last_modified=excluded.last_modified,
               synced_at=excluded.synced_at""",
        (
            folder.id, folder.container_id, folder.parent_folder_id,
            folder.name, folder.location, folder.description,
            folder.created_on, folder.last_modified, folder.synced_at,
        ),
    )


def get_folders_by_container(conn: sqlite3.Connection, container_id: str) -> list[Folder]:
    """Load all folders for a container."""
    rows = conn.execute(
        "SELECT * FROM folders WHERE container_id = ? ORDER BY location", (container_id,)
    ).fetchall()
    return [_row_to_folder(row) for row in rows]


def update_object_folder(conn: sqlite3.Connection, object_id: str, folder_id: str) -> None:
    """Set the folder_id on an object row."""
    conn.execute("UPDATE objects SET folder_id = ? WHERE id = ?", (folder_id, object_id))


def _row_to_folder(row: sqlite3.Row) -> Folder:
    return Folder(
        id=row["id"],
        container_id=row["container_id"],
        parent_folder_id=row["parent_folder_id"],
        name=row["name"],
        location=row["location"],
        description=row["description"],
        created_on=row["created_on"],
        last_modified=row["last_modified"],
        synced_at=row["synced_at"],
    )


# --- Relationships ---


def save_relationships(
    conn: sqlite3.Connection, source_id: str, rel_type: str, items: list[dict], synced_at: str
) -> None:
    """Replace all relationships of a given type for an object (delete then insert)."""
    conn.execute(
        "DELETE FROM relationships WHERE source_id = ? AND rel_type = ?",
        (source_id, rel_type),
    )
    conn.executemany(
        """INSERT INTO relationships (source_id, target_id, rel_type, attributes_json, synced_at)
           VALUES (?, ?, ?, ?, ?)""",
        [(source_id, item.get("ID"), rel_type, json.dumps(item), synced_at) for item in items],
    )


def get_relationships_for_object(
    conn: sqlite3.Connection, object_id: str, rel_type: str | None = None
) -> list[dict]:
    """Load relationship records for an object, optionally filtered by rel_type."""
    if rel_type:
        rows = conn.execute(
            "SELECT * FROM relationships WHERE source_id = ? AND rel_type = ? ORDER BY rel_type",
            (object_id, rel_type),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM relationships WHERE source_id = ? ORDER BY rel_type",
            (object_id,),
        ).fetchall()
    return [
        {
            "id": row["id"],
            "source_id": row["source_id"],
            "target_id": row["target_id"],
            "rel_type": row["rel_type"],
            "attributes": json.loads(row["attributes_json"]),
            "synced_at": row["synced_at"],
        }
        for row in rows
    ]


def get_check_results(
    conn: sqlite3.Connection,
    check_name: str | None = None,
    failed_only: bool = False,
) -> list[CheckResult]:
    """Retrieve check results with optional filtering."""
    query = "SELECT * FROM check_results WHERE 1=1"
    params: list = []
    if check_name:
        query += " AND check_name = ?"
        params.append(check_name)
    if failed_only:
        query += " AND passed = 0"
    query += " ORDER BY check_name, source_object_id"
    rows = conn.execute(query, params).fetchall()
    return [
        CheckResult(
            id=row["id"],
            check_name=row["check_name"],
            source_object_id=row["source_object_id"],
            target_object_id=row["target_object_id"],
            source_attr=row["source_attr"],
            target_attr=row["target_attr"],
            source_value=row["source_value"],
            target_value=row["target_value"],
            passed=bool(row["passed"]),
            message=row["message"],
            checked_at=row["checked_at"],
        )
        for row in rows
    ]
