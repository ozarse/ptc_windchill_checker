"""Pandas DataFrame helpers for exploring oneplm data."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd

DEFAULT_DB_PATH = Path("data/oneplm.db")


def _import_pandas():
    """Import pandas with a helpful error message if not installed."""
    try:
        import pandas as pd
        return pd
    except ImportError:
        raise ImportError(
            "pandas is required for DataFrame helpers. "
            "Install with: pip install oneplm_ingestion[notebook]"
        ) from None


def get_connection(db_path: str | Path | None = None) -> sqlite3.Connection:
    """Get a read-only SQLite connection."""
    path = str(db_path or DEFAULT_DB_PATH)
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def load_objects(
    db_path: str | Path | None = None,
    type_name: str | None = None,
    expand_attributes: bool = True,
) -> pd.DataFrame:
    """Load objects table into a DataFrame.

    Args:
        db_path: Path to SQLite database. Defaults to data/oneplm.db.
        type_name: If provided, filter to this type only.
        expand_attributes: If True, expand the JSON attributes into columns.

    Returns:
        DataFrame with one row per object.
    """
    pd = _import_pandas()

    conn = get_connection(db_path)
    query = "SELECT * FROM objects"
    params: list = []
    if type_name:
        query += " WHERE type_name = ?"
        params.append(type_name)
    query += " ORDER BY type_name, number"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if expand_attributes and "attributes_json" in df.columns:
        attrs = df["attributes_json"].apply(json.loads).apply(pd.Series)
        # Prefix expanded columns to avoid collision with base columns
        attrs.columns = [f"attr_{c}" if c in df.columns else c for c in attrs.columns]
        df = pd.concat([df.drop(columns=["attributes_json"]), attrs], axis=1)

    return df


def load_check_results(
    db_path: str | Path | None = None,
    check_name: str | None = None,
    failed_only: bool = False,
) -> pd.DataFrame:
    """Load check_results table into a DataFrame."""
    pd = _import_pandas()

    conn = get_connection(db_path)
    query = "SELECT * FROM check_results WHERE 1=1"
    params: list = []
    if check_name:
        query += " AND check_name = ?"
        params.append(check_name)
    if failed_only:
        query += " AND passed = 0"
    query += " ORDER BY check_name, source_object_id"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    df["passed"] = df["passed"].astype(bool)
    return df


def load_pdfs(db_path: str | Path | None = None) -> pd.DataFrame:
    """Load pdfs table into a DataFrame."""
    pd = _import_pandas()

    conn = get_connection(db_path)
    df = pd.read_sql_query("SELECT * FROM pdfs ORDER BY object_id", conn)
    conn.close()
    return df


def load_sync_log(db_path: str | Path | None = None) -> pd.DataFrame:
    """Load sync_log table into a DataFrame."""
    pd = _import_pandas()

    conn = get_connection(db_path)
    df = pd.read_sql_query("SELECT * FROM sync_log ORDER BY last_sync_at DESC", conn)
    conn.close()
    return df
