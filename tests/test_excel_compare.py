"""Tests for the excel_compare check (published-products export vs Windchill)."""

from __future__ import annotations

import json

import pytest
from openpyxl import Workbook

from ptc_syncer_ingestion.checks import load_check_configs, run_all_checks
from ptc_syncer_ingestion.db import get_connection, init_db, save_relationships, upsert_object
from ptc_syncer_ingestion.models import WindchillObject

# The publishing website's export headers, as provided.
HEADERS = [
    "Key code", "Publish date", "Product type", "Product groups",
    "ProductDescriptionsorName", "Reforcatalognumber", "Serialnumber",
    "deviceifukeycode", "ProductFamily", "Public URL",
]


def _obj(oid, type_name, number, name):
    return WindchillObject(
        id=oid, type_name=type_name, windchill_type="PTC.X", number=number, name=name,
        state=None, revision=None, last_modified="2025-01-01T00:00:00Z",
        attributes={"ID": oid, "Number": number, "Name": name},
        synced_at="2025-01-01T00:00:00Z",
    )


@pytest.fixture
def conn(tmp_path):
    c = get_connection(tmp_path / "test.db")
    init_db(c)
    upsert_object(c, _obj("CFG1", "Config PDP", "C-1", "Widget IFU Options"))
    upsert_object(c, _obj("IFU-EN", "IFU PDP", "50-0060-EN", "Widget IFU EN"))
    upsert_object(c, _obj("IFU-DE", "IFU PDP", "50-0060-DE", "Widget IFU DE"))
    # Config PDP uses two IFU PDPs; products REF-100 and REF-200 use the Config PDP.
    # The products themselves are NOT synced as objects — only the relationship rows exist.
    save_relationships(c, "CFG1", "uses",
                       [{"ID": "IFU-EN", "Number": "50-0060-EN"},
                        {"ID": "IFU-DE", "Number": "50-0060-DE"}], "now")
    save_relationships(c, "CFG1", "used_by",
                       [{"ID": "PROD-100", "Number": "REF-100"},
                        {"ID": "PROD-200", "Number": "REF-200"}], "now")
    c.commit()
    yield c
    c.close()


def _write_export(path, rows):
    """rows: list of (reforcatalognumber, product_groups) tuples."""
    wb = Workbook()
    ws = wb.active
    ws.append(HEADERS)
    for ref, groups in rows:
        ws.append(["K", "2026-01-01", "Device", groups, "Some product", ref, "", "D-1", "Fam", "http://x"])
    wb.save(path)


def _check_entry(tmp_path, file_name="export.xlsx", **overrides):
    entry = {
        "name": "published_products_match",
        "kind": "excel_compare",
        "file": str(tmp_path / file_name),
        "product_column": "Reforcatalognumber",
        "ifu_column": "Product groups",
        "ifu_separator": "|",
    }
    entry.update(overrides)
    cfg = tmp_path / "checks.json"
    cfg.write_text(json.dumps([entry]))
    return cfg


def _run(conn, cfg):
    return run_all_checks(conn, cfg)["published_products_match"]


def test_matching_export_passes(conn, tmp_path):
    _write_export(tmp_path / "export.xlsx", [
        ("REF-100", "50-0060-EN|50-0060-DE"),
        ("REF-200", "50-0060-DE|50-0060-EN"),
    ])
    results = _run(conn, _check_entry(tmp_path))
    assert len(results) == 2
    assert all(r.passed for r in results)
    assert all(r.status == "pass" for r in results)


def test_missing_product_fails(conn, tmp_path):
    # REF-200 is absent from the export.
    _write_export(tmp_path / "export.xlsx", [("REF-100", "50-0060-EN|50-0060-DE")])
    results = _run(conn, _check_entry(tmp_path))
    by_product = {r.message.split()[2]: r for r in results if not r.passed}
    assert "REF-200" in by_product
    missing = by_product["REF-200"]
    assert missing.target_object_id == "MISSING"
    assert "C-1" in missing.message  # names the Config PDP it came from


def test_ifu_mismatch_fails_both_directions(conn, tmp_path):
    # REF-100: missing DE (unpublished) and has an extra FR (unknown to Windchill).
    _write_export(tmp_path / "export.xlsx", [
        ("REF-100", "50-0060-EN|50-0060-FR"),
        ("REF-200", "50-0060-EN|50-0060-DE"),
    ])
    results = _run(conn, _check_entry(tmp_path))
    fails = [r for r in results if not r.passed]
    assert len(fails) == 1
    assert "not published: 50-0060-DE" in fails[0].message
    assert "not in Windchill: 50-0060-FR" in fails[0].message
    assert fails[0].source_value == "50-0060-DE|50-0060-EN"
    assert fails[0].target_value == "50-0060-EN|50-0060-FR"


def test_orphan_published_product_fails(conn, tmp_path):
    _write_export(tmp_path / "export.xlsx", [
        ("REF-100", "50-0060-EN|50-0060-DE"),
        ("REF-200", "50-0060-EN|50-0060-DE"),
        ("REF-999", "50-0060-EN"),
    ])
    results = _run(conn, _check_entry(tmp_path))
    fails = [r for r in results if not r.passed]
    assert len(fails) == 1
    assert fails[0].source_object_id == "EXPORT"
    assert fails[0].target_object_id == "REF-999"
    assert "not used by any Config PDP" in fails[0].message


def test_duplicate_export_rows_union_ifus(conn, tmp_path):
    # Same product split over two rows — IFU sets must be merged before comparing.
    _write_export(tmp_path / "export.xlsx", [
        ("REF-100", "50-0060-EN"),
        ("REF-100", "50-0060-DE"),
        ("REF-200", "50-0060-EN|50-0060-DE"),
    ])
    results = _run(conn, _check_entry(tmp_path))
    assert all(r.passed for r in results)


def test_missing_file_skips(conn, tmp_path):
    results = _run(conn, _check_entry(tmp_path, file_name="nope.xlsx"))
    assert len(results) == 1
    assert results[0].status == "skip"
    assert "export file not found" in results[0].message


def test_no_relationships_skips(tmp_path):
    c = get_connection(tmp_path / "empty.db")
    init_db(c)
    _write_export(tmp_path / "export.xlsx", [("REF-100", "50-0060-EN")])
    results = _run(c, _check_entry(tmp_path))
    assert len(results) == 1
    assert results[0].status == "skip"
    assert "sync relationships" in results[0].message
    c.close()


def test_wrong_column_name_raises(conn, tmp_path):
    _write_export(tmp_path / "export.xlsx", [("REF-100", "50-0060-EN")])
    cfg = _check_entry(tmp_path, product_column="Catalog Number")
    with pytest.raises(ValueError, match="Catalog Number"):
        _run(conn, cfg)


def test_missing_required_fields_rejected_at_load(tmp_path):
    cfg = tmp_path / "checks.json"
    cfg.write_text(json.dumps([{"name": "bad", "kind": "excel_compare"}]))
    with pytest.raises(ValueError) as exc:
        load_check_configs(cfg)
    for field in ("file", "product_column", "ifu_column"):
        assert f"'{field}' is required" in str(exc.value)
