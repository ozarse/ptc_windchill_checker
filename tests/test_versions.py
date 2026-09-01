"""Tests for version-history storage and the previous-version concept check."""

from __future__ import annotations

from ptc_syncer_ingestion.db import (
    get_connection,
    get_versions_for_object,
    init_db,
    object_has_versions,
    save_versions,
    upsert_object,
)
from ptc_syncer_ingestion.models import WindchillObject
from ptc_syncer_ingestion.versions import check_previous_versions


def _obj(oid, number):
    return WindchillObject(
        id=oid, type_name="IFU PDP", windchill_type="PTC.ProdMgmt.ProductDefinitionPart",
        number=number, name=number, state="PRODUCTIONRELEASED", revision="AB",
        last_modified="2026-01-01T00:00:00Z", attributes={"ID": oid, "Number": number},
        synced_at="x",
    )


def _version(state_value, state_display, latest, version="AA.1", revision="AA"):
    return {
        "ID": f"VR:{version}", "Number": "50-0060-EN", "Version": version, "Revision": revision,
        "Latest": latest, "CreatedOn": "2025-01-01T00:00:00Z",
        "State": {"Value": state_value, "Display": state_display},
    }


def test_save_and_load_versions(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    save_versions(conn, "P1", [
        _version("PRODUCTIONRELEASED", "Production Released", True, "AB.1", "AB"),
        _version("CONCEPT", "Concept", False, "AA.1", "AA"),
    ], "now")
    conn.commit()

    assert object_has_versions(conn, "P1") is True
    rows = get_versions_for_object(conn, "P1")
    assert len(rows) == 2
    assert rows[0]["is_latest"] is True  # latest ordered first
    conn.close()


def test_previous_version_in_concept_fails(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    save_versions(conn, "P1", [
        _version("PRODUCTIONRELEASED", "Production Released", True, "AB.1", "AB"),
        _version("CONCEPT", "Concept", False, "AA.1", "AA"),  # previous, in concept -> fail
    ], "now")
    conn.commit()

    results = check_previous_versions(conn)
    assert len(results) == 1
    assert results[0].passed is False
    assert "concept" in results[0].message.lower()
    conn.close()


def test_previous_versions_clean_passes(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    save_versions(conn, "P1", [
        _version("PRODUCTIONRELEASED", "Production Released", True, "AB.1", "AB"),
        _version("DESIGNRELEASED", "Design Released", False, "AA.1", "AA"),
    ], "now")
    conn.commit()

    results = check_previous_versions(conn)
    assert len(results) == 1 and results[0].passed is True
    conn.close()


def test_only_latest_version_is_skipped(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    save_versions(conn, "P1", [
        _version("CONCEPT", "Concept", True, "AA.1", "AA"),  # only version, and it's latest
    ], "now")
    conn.commit()

    # No previous versions -> check does not apply -> no result rows.
    assert check_previous_versions(conn) == []
    conn.close()


def test_object_without_versions_skipped(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    conn.commit()
    assert check_previous_versions(conn) == []
    conn.close()


def test_registered_via_engine(tmp_path):
    from ptc_syncer_ingestion.checks import run_all_checks

    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    upsert_object(conn, _obj("P1", "50-0060-EN"))
    save_versions(conn, "P1", [
        _version("PRODUCTIONRELEASED", "Production Released", True, "AB.1", "AB"),
        _version("CONCEPT", "Concept", False, "AA.1", "AA"),
    ], "now")
    conn.commit()

    cfg = tmp_path / "checks.json"
    cfg.write_text('[{"name": "Versions", "kind": "python", '
                   '"function": "previous_versions_not_in_concept"}]')
    results = run_all_checks(conn, cfg)["Versions"]
    assert results and results[0].passed is False
    conn.close()
