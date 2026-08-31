"""Tests for the record validation engine (attribute / relationship / python checks)."""

from __future__ import annotations

import json

import pytest

from oneplm_ingestion.checks import load_check_configs, run_all_checks
from oneplm_ingestion.db import get_connection, init_db, save_relationships, upsert_object
from oneplm_ingestion.models import WindchillObject


def _obj(oid, type_name, number, name, attrs=None):
    base = {"ID": oid, "Number": number, "Name": name}
    base.update(attrs or {})
    return WindchillObject(
        id=oid,
        type_name=type_name,
        windchill_type="PTC.X",
        number=number,
        name=name,
        state=base.get("State", {}).get("Value") if isinstance(base.get("State"), dict) else None,
        revision=None,
        last_modified="2025-01-01T00:00:00Z",
        attributes=base,
        synced_at="2025-01-01T00:00:00Z",
    )


@pytest.fixture
def conn(tmp_path):
    c = get_connection(tmp_path / "test.db")
    init_db(c)
    # IFU PDP and its IFU Drawing share Number; Name differs to trigger a failure.
    pdp = _obj("PDP1", "IFU PDP", "12345", "Widget IFU", {"ConfigurableModule": {"Value": "No"}})
    drawing_ok = _obj("DRW1", "IFU Drawing", "12345", "Widget IFU", {"DocTypeName": "IFU Drawing"})
    drawing_bad = _obj("DRW2", "IFU Drawing", "67890", "Mismatch", {"DocTypeName": "IFU Drawing"})
    config = _obj("CFG1", "Config PDP", "C-1", "", {"ConfigurableModule": {"Value": "Yes"}})
    for o in (pdp, drawing_ok, drawing_bad, config):
        upsert_object(c, o)

    # IFU PDP described_by the good drawing (resolved relationship: target = drawing id/number)
    save_relationships(c, "PDP1", "described_by",
                       [{"ID": "DRW1", "Number": "12345", "Name": "Widget IFU"}], "now")
    # IFU PDP used_by the config part; config uses the IFU PDP
    save_relationships(c, "PDP1", "used_by", [{"ID": "CFG1", "Number": "C-1"}], "now")
    save_relationships(c, "CFG1", "uses", [{"ID": "PDP1", "Number": "12345"}], "now")
    c.commit()
    yield c
    c.close()


def _write_checks(tmp_path, data):
    p = tmp_path / "checks.json"
    p.write_text(json.dumps(data))
    return p


def test_attribute_check_pass_and_fail(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "config_required", "kind": "attribute", "type": "Config PDP",
        "assertions": [{"attr": "Name", "operator": "not_empty"}],
    }])
    results = run_all_checks(conn, cfg)["config_required"]
    assert len(results) == 1
    assert results[0].passed is False  # Config PDP has empty Name


def test_attribute_when_precondition_skips(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "released_only", "kind": "attribute", "type": "Config PDP",
        "assertions": [{
            "attr": "ApprovalDate", "operator": "not_empty",
            "when": {"attr": "State.Value", "operator": "equals", "value": "Released"},
        }],
    }])
    results = run_all_checks(conn, cfg)["released_only"]
    # Config PDP is not Released -> precondition not met -> passes as SKIP
    assert results[0].passed is True
    assert results[0].status == "skip"
    assert "SKIP" in results[0].message


def test_relationship_describes_inverse(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "drawing_matches_pdp", "kind": "relationship",
        "type": "IFU Drawing", "related_type": "IFU PDP", "via": "describes",
        "on_missing": "fail",
        "comparisons": [
            {"source_attr": "Number", "target_attr": "Number", "operator": "equals"},
            {"source_attr": "Name", "target_attr": "Name", "operator": "equals"},
        ],
    }])
    results = run_all_checks(conn, cfg)["drawing_matches_pdp"]
    by_target = {(r.source_object_id, r.source_attr): r for r in results}
    # Good drawing (DRW1) matches PDP on both Number and Name
    assert by_target[("DRW1", "Number")].passed is True
    assert by_target[("DRW1", "Name")].passed is True
    # Bad drawing (DRW2) has no described_by relationship -> on_missing=fail
    assert by_target[("DRW2", "Number")].passed is False
    assert by_target[("DRW2", "Number")].target_object_id == "MISSING"


def test_relationship_forward_used_by(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "pdp_used_by_config", "kind": "relationship",
        "type": "IFU PDP", "related_type": "Config PDP", "via": "used_by",
        "comparisons": [{"source_attr": "Number", "target_attr": "Number", "operator": "not_empty"}],
    }])
    results = run_all_checks(conn, cfg)["pdp_used_by_config"]
    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].target_object_id == "CFG1"


def test_python_check_dispatch(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "PDF Filenames", "kind": "python", "function": "ifu_drawing_pdf_filename",
    }])
    # No PDFs in DB -> function returns no results, but must dispatch without error.
    results = run_all_checks(conn, cfg)
    assert "PDF Filenames" in results


def test_unknown_via_rejected_at_load(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "bad", "kind": "relationship",
        "type": "IFU Drawing", "related_type": "IFU PDP", "via": "bogus",
        "comparisons": [{"source_attr": "Number", "target_attr": "Number", "operator": "equals"}],
    }])
    with pytest.raises(ValueError, match="unknown 'via'"):
        run_all_checks(conn, cfg)


def test_unknown_operator_rejected_at_load(tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "typo", "kind": "attribute", "type": "Config PDP",
        "assertions": [{"attr": "Name", "operator": "equalz", "value": "x"}],
    }])
    with pytest.raises(ValueError, match="unknown operator 'equalz'"):
        load_check_configs(cfg)


def test_binary_operator_without_value_rejected_at_load(tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "no_value", "kind": "attribute", "type": "Config PDP",
        "assertions": [{"attr": "Name", "operator": "equals"}],
    }])
    with pytest.raises(ValueError, match="requires a 'value'"):
        load_check_configs(cfg)


def test_duplicate_check_names_rejected_at_load(tmp_path):
    entry = {"name": "dup", "kind": "attribute", "type": "Config PDP",
             "assertions": [{"attr": "Number", "operator": "not_empty"}]}
    cfg = _write_checks(tmp_path, [entry, dict(entry)])
    with pytest.raises(ValueError, match="duplicate check name 'dup'"):
        load_check_configs(cfg)


def test_relationship_without_comparisons_or_counts_rejected(tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "empty_rel", "kind": "relationship",
        "type": "IFU PDP", "related_type": "Config PDP", "via": "used_by",
    }])
    with pytest.raises(ValueError, match="needs 'comparisons' and/or 'min_count'"):
        load_check_configs(cfg)


def test_relationship_min_count_pass(conn, tmp_path):
    cfg = _write_checks(tmp_path, [{
        "name": "pdp_used", "kind": "relationship",
        "type": "IFU PDP", "related_type": "Config PDP", "via": "used_by",
        "min_count": 1,
    }])
    results = run_all_checks(conn, cfg)["pdp_used"]
    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].source_value == "1"
    assert results[0].target_object_id == "CFG1"


def test_relationship_min_count_fail_when_no_links(conn, tmp_path):
    # The Config PDP has no used_by rows at all -> count 0 -> fail.
    cfg = _write_checks(tmp_path, [{
        "name": "cfg_used", "kind": "relationship",
        "type": "Config PDP", "related_type": "Config PDP", "via": "used_by",
        "min_count": 1,
    }])
    results = run_all_checks(conn, cfg)["cfg_used"]
    assert len(results) == 1
    assert results[0].passed is False
    assert results[0].target_object_id == "MISSING"
    assert "expected at least 1" in results[0].message


def test_relationship_target_value_literal(conn, tmp_path):
    # Assert directly on the related record's attribute against a literal.
    cfg = _write_checks(tmp_path, [{
        "name": "uses_number", "kind": "relationship",
        "type": "Config PDP", "related_type": "IFU PDP", "via": "uses",
        "comparisons": [{"target_attr": "Number", "operator": "equals", "target_value": "12345"}],
    }])
    results = run_all_checks(conn, cfg)["uses_number"]
    assert len(results) == 1
    assert results[0].passed is True

    cfg_bad = _write_checks(tmp_path, [{
        "name": "uses_number", "kind": "relationship",
        "type": "Config PDP", "related_type": "IFU PDP", "via": "uses",
        "comparisons": [{"target_attr": "Number", "operator": "equals", "target_value": "99999"}],
    }])
    results = run_all_checks(conn, cfg_bad)["uses_number"]
    assert results[0].passed is False


def test_relationship_unsynced_target_reported(conn, tmp_path):
    # Replace the IFU PDP's used_by link with one pointing at an object that
    # was never synced: the failure must name the data gap, not "no related".
    save_relationships(conn, "PDP1", "used_by", [{"ID": "GHOST", "Number": "G-1"}], "now")
    conn.commit()
    cfg = _write_checks(tmp_path, [{
        "name": "pdp_used", "kind": "relationship",
        "type": "IFU PDP", "related_type": "Config PDP", "via": "used_by",
        "on_missing": "fail",
        "comparisons": [{"source_attr": "Number", "target_attr": "Number", "operator": "equals"}],
    }])
    results = run_all_checks(conn, cfg)["pdp_used"]
    assert len(results) == 1
    assert results[0].passed is False
    assert "not in local DB" in results[0].message
    assert "G-1" in results[0].message


def test_shipped_attribute_checks_pass_on_real_payload_shape(tmp_path):
    """Config/IFU PDP attribute checks pass against the real enum payload shape.

    Enum attributes are {"Value": <code>, "Display": <label>}; the checks compare
    on .Display, so a Config PDP has ConfigurableModule.Display == "Yes".
    """
    from pathlib import Path

    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    config = WindchillObject(
        id="CFG", type_name="Config PDP", windchill_type="PTC.ProdMgmt.ProductDefinitionPart",
        number="CMF_C00003", name="MEDPOR IFU Options", state="PRODUCTIONRELEASED",
        revision="AA", last_modified="2024-03-16T20:43:46Z",
        attributes={"Number": "CMF_C00003", "Name": "MEDPOR IFU Options",
                    "ConfigurableModule": {"Value": "dynamic", "Display": "Yes"}},
        synced_at="x",
    )
    ifu = WindchillObject(
        id="IFU", type_name="IFU PDP", windchill_type="PTC.ProdMgmt.ProductDefinitionPart",
        number="50-0060-EN", name="TMJ IFU EN", state="PRODUCTIONRELEASED",
        revision="AB", last_modified="2026-04-14T12:28:10Z",
        attributes={"Number": "50-0060-EN", "Name": "TMJ IFU EN",
                    "ConfigurableModule": {"Value": "standard", "Display": "No"}},
        synced_at="x",
    )
    upsert_object(conn, config)
    upsert_object(conn, ifu)
    conn.commit()

    results = run_all_checks(conn, Path("config/checks.json"),
                             check_names=["config_pdp_attributes", "ifu_pdp_attributes"])
    all_rows = [r for rows in results.values() for r in rows]
    assert all_rows and all(r.passed for r in all_rows), \
        [r.message for r in all_rows if not r.passed]
    conn.close()


def test_shipped_checks_config_parses():
    from pathlib import Path
    checks = load_check_configs(Path("config/checks.json"))
    assert len(checks) >= 1
    kinds = {type(c).__name__ for c in checks}
    assert "AttributeCheck" in kinds
    assert "RelationshipCheck" in kinds
    assert "PythonCheck" in kinds


def test_clear_check_results(conn, tmp_path):
    from oneplm_ingestion.db import clear_check_results, get_check_results

    cfg = _write_checks(tmp_path, [{
        "name": "plain", "kind": "attribute", "type": "Config PDP",
        "assertions": [{"attr": "Number", "operator": "not_empty"}],
    }])
    run_all_checks(conn, cfg)
    assert get_check_results(conn)  # rows exist
    removed = clear_check_results(conn)
    conn.commit()
    assert removed >= 1
    assert get_check_results(conn) == []


def test_skip_pdf_excludes_marked_checks(conn, tmp_path):
    cfg = _write_checks(tmp_path, [
        {"name": "plain", "kind": "attribute", "type": "Config PDP",
         "assertions": [{"attr": "Number", "operator": "not_empty"}]},
        {"name": "pdf_one", "kind": "attribute", "type": "Config PDP", "requires_pdf": True,
         "assertions": [{"attr": "Number", "operator": "not_empty"}]},
    ])
    with_pdf = run_all_checks(conn, cfg)
    assert set(with_pdf) == {"plain", "pdf_one"}

    without_pdf = run_all_checks(conn, cfg, skip_pdf=True)
    assert set(without_pdf) == {"plain"}
