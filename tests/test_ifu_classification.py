"""Tests for the Config PDP eIFU/Hybrid/Print classification check."""

from __future__ import annotations

from oneplm_ingestion import ifu_classification as ifc
from oneplm_ingestion.db import get_connection, init_db, save_relationships, upsert_object
from oneplm_ingestion.ifu_classification import (
    classify,
    classify_config_pdps,
    has_language_suffix,
)
from oneplm_ingestion.models import WindchillObject


def _set_dotted(attrs: dict, dotted_key: str, value) -> None:
    """Store ``value`` at a dot-notation path, creating nested dicts as needed."""
    keys = dotted_key.split(".")
    cur = attrs
    for k in keys[:-1]:
        cur = cur.setdefault(k, {})
    cur[keys[-1]] = value


def _obj(oid, type_name, number, extra_attrs=None):
    attributes = {"ID": oid, "Number": number}
    for dotted, val in (extra_attrs or {}).items():
        _set_dotted(attributes, dotted, val)
    return WindchillObject(
        id=oid, type_name=type_name, windchill_type="PTC.X", number=number,
        name=number, state=None, revision=None, last_modified="2025-01-01T00:00:00Z",
        attributes=attributes, synced_at="2025-01-01T00:00:00Z",
    )


def _eifu_compliant():
    # StrykercorpeIFUFlag is a real boolean in the payload.
    return {ifc.ATTR_EIFU_FLAG: True, ifc.ATTR_DEFAULT_UNIT: ifc.VAL_UNIT_AS_NEEDED}


def _print_compliant():
    return {ifc.ATTR_EIFU_FLAG: False, ifc.ATTR_DEFAULT_UNIT: ifc.VAL_UNIT_PIECE}


def test_has_language_suffix():
    assert has_language_suffix("12345-EN") is True
    assert has_language_suffix("ABC-123-fr") is True  # case-insensitive
    assert has_language_suffix("12345-ZZ") is False   # ZZ not a real code
    assert has_language_suffix("12345") is False
    assert has_language_suffix("12345-ENG") is False  # three chars, not -XX
    assert has_language_suffix("") is False
    assert has_language_suffix(None) is False


def test_classify_branches():
    assert classify(["A-EN", "A-FR"], [])[0] == "eIFU"
    assert classify(["A-EN", "A-FR"], ["A-PRINT"])[0] == "Hybrid (eIFU + print)"
    print_label, passed, _ = classify([], ["A-PRINT", "B-PRINT"])
    assert print_label == "Print" and passed is False
    assert classify(["A-EN"], ["B", "C"])[0] == "Needs Review"   # ambiguous mix
    assert classify([], [])[0] == "Needs Review"                  # no IFU PDPs


def _config_with_ifus(conn, config_id, ifu_specs):
    """ifu_specs: list of (number, extra_attrs) tuples."""
    upsert_object(conn, _obj(config_id, "Config PDP", config_id))
    items = []
    for i, (num, attrs) in enumerate(ifu_specs):
        iid = f"{config_id}-IFU{i}"
        upsert_object(conn, _obj(iid, "IFU PDP", num, attrs))
        items.append({"ID": iid, "Number": num})
    save_relationships(conn, config_id, "uses", items, "now")


def _classification_row(results, config_id):
    return next(r for r in results if r.source_object_id == config_id
               and r.target_object_id == "CLASSIFICATION")


def test_end_to_end_classification(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    _config_with_ifus(conn, "CFG_EIFU", [(f"100-{c}", _eifu_compliant()) for c in ("EN", "FR", "DE")])
    _config_with_ifus(conn, "CFG_HYBRID", [
        ("200-EN", _eifu_compliant()), ("200-FR", _eifu_compliant()), ("200", _print_compliant()),
    ])
    _config_with_ifus(conn, "CFG_PRINT", [("300", {}), ("301", {})])
    _config_with_ifus(conn, "CFG_NONE", [])
    conn.commit()

    results = classify_config_pdps(conn)
    assert _classification_row(results, "CFG_EIFU").source_value == "eIFU"
    assert _classification_row(results, "CFG_EIFU").passed is True
    assert _classification_row(results, "CFG_HYBRID").source_value == "Hybrid (eIFU + print)"
    assert _classification_row(results, "CFG_HYBRID").passed is True
    assert _classification_row(results, "CFG_PRINT").source_value == "Print"
    assert _classification_row(results, "CFG_PRINT").passed is False
    assert _classification_row(results, "CFG_NONE").source_value == "Needs Review"
    conn.close()


def test_eifu_attribute_failure_fails_classification(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    bad = _eifu_compliant()
    bad[ifc.ATTR_DEFAULT_UNIT] = "Piece"  # wrong unit for eIFU
    _config_with_ifus(conn, "CFG", [("100-EN", _eifu_compliant()), ("100-FR", bad)])
    conn.commit()

    results = classify_config_pdps(conn)
    row = _classification_row(results, "CFG")
    assert row.source_value == "eIFU"
    assert row.passed is False
    assert "attribute check(s) failed" in row.message
    # one failing attribute row for the bad IFU PDP's Default Unit
    fails = [r for r in results if not r.passed and r.target_attr == "Default Unit"]
    assert len(fails) == 1


def test_hybrid_print_part_uses_piece_rules(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    # Print part correct (No + Piece) -> passes; make electronic part wrong to prove split.
    _config_with_ifus(conn, "CFG", [("200-EN", _eifu_compliant()), ("200", _print_compliant())])
    conn.commit()

    results = classify_config_pdps(conn)
    assert _classification_row(results, "CFG").passed is True
    # Print part is checked with the print rules (eIFU Flag=False, Default Unit=Piece).
    print_part_rows = [r for r in results if r.source_object_id == "CFG-IFU1"]
    assert {r.target_attr for r in print_part_rows} == {"eIFU Flag", "Default Unit"}


def test_registered_and_runs_via_engine(tmp_path):
    from oneplm_ingestion.checks import run_all_checks

    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    _config_with_ifus(conn, "CFG_EIFU", [("100-EN", _eifu_compliant()), ("100-FR", _eifu_compliant())])
    conn.commit()

    cfg = tmp_path / "checks.json"
    cfg.write_text('[{"name": "IFU Class", "kind": "python", '
                   '"function": "config_pdp_ifu_classification"}]')
    results = run_all_checks(conn, cfg)["IFU Class"]
    assert _classification_row(results, "CFG_EIFU").source_value == "eIFU"
    conn.close()
