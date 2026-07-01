"""Tests for the IFU Drawing PDF language-consistency check."""

from __future__ import annotations

from oneplm_ingestion.content_checks import code_token_present, run_pdf_language_checks
from oneplm_ingestion.db import get_connection, init_db, upsert_object, upsert_pdf
from oneplm_ingestion.models import PDFContent, WindchillObject


def _drawing(oid, number, name):
    return WindchillObject(
        id=oid, type_name="IFU Drawing", windchill_type="PTC.DocMgmt.IFUDrawing",
        number=number, name=name, state=None, revision="AA",
        last_modified="2025-01-01T00:00:00Z",
        attributes={"ID": oid, "Number": number, "Name": name},
        synced_at="2025-01-01T00:00:00Z",
    )


def _add(conn, oid, number, name, filename, extracted_text=None):
    upsert_object(conn, _drawing(oid, number, name))
    upsert_pdf(conn, PDFContent(
        object_id=oid, content_role="primary", filename=filename,
        extracted_text=extracted_text,
    ))


def test_code_token_present():
    assert code_token_present("Widget IFU_EN", "EN") is True
    assert code_token_present("Widget IFU EN", "EN") is True
    assert code_token_present("12345-EN", "EN") is True
    assert code_token_present("ENGINE manual", "EN") is False   # not standalone
    assert code_token_present("Widget FR", "EN") is False
    assert code_token_present(None, "EN") is False


def _by_attr(results, oid):
    return {r.target_attr: r for r in results if r.source_object_id == oid}


def test_all_sources_match(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    _add(conn, "D1", "12345-EN", "Widget IFU EN",
         "12345_AA_IFU_EN.pdf", extracted_text="...body...\nFooter code: EN\n")
    conn.commit()

    r = _by_attr(run_pdf_language_checks(conn), "D1")
    assert r["Title"].passed is True
    assert r["Number"].passed is True
    assert r["Last Page"].passed is True
    conn.close()


def test_mismatches_flagged(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    # filename says EN; title says FR, number suffix FR, last page shows FR
    _add(conn, "D1", "12345-FR", "Widget IFU FR",
         "12345_AA_IFU_EN.pdf", extracted_text="Footer: FR")
    conn.commit()

    r = _by_attr(run_pdf_language_checks(conn), "D1")
    assert r["Title"].passed is False
    assert r["Number"].passed is False
    assert r["Last Page"].passed is False
    conn.close()


def test_missing_extracted_text_skips_last_page(tmp_path):
    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    _add(conn, "D1", "12345-EN", "Widget IFU EN", "12345_AA_IFU_EN.pdf")
    conn.commit()

    r = _by_attr(run_pdf_language_checks(conn), "D1")
    assert r["Last Page"].passed is True
    assert "SKIP" in r["Last Page"].message
    conn.close()


def test_registered_via_engine(tmp_path):
    from oneplm_ingestion.checks import run_all_checks

    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    _add(conn, "D1", "12345-EN", "Widget IFU EN", "12345_AA_IFU_EN.pdf",
         extracted_text="Footer: EN")
    conn.commit()

    cfg = tmp_path / "checks.json"
    cfg.write_text('[{"name": "PDF Lang", "kind": "python", '
                   '"function": "ifu_drawing_pdf_language"}]')
    results = run_all_checks(conn, cfg)["PDF Lang"]
    assert any(r.target_attr == "Title" and r.passed for r in results)
    conn.close()
