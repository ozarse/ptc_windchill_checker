"""Tests for relationship sync resolution helpers."""

from __future__ import annotations

from ptc_syncer_ingestion.relationships import _resolve_uses


class _FakeClient:
    def get_part_uses(self, part_id):
        return [
            {"ID": "LINK1", "Quantity": 0, "Unit": {"Value": "ea"}, "@odata.type": "#PTC.PartUse"},
            {"ID": "LINK2", "Quantity": 2},
            {"Quantity": 9},  # no ID -> skipped
        ]

    def get_uses_part(self, part_id, use_id):
        if use_id == "LINK2":
            raise RuntimeError("boom")  # failed follow -> item dropped, others kept
        return {"ID": f"CHILD-{use_id}", "Number": f"N-{use_id}"}


def test_resolve_uses_preserves_link_attributes():
    items = _resolve_uses(_FakeClient(), "P1")
    assert len(items) == 1
    item = items[0]
    # The resolved child part's identity is what gets stored as target_id/number.
    assert item["ID"] == "CHILD-LINK1"
    assert item["Number"] == "N-LINK1"
    # The PartUse link's usage attributes ride along under UsesLink.
    assert item["UsesLink"]["Quantity"] == 0
    assert item["UsesLink"]["Unit"] == {"Value": "ea"}
    # OData noise is stripped from the preserved link.
    assert "@odata.type" not in item["UsesLink"]


def test_check_result_status_roundtrip(tmp_path):
    from ptc_syncer_ingestion.db import get_check_results, get_connection, init_db, save_check_results
    from ptc_syncer_ingestion.models import CheckResult

    conn = get_connection(tmp_path / "t.db")
    init_db(conn)
    results = [
        CheckResult("c", "S", "T", "a", "", "v", None, passed=True, message="PASS", checked_at="x"),
        CheckResult("c", "S", "T", "a", "", "v", None, passed=False, message="FAIL", checked_at="x"),
        CheckResult("c", "S", "T", "a", "", "v", None, passed=True, status="skip",
                    message="SKIP", checked_at="x"),
    ]
    save_check_results(conn, results)
    conn.commit()

    stored = get_check_results(conn, check_name="c")
    assert [r.status for r in stored] == ["pass", "fail", "skip"]
    # Skips stay passed=True so failed_only filtering is unaffected.
    assert [r.status for r in get_check_results(conn, check_name="c", failed_only=True)] == ["fail"]
    conn.close()
