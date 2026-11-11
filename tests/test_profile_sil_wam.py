"""The sil-wam dashboard profile: it draws, and its filters build safe queries."""
import profiles


def test_sil_wam_profile_is_public_and_hides_field_paths():
    meta = profiles.public("sil-wam")
    assert meta["title"] == "API requests"
    assert {f["key"] for f in meta["filters"]} >= {"env", "api", "status", "company", "mobile", "error"}
    assert all("field" not in f for f in meta["filters"])


def test_sil_wam_filters_build_queries():
    q = profiles.build_query("sil-wam", "sil-wam", {
        "env": "live", "api": "client", "status": "4xx",
        "mobile": "971589177070", "error": "customer_mobile", "slow": "500",
    })
    assert q["project_id"] == "sil-wam"
    clauses = q["$and"]
    assert {"log.environment": "live"} in clauses
    assert {"log.api": "client"} in clauses
    assert {"log.status": {"$gte": 400, "$lte": 499}} in clauses
    assert {"log.duration_ms": {"$gte": 500}} in clauses


def test_sil_wam_rejects_an_unknown_api():
    import pytest
    with pytest.raises(profiles.BadFilter):
        profiles.build_query("sil-wam", "sil-wam", {"api": "panel"})


def test_add_project_offers_the_profile():
    from pathlib import Path
    assert '"sil-wam"' in (Path(__file__).resolve().parent.parent / "scripts" / "add_project.py").read_text()
