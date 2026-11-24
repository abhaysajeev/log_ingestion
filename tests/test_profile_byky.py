"""The byky dashboard profile's Order no filter (24 Nov 2026): byky_django
ships log.order_nos, the orders a call was about; the filter finds every call
of one order, and an index keeps it fast."""

import datetime

import profiles
import pytest

ORDER = "DUBPP110800000316"


def test_the_order_filter_is_offered_without_its_field_path():
    meta = profiles.public("byky")
    [spec] = [f for f in meta["filters"] if f["key"] == "order"]
    assert (spec["label"], spec["kind"]) == ("Order no", "exact")
    assert "field" not in spec and not spec.get("suggest")


def test_an_order_no_builds_one_exact_clause():
    q = profiles.build_query("byky", "byky-qa", {"order": f"  {ORDER} "})
    assert q["project_id"] == "byky-qa"
    assert {"log.order_nos": ORDER} in q["$and"]


def test_an_empty_order_adds_nothing():
    q = profiles.build_query("byky", "byky-qa", {"order": "  "})
    assert all("log.order_nos" not in clause for clause in q.get("$and", []))


def test_the_filter_finds_every_log_that_lists_the_order(db):
    now = datetime.datetime.now(datetime.UTC)
    db.logs.insert_many([
        {"project_id": "byky-qa", "received_at": now, "log": {"path": "/orders", "order_nos": [ORDER]}},
        {"project_id": "byky-qa", "received_at": now, "log": {"path": "/orders/changes",
                                                              "order_nos": ["DUBPP1", ORDER, "DUBPP2"]}},
        {"project_id": "byky-qa", "received_at": now, "log": {"path": "/orders", "order_nos": ["DUBPP1"]}},
        {"project_id": "byky-qa", "received_at": now, "log": {"path": "/auth/login"}},           # older log
        {"project_id": "byky-live", "received_at": now, "log": {"path": "/orders", "order_nos": [ORDER]}},
    ])
    query = profiles.build_query("byky", "byky-qa", {"order": ORDER, "env": "all"})
    assert sorted(d["log"]["path"] for d in db.logs.find(query)) == ["/orders", "/orders/changes"]


@pytest.mark.usefixtures("api")
def test_the_order_index_is_built_at_start_up(db):
    index = db.logs.index_information()["project_order_newest"]
    assert [field for field, _ in index["key"]] == ["project_id", "log.order_nos", "received_at", "_id"]
    assert index["partialFilterExpression"] == {"log.order_nos": {"$exists": True}}
