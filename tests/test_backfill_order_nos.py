"""scripts/backfill_order_nos.py: older BYKY logs get log.order_nos from what
they already hold; nothing else changes; a second run does nothing."""

import datetime
import importlib.util
import io
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("backfill", ROOT / "scripts" / "backfill_order_nos.py")
backfill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backfill)

NOW = datetime.datetime.now(datetime.UTC)


def doc(project, form_data=None, response=None, **log):
    return {"project_id": project, "received_at": NOW, "device_id": "d", "form_data": form_data or {},
            "log": {"path": "/x", "response": response or {}, **log}}


def seed(db):
    db.logs.insert_many([
        doc("byky-qa", {"request_data": {"order_no": "A1"}}, {"data": {"order_no": "A1"}}),
        doc("byky-qa", response={"data": {"orders": [{"order_no": "B2"}, {"order_no": "A1"}]}}),
        doc("byky-qa", response={"text": '{"data": {"orders": [{"order_no": "C3", "x": 1}, {"order_no": "D'}),
        doc("byky-qa", response={"code": "ok", "data": {"customer_id": 4}}),          # names no order
        doc("byky-live", response={"data": {"order_no": "L1"}}),
        doc("byky-qa", response={"data": {"order_no": "Z9"}}, order_nos=["Z9"]),     # already done
        doc("other", response={"data": {"order_no": "X"}}),                          # another project
    ])


def by_path(db):
    return [d["log"].get("order_nos") for d in db.logs.find({}, sort=[("_id", 1)])]


def test_a_dry_run_writes_nothing(db):
    seed(db)
    result = backfill.run(db.logs, ["byky-qa", "byky-live"], apply=False, out=io.StringIO())
    assert result == {"seen": 5, "with_orders": 4, "written": 0}
    assert by_path(db) == [None, None, None, None, None, ["Z9"], None]


def test_apply_fills_order_nos_and_nothing_else(db):
    seed(db)
    before = list(db.logs.find({}, sort=[("_id", 1)]))
    result = backfill.run(db.logs, ["byky-qa", "byky-live"], apply=True, batch=2, out=io.StringIO())
    assert result["written"] == 5
    assert by_path(db) == [["A1"], ["A1", "B2"], ["C3"], [], ["L1"], ["Z9"], None]
    for old, new in zip(before, db.logs.find({}, sort=[("_id", 1)]), strict=True):
        new["log"].pop("order_nos", None)
        old["log"].pop("order_nos", None)
        assert old == new                                   # only the one field changed

    again = backfill.run(db.logs, ["byky-qa", "byky-live"], apply=True, out=io.StringIO())
    assert again == {"seen": 0, "with_orders": 0, "written": 0}
