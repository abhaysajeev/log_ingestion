"""The API: ingest, back-pressure, Redis failures, validation, /health, and
the dead-letter admin endpoints."""

import json
import time
from datetime import datetime, timezone

import pytest

from conftest import ADMIN, GROUP, STREAM, log, queue, send


def test_a_batch_is_queued(api, r):
    response = send(api, [log(i) for i in range(5)])

    assert response.status_code == 200, response.text
    assert response.json()["accepted"] == 5
    assert r.xlen(STREAM) == 5


def test_the_stream_is_capped_at_stream_maxlen(api, r, monkeypatch):
    import main

    monkeypatch.setattr(main, "STREAM_MAXLEN", 10)
    for _ in range(6):
        assert send(api, [log(i) for i in range(50)]).status_code == 200
    assert r.xlen(STREAM) < 300            # approximate trim: bounded, not exact


def test_a_backlog_over_the_limit_is_429(api, monkeypatch):
    import dependencies

    monkeypatch.setattr(dependencies, "QUEUE_BACKPRESSURE_LIMIT", 3)
    assert send(api, [log(i) for i in range(5)]).status_code == 200

    response = send(api, [log(9)])

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "30"
    assert response.json()["detail"]["reason"] == "backlog"


def test_redis_memory_near_its_limit_is_429(api, monkeypatch):
    import dependencies

    monkeypatch.setattr(dependencies, "REDIS_MEMORY_LIMIT", 0.000001)
    dependencies._memory["at"] = 0.0

    response = send(api, [log(1)])

    assert response.status_code == 429
    assert response.json()["detail"]["reason"] == "redis_memory"


def test_a_full_redis_is_503_with_retry_after_never_500(api, r, monkeypatch):
    """2 Nov 2026: Redis out of memory made every ingest a bare 500."""
    import dependencies

    monkeypatch.setattr(dependencies, "REDIS_MEMORY_LIMIT", 1e15)    # guard off: reach Redis itself
    r.config_set("maxmemory", "1")

    response = send(api, [log(1)])

    assert response.status_code == 503, response.text
    assert response.headers["Retry-After"] == "15"
    assert response.json()["detail"]["error"] == "queue_unavailable"


@pytest.mark.parametrize("bad", [
    {"device_id": "d", "log": json.loads('{"a":' * 25 + "1" + "}" * 25)},
    {"device_id": "d", "form_data": {"n": 2 ** 70}},
    {"device_id": "  "},
], ids=["too-deep", "int-over-64-bit", "blank-device"])
def test_what_mongodb_cannot_store_is_refused_at_the_door(api, r, bad):
    response = send(api, [bad])

    assert response.status_code == 422, response.text
    assert r.xlen(STREAM) == 0


def test_more_than_100_logs_is_422(api, r):
    assert send(api, [log(i) for i in range(101)]).status_code == 422
    assert r.xlen(STREAM) == 0


def test_a_wrong_key_is_401(api):
    assert api.post("/ingest/batch", json={"logs": [log()]}, headers={"X-API-Key": "nope"}).status_code == 401


def test_a_long_trace_id_header_is_cut_to_64(api):
    response = send(api, [log()], **{"X-Trace-ID": "x" * 300})

    assert response.status_code == 200
    assert response.headers["x-trace-id"] == "x" * 64


def test_an_undecodable_trace_id_is_cleaned_not_a_500():
    from middleware import trace_id_from

    assert trace_id_from(b"\xff\xfeabc") == "��abc"
    assert len(trace_id_from(b"   ")) == 36                    # blank: a fresh uuid4


# ── /health ──────────────────────────────────────────────────────────────────

def test_health_is_200_when_logs_would_be_stored(api, r):
    r.set("worker:hb:w1", "now", ex=30)

    response = api.get("/health")

    assert response.status_code == 200, response.json()
    assert response.json()["checks"]["workers"]["alive"] == 1


def test_health_is_503_without_a_live_worker(api):
    response = api.get("/health")

    assert response.status_code == 503
    assert response.json()["checks"]["workers"]["ok"] is False


def test_health_is_503_when_redis_is_near_full(api, r):
    import dependencies

    r.set("worker:hb:w1", "now", ex=30)
    r.config_set("maxmemory", "1mb")
    dependencies._memory["at"] = 0.0

    checks = api.get("/health").json()["checks"]

    assert checks["redis"]["ok"] is False and checks["redis"]["share"] >= 0.8


def test_health_is_503_when_a_message_is_stuck_pending(api, r, monkeypatch):
    import main

    r.set("worker:hb:w1", "now", ex=30)
    queue(r, {"device_id": "d"})
    r.xreadgroup(GROUP, "ghost", {STREAM: ">"}, count=10)      # read, never finished
    monkeypatch.setattr(main, "STUCK_PENDING_MS", 0)
    time.sleep(0.01)

    response = api.get("/health")

    assert response.status_code == 503
    assert response.json()["checks"]["backlog"]["pending"] == 1


def test_livez_only_says_the_process_is_up(api, r):
    r.config_set("maxmemory", "1")
    assert api.get("/livez").status_code == 200


# ── Dead letters (MongoDB) ───────────────────────────────────────────────────

def dead_row(db, sid, reason):
    db.dead_letters.insert_one({"stream_id": sid, "reason": reason, "error": "x", "project_id": "test-proj",
                                "entry": {"trace_id": "t", "received_at": "1793619841.0",
                                          "data": json.dumps({"device_id": "d", "project_id": "test-proj"})},
                                "at": datetime.now(timezone.utc)})


def test_admin_lists_dead_letters_newest_first(api, db):
    dead_row(db, "1-0", "max_deliveries")
    dead_row(db, "2-0", "parse_error")

    body = api.get("/admin/dlq?limit=10", headers={"X-Admin-Token": ADMIN}).json()

    assert body["total"] == 2 and [e["stream_id"] for e in body["entries"]] == ["2-0", "1-0"]
    assert api.get("/admin/dlq", headers={"X-Admin-Token": "wrong"}).status_code == 403


def test_replay_requeues_only_replayable_dead_letters_with_their_origin_id(api, db, r):
    dead_row(db, "1793619841000-0", "max_deliveries")
    dead_row(db, "1793619841000-1", "parse_error")

    body = api.post("/admin/dlq/replay", headers={"X-Admin-Token": ADMIN}).json()

    assert body == {"replayed": 1, "failed": 0}
    assert [d["reason"] for d in db.dead_letters.find()] == ["parse_error"]
    [(_, fields)] = r.xrange(STREAM)
    assert fields["origin_id"] == "1793619841000-0"


def test_old_redis_dead_letters_move_to_mongodb_at_start(r, db):
    """Before 2 Nov 2026 dead letters lived in the logs_dlq stream."""
    r.xadd("logs_dlq", {"original_id": "5-0", "reason": "parse_error: bad", "failed_at": "x", "data": "{"})
    r.xadd("logs_dlq", {"original_id": "6-0", "reason": "max_retries (3) exceeded", "failed_at": "x",
                        "data": "{}"})
    from fastapi.testclient import TestClient

    import main

    main._indexes_created = False
    with TestClient(main.app):
        pass

    assert not r.exists("logs_dlq")
    rows = {d["stream_id"]: d["reason"] for d in db.dead_letters.find()}
    assert rows == {"5-0": "parse_error", "6-0": "max_deliveries"}
