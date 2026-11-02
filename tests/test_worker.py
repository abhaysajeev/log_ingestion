"""The worker: storing, deleting what it stored, isolating bad logs,
recovering crashed workers' messages, and waiting out a MongoDB outage."""

import asyncio

from pymongo.errors import ServerSelectionTimeoutError

import worker as W
from conftest import GROUP, STREAM, drain, queue


def good(i):
    return {"project_id": "test-proj", "device_id": f"d{i}", "form_data": {}, "log": {"i": i}}


async def test_stored_logs_leave_the_stream(stack, r, db):
    """2 Nov 2026: logs were only XACK'd, the stream kept them all and Redis
    filled up. A stored log is deleted from the stream."""
    redis, col, dead = stack
    for i in range(5):
        queue(r, good(i))

    await drain(redis, col, dead)

    assert db.logs.count_documents({}) == 5
    assert r.xlen(STREAM) == 0
    assert r.xpending(STREAM, GROUP)["pending"] == 0


async def test_one_unstorable_log_is_dead_lettered_and_the_rest_stored(stack, r, db):
    """A log MongoDB refuses (a 2^70 integer) no longer stalls its batch."""
    redis, col, dead = stack
    for i in range(9):
        queue(r, good(i))
    bad_id = queue(r, {**good(99), "log": {"n": 2 ** 70}})

    await drain(redis, col, dead)

    assert db.logs.count_documents({}) == 9
    [row] = list(db.dead_letters.find())
    assert (row["stream_id"], row["reason"]) == (bad_id, W.INVALID_DOCUMENT)
    assert r.xlen(STREAM) == 0


async def test_a_log_the_server_refuses_is_dead_lettered_and_the_rest_stored(stack, r, db):
    """The other path: the driver sends the batch and MongoDB rejects one
    document (here a collection validator; in life e.g. a too-deep document)."""
    redis, col, dead = stack
    db.create_collection("logs", validator={"device_id": {"$ne": "reject-me"}})
    queue(r, good(1))
    bad_id = queue(r, {**good(2), "device_id": "reject-me"})
    queue(r, good(3))

    await drain(redis, col, dead)

    assert db.logs.count_documents({}) == 2
    [row] = list(db.dead_letters.find())
    assert (row["stream_id"], row["reason"]) == (bad_id, W.INVALID_DOCUMENT)
    assert r.xlen(STREAM) == 0


async def test_a_malformed_message_is_a_parse_error(stack, r, db):
    redis, col, dead = stack
    queue(r, "not json")
    queue(r, good(1))

    await drain(redis, col, dead)

    assert db.logs.count_documents({}) == 1
    assert [d["reason"] for d in db.dead_letters.find()] == [W.PARSE_ERROR]
    assert r.xlen(STREAM) == 0


async def test_a_log_stored_twice_is_one_document(stack, r, db):
    """A redelivery or replay of an already stored log: same _id, no copy."""
    redis, col, dead = stack
    first = queue(r, good(1))
    await drain(redis, col, dead)

    queue(r, good(1), origin_id=first)          # e.g. a dead letter replayed
    await drain(redis, col, dead)

    assert db.logs.count_documents({}) == 1
    assert db.logs.find_one()["_id"] == W.stream_object_id(first)


def test_stream_ids_map_to_time_ordered_object_ids():
    early, late = W.stream_object_id("1793619841000-0"), W.stream_object_id("1793619900000-0")
    assert early.generation_time < late.generation_time
    assert W.stream_object_id("1793619841000-0") == early
    assert W.stream_object_id("1793619841000-1") != early


async def test_a_crashed_workers_messages_are_stored_not_dead_lettered(stack, r, db, monkeypatch):
    """Before: the janitor claimed them, never stored them, and after a few
    rounds sent them all to the dead-letter queue."""
    redis, col, dead = stack
    for i in range(3):
        queue(r, good(i))
    r.xreadgroup(GROUP, "worker-that-died", {STREAM: ">"}, count=10)
    monkeypatch.setattr(W, "CLAIM_IDLE_MS", 0)

    await W.take_over_stuck(redis, col, dead)

    assert db.logs.count_documents({}) == 3
    assert db.dead_letters.count_documents({}) == 0
    assert r.xlen(STREAM) == 0 and r.xpending(STREAM, GROUP)["pending"] == 0


async def test_a_restarted_worker_finishes_its_own_pending_messages(stack, r, db):
    redis, col, dead = stack
    for i in range(4):
        queue(r, good(i))
    r.xreadgroup(GROUP, W.CONSUMER_NAME, {STREAM: ">"}, count=10)      # read, then "crashed"

    await W.recover_own_pending(redis, col, dead)

    assert db.logs.count_documents({}) == 4
    assert r.xlen(STREAM) == 0


async def test_a_message_that_keeps_killing_workers_is_dead_lettered(stack, r, db, monkeypatch):
    redis, col, dead = stack
    msg = queue(r, good(1))
    r.xreadgroup(GROUP, "worker-that-died", {STREAM: ">"}, count=10)   # delivery 1
    monkeypatch.setattr(W, "CLAIM_IDLE_MS", 0)
    monkeypatch.setattr(W, "MAX_DELIVERIES", 1)

    await W.take_over_stuck(redis, col, dead)                          # delivery 2 > 1

    assert db.logs.count_documents({}) == 0
    [row] = list(db.dead_letters.find())
    assert (row["stream_id"], row["reason"]) == (msg, W.MAX_DELIVERED)
    assert r.xlen(STREAM) == 0


class FlakyCollection:
    """The logs collection, unreachable while `down` is set."""

    def __init__(self, col):
        self.col, self.down, self.calls = col, True, 0

    async def insert_many(self, *args, **kwargs):
        self.calls += 1
        if self.down:
            raise ServerSelectionTimeoutError("mongodb is down")
        return await self.col.insert_many(*args, **kwargs)

    async def insert_one(self, *args, **kwargs):
        if self.down:
            raise ServerSelectionTimeoutError("mongodb is down")
        return await self.col.insert_one(*args, **kwargs)


async def test_while_mongodb_is_down_the_worker_holds_one_batch_and_backs_off(stack, r, db, monkeypatch):
    """Before: it kept reading 500 more messages per pass until the container
    was OOM-killed. Now it holds one batch, reads nothing new, retries it with
    backoff, and stores everything once MongoDB is back."""
    redis, col, dead = stack
    monkeypatch.setattr(W, "BATCH_SIZE", 5)
    monkeypatch.setattr(W, "FLUSH_INTERVAL", 0)
    monkeypatch.setattr(W, "BACKOFF_MAX", 0.2)
    for i in range(12):
        queue(r, good(i))
    flaky = FlakyCollection(col)
    task = asyncio.create_task(W.worker_loop(redis, flaky, dead))
    try:
        await asyncio.sleep(1.5)
        assert flaky.calls >= 3                                   # retried, with backoff
        assert r.xpending(STREAM, GROUP)["pending"] == 5          # one batch held, nothing more read
        assert db.logs.count_documents({}) == 0
        assert db.dead_letters.count_documents({}) == 0           # waiting is not a failure

        flaky.down = False
        for _ in range(50):
            if db.logs.count_documents({}) == 12:
                break
            await asyncio.sleep(0.1)
    finally:
        task.cancel()
        W._storing_fails = False

    assert db.logs.count_documents({}) == 12
    assert r.xlen(STREAM) == 0


async def test_the_worker_reports_a_heartbeat(stack, r, tmp_path, monkeypatch):
    redis, _, _ = stack
    monkeypatch.setattr(W, "HEARTBEAT_FILE", str(tmp_path / "hb"))
    monkeypatch.setattr(W, "_last_heartbeat", 0.0)

    await W.heartbeat(redis)

    assert r.ttl(W.HEARTBEAT_KEY) > 0
    assert (tmp_path / "hb").exists()
