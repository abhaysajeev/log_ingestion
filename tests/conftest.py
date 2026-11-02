"""Shared fixtures. Every test starts with an empty Redis and an empty test
database (MONGO_DB), so tests never see each other's logs."""

import json
import os
import sys
from pathlib import Path

import pymongo
import pytest
import redis as redis_sync

ROOT = Path(__file__).resolve().parent.parent
sys.path[:0] = [str(ROOT / "fastapi"), str(ROOT / "worker")]

import worker as W  # noqa: E402

KEY = json.loads((ROOT / "tests" / "projects.test.json").read_text())["test-proj"]["ingest_key"]
ADMIN = os.environ["ADMIN_TOKEN"]
STREAM = "logs_stream"
GROUP = "workers"


@pytest.fixture
def r():
    client = redis_sync.Redis.from_url(os.environ["REDIS_URL"], decode_responses=True)
    yield client
    client.close()


@pytest.fixture
def db():
    client = pymongo.MongoClient(os.environ["MONGODB_URL"])
    yield client[os.environ["MONGO_DB"]]
    client.close()


@pytest.fixture(autouse=True)
def clean(r, db):
    r.config_set("maxmemory", "128mb")
    r.flushall()
    db.client.drop_database(db.name)
    r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    yield
    r.config_set("maxmemory", "128mb")


@pytest.fixture
def api():
    """The API with its lifespan (indexes, consumer group, old-DLQ move) run."""
    from fastapi.testclient import TestClient

    import dependencies
    import main

    main._indexes_created = False
    dependencies._memory["at"] = 0.0
    with TestClient(main.app) as client:
        yield client


@pytest.fixture
async def stack():
    """The worker's own Redis and MongoDB clients, in this test's loop."""
    redis, col, dead = W.connect()
    yield redis, col, dead
    await redis.aclose()
    col.database.client.close()


def log(i=0, **fields):
    return {"device_id": f"dev-{i}", "form_data": {"n": i}, "log": {"path": "/x", "i": i, **fields}}


def send(api, logs, **headers):
    return api.post("/ingest/batch", json={"logs": logs}, headers={"X-API-Key": KEY, **headers})


def queue(r, data, **fields):
    """Put a raw message on the stream, as the API would."""
    return r.xadd(STREAM, {"trace_id": "t", "received_at": "1793619841.0",
                           "data": data if isinstance(data, str) else json.dumps(data), **fields})


async def drain(redis, col, dead, consumer=None):
    """Read everything new as the worker and store it."""
    result = await redis.xreadgroup(GROUP, consumer or W.CONSUMER_NAME, {STREAM: ">"}, count=1000, block=100)
    entries = result[0][1] if result else []
    if entries:
        await W.store_messages(redis, col, dead, entries)
    return len(entries)
