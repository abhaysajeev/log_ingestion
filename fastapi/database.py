"""
MongoDB indexes for the logs collection. Idempotent: run at every start-up.

Every dashboard query is fenced to one project, so every index starts with
project_id:

  project_newest       (project_id, received_at desc, _id desc)
                       -- the page sort itself, and date ranges
  project_device       (project_id, device_id, received_at desc)
  project_request_id   unique (project_id, log.request_id), only where a
                       request id exists: a log sent twice (a sender retry, a
                       worker replay) is stored once, and lookup by request id
                       is instant
  retention            TTL on received_at: LOG_RETENTION_DAYS (default 45);
                       0 keeps logs forever. Changing the number later updates
                       the index in place (collMod) -- no rebuild.
"""

import logging
import os

import pymongo
from motor.motor_asyncio import AsyncIOMotorCollection

log = logging.getLogger(__name__)

RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "45"))
TTL_NAME = "retention"
# Indexes from the first version, replaced by the project-first ones above.
OLD_INDEXES = ("project_time", "device_time", "project_device_time", "ttl_30_days")


async def ensure_indexes(col: AsyncIOMotorCollection):
    existing = await col.index_information()
    for name in OLD_INDEXES:
        if name in existing:
            try:
                await col.drop_index(name)
            except pymongo.errors.OperationFailure:
                pass        # another gunicorn worker dropped it first

    await col.create_index(
        [("project_id", pymongo.ASCENDING), ("received_at", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)],
        name="project_newest",
    )
    await col.create_index(
        [("project_id", pymongo.ASCENDING), ("device_id", pymongo.ASCENDING), ("received_at", pymongo.DESCENDING)],
        name="project_device",
    )
    await col.create_index(
        [("project_id", pymongo.ASCENDING), ("log.request_id", pymongo.ASCENDING)],
        name="project_request_id",
        unique=True,
        partialFilterExpression={"log.request_id": {"$type": "string"}},
    )
    await col.create_index([("trace_id", pymongo.ASCENDING)], name="trace_id", sparse=True)
    await ensure_retention(col, existing)


async def ensure_retention(col: AsyncIOMotorCollection, existing: dict):
    seconds = RETENTION_DAYS * 86_400
    current = existing.get(TTL_NAME)
    # Each gunicorn worker runs this at start-up, so any step may find another
    # worker already did it.
    try:
        if RETENTION_DAYS <= 0:
            if current:
                await col.drop_index(TTL_NAME)
                log.info("log retention: keeping logs forever (TTL index dropped)")
            return
        if current is None:
            await col.create_index([("received_at", pymongo.ASCENDING)], name=TTL_NAME,
                                   expireAfterSeconds=seconds)
        elif current.get("expireAfterSeconds") != seconds:
            await col.database.command("collMod", col.name,
                                       index={"name": TTL_NAME, "expireAfterSeconds": seconds})
    except pymongo.errors.OperationFailure:
        pass
    log.info("log retention: %s days", RETENTION_DAYS)
