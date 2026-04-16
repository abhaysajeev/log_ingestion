"""
MongoDB index management for the logs collection.

All indexes are created with background=True and are fully idempotent.
Indexes are designed around the client's payload fields:
  - project_id: which project the log belongs to
  - device_id:  which device sent the log
"""

import pymongo
from motor.motor_asyncio import AsyncIOMotorCollection


async def ensure_indexes(col: AsyncIOMotorCollection):
    """
    Creates all required indexes on the 'logs' collection.

    Index strategy:
    1. project_time  — per-project time-range queries
    2. device_time   — per-device time-range queries
    3. project_device_time — compound: project + device + time
    4. trace_id      — debugging specific requests (sparse)
    5. ttl_30_days   — auto-delete documents after 30 days
    """

    # Per-project time-range queries
    await col.create_index(
        [("project_id", pymongo.ASCENDING), ("received_at", pymongo.DESCENDING)],
        name="project_time",
        background=True,
    )

    # Per-device time-range queries
    await col.create_index(
        [("device_id", pymongo.ASCENDING), ("received_at", pymongo.DESCENDING)],
        name="device_time",
        background=True,
    )

    # Compound — project + device + time (most common real query)
    await col.create_index(
        [
            ("project_id", pymongo.ASCENDING),
            ("device_id",  pymongo.ASCENDING),
            ("received_at", pymongo.DESCENDING),
        ],
        name="project_device_time",
        background=True,
    )

    # Trace ID lookup for debugging specific requests
    await col.create_index(
        [("trace_id", pymongo.ASCENDING)],
        name="trace_id",
        sparse=True,
        background=True,
    )

    # TTL — MongoDB auto-deletes documents 30 days after received_at
    await col.create_index(
        [("received_at", pymongo.ASCENDING)],
        name="ttl_30_days",
        expireAfterSeconds=2_592_000,  # 30 × 24 × 60 × 60
        background=True,
    )
