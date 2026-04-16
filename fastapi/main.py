"""
FastAPI Log Ingestion API — main application module.

Endpoints:
  GET  /health            — service health with queue/DLQ depth
  POST /ingest/batch      — accept up to 100 logs per batch, queue to Redis Stream
  GET  /logs              — query logs with filters and cursor-based pagination
  GET  /logs/export       — stream all matching logs as JSON download
  GET  /dashboard         — web dashboard for browsing logs
  GET  /admin/dlq         — inspect dead-letter queue (admin-only)
  POST /admin/dlq/replay  — replay DLQ entries to main stream (admin-only)

Authentication:
  Single shared API_KEY for all devices (internal system).
  No per-client keys — all devices are trusted, owned clients.

Rate limiting:
  Per device_id, measured in logs per minute. Extracted from the
  batch body after Pydantic validation, using the first log's device_id.
"""

import json
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import uvloop
from bson import ObjectId
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis

from dependencies import (
    check_queue_pressure,
    deduct_rate_limit,
    verify_admin_token,
    verify_api_key,
    verify_dashboard_token,
)
from database import ensure_indexes
from middleware import TracingMiddleware
from models import BatchIngestPayload

uvloop.install()  # Must be called before any event loop is created

_indexes_created = False  # only run ensure_indexes once per process


# ── Application Lifespan ──────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages startup and shutdown of shared resources.

    Startup:
      1. Connect to Redis (shared across all requests in this worker)
      2. Connect to MongoDB (pool capped at 10 per worker)
      3. Create indexes if not already done
      4. Create Redis consumer group for workers (idempotent)

    Shutdown:
      1. Close Redis connection
      2. Close MongoDB connection pool
    """
    global _indexes_created

    app.state.redis = Redis.from_url(
        os.getenv("REDIS_URL"), decode_responses=True
    )

    mongo_client = AsyncIOMotorClient(
        os.getenv("MONGODB_URL"),
        maxPoolSize=10,
        minPoolSize=2,
    )
    app.state.mongo_col = mongo_client["logsdb"]["logs"]

    if not _indexes_created:
        await ensure_indexes(app.state.mongo_col)
        _indexes_created = True

    try:
        await app.state.redis.xgroup_create(
            "logs_stream", "workers", id="0", mkstream=True
        )
    except Exception:
        pass  # Group already exists

    yield

    await app.state.redis.aclose()
    mongo_client.close()


# ── App Instance ──────────────────────────────────────────────────────────────

app = FastAPI(lifespan=lifespan, title="Log Ingestion API")
app.add_middleware(TracingMiddleware)

STATIC_DIR = Path(__file__).parent / "static"


# ── Dashboard ─────────────────────────────────────────────────────────────────

@app.get("/dashboard")
async def dashboard():
    """Serve the log dashboard HTML page."""
    html_path = STATIC_DIR / "dashboard.html"
    return FileResponse(html_path, media_type="text/html")


@app.get("/logs/distinct")
async def logs_distinct(
    request: Request,
    field: str,
    prefix: Optional[str] = None,
    limit: int = 15,
    _auth: None = Depends(verify_dashboard_token),
):
    """
    Return distinct values for a given field (project_id or device_id).
    Optionally filtered by a prefix string (for autocomplete dropdowns).
    Capped at limit values (max 50).
    """
    if field not in ("project_id", "device_id"):
        raise HTTPException(status_code=400, detail="field must be project_id or device_id")

    col = request.app.state.mongo_col
    limit = min(max(limit, 1), 50)

    query = {}
    if prefix:
        # Case-insensitive prefix match
        query[field] = {"$regex": f"^{prefix}", "$options": "i"}

    values = await col.distinct(field, query)
    values = sorted([v for v in values if v])[:limit]
    return {"field": field, "values": values}


# ── Health Endpoint ───────────────────────────────────────────────────────────

@app.get("/health")
async def health(request: Request):
    """Returns service health status including Redis queue and DLQ depths."""
    redis: Redis = request.app.state.redis
    queue_len = await redis.xlen("logs_stream")
    dlq_len = await redis.xlen("logs_dlq") if await redis.exists("logs_dlq") else 0
    return {
        "status": "ok",
        "queue_depth": queue_len,
        "dlq_depth": dlq_len,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── Ingest Endpoint ──────────────────────────────────────────────────────────

@app.post("/ingest/batch")
async def ingest_batch(
    request: Request,
    batch: BatchIngestPayload,
    _auth: None = Depends(verify_api_key),
    _queue: None = Depends(check_queue_pressure),
):
    """
    Accept a batch of log events and queue them to Redis Stream.

    Auth: single X-API-Key header check (all devices share one key).
    Rate limit: per device_id from the body, applied after parse.

    Each log entry is stored as a separate Redis Stream message with:
      - trace_id:    from TracingMiddleware (UUID4)
      - received_at: server epoch timestamp
      - data:        JSON-serialized log entry (parsed by worker)
    """
    redis: Redis = request.app.state.redis
    trace_id = request.state.trace_id
    received_at = datetime.now(timezone.utc).timestamp()

    if not batch.logs:
        return {"accepted": 0, "trace_id": trace_id}

    if len(batch.logs) > 100:
        raise HTTPException(status_code=400, detail="Max 100 logs per batch")

    # Rate limit by device_id — use the first log's device_id
    # (all logs in a batch come from the same device in practice)
    device_id = batch.logs[0].device_id
    await deduct_rate_limit(request, device_id, len(batch.logs))

    # Pipeline XADD — all logs queued in a single Redis round-trip
    pipe = redis.pipeline()
    for log in batch.logs:
        pipe.xadd(
            "logs_stream",
            {
                "trace_id":    trace_id,
                "received_at": str(received_at),
                "data":        log.model_dump_json(),
            },
            maxlen=500_000,
            approximate=True,
        )
    await pipe.execute()

    return {
        "accepted":  len(batch.logs),
        "trace_id":  trace_id,
        "queued_at": datetime.fromtimestamp(received_at, tz=timezone.utc).isoformat(),
    }


# ── Query Endpoint ────────────────────────────────────────────────────────────

@app.get("/logs")
async def query_logs(
    request: Request,
    project_id: Optional[str] = None,
    device_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    from_dt: Optional[datetime] = None,
    to_dt: Optional[datetime] = None,
    limit: int = 100,
    last_id: Optional[str] = None,
    _auth: None = Depends(verify_dashboard_token),
):
    """
    Fetch logs with optional filters and cursor-based pagination.

    Filters (all optional, AND logic):
      - project_id:  exact match on project identifier
      - device_id:   exact match on device identifier
      - trace_id:    exact match on request trace ID
      - from_dt:     logs received at or after this datetime
      - to_dt:       logs received at or before this datetime

    Pagination:
      - First request: omit last_id. Response includes next_cursor.
      - Next request: pass next_cursor as last_id to get the next page.
      - Results sorted by _id descending (newest first).

    Limit: 1–1000, default 100.
    """
    col = request.app.state.mongo_col
    query: dict = {}

    if trace_id:    query["trace_id"]   = trace_id
    if project_id:  query["project_id"] = project_id
    if device_id:   query["device_id"]  = device_id
    if from_dt or to_dt:
        query["received_at"] = {}
        if from_dt: query["received_at"]["$gte"] = from_dt
        if to_dt:   query["received_at"]["$lte"] = to_dt

    if last_id:
        try:
            query["_id"] = {"$lt": ObjectId(last_id)}
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid last_id cursor")

    limit = min(max(limit, 1), 1000)
    docs = (
        await col
        .find(query)
        .sort("_id", -1)
        .limit(limit)
        .to_list(limit)
    )

    results = []
    for doc in docs:
        doc["id"] = str(doc.pop("_id"))
        if isinstance(doc.get("received_at"), datetime):
            doc["received_at"] = doc["received_at"].isoformat()
        results.append(doc)

    next_cursor = results[-1]["id"] if len(results) == limit else None

    return {
        "count":       len(results),
        "next_cursor": next_cursor,
        "logs":        results,
    }


# ── Export Endpoint ───────────────────────────────────────────────────────────

@app.get("/logs/export")
async def export_logs(
    request: Request,
    project_id: Optional[str] = None,
    device_id: Optional[str] = None,
    trace_id: Optional[str] = None,
    from_dt: Optional[datetime] = None,
    to_dt: Optional[datetime] = None,
):
    """
    Stream all matching logs as a JSON file download.
    Iterates through results in batches of 500 to avoid loading
    everything into memory at once.
    """
    col = request.app.state.mongo_col
    query: dict = {}

    if trace_id:    query["trace_id"]   = trace_id
    if project_id:  query["project_id"] = project_id
    if device_id:   query["device_id"]  = device_id
    if from_dt or to_dt:
        query["received_at"] = {}
        if from_dt: query["received_at"]["$gte"] = from_dt
        if to_dt:   query["received_at"]["$lte"] = to_dt

    async def stream_docs():
        yield "[\n"
        cursor = col.find(query).sort("_id", -1)
        first = True
        async for doc in cursor:
            doc["id"] = str(doc.pop("_id"))
            if isinstance(doc.get("received_at"), datetime):
                doc["received_at"] = doc["received_at"].isoformat()
            prefix = "  " if first else ",\n  "
            first = False
            yield prefix + json.dumps(doc, default=str)
        yield "\n]\n"

    filename = f"logs_export_{project_id or 'all'}_{datetime.now().strftime('%Y%m%d')}.json"
    return StreamingResponse(
        stream_docs(),
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Admin Endpoints — protected by X-Admin-Token header ──────────────────────

@app.get("/admin/dlq")
async def inspect_dlq(
    request: Request,
    limit: int = 50,
    _: None = Depends(verify_admin_token),
):
    """Inspect dead-letter queue entries. Requires X-Admin-Token header."""
    redis: Redis = request.app.state.redis
    entries = await redis.xrange("logs_dlq", count=min(limit, 500))
    return {"count": len(entries), "entries": entries}


@app.post("/admin/dlq/replay")
async def replay_dlq(
    request: Request,
    _: None = Depends(verify_admin_token),
):
    """Move all DLQ entries back to main stream for reprocessing."""
    redis: Redis = request.app.state.redis
    entries = await redis.xrange("logs_dlq", count=1000)
    if not entries:
        return {"replayed": 0, "message": "DLQ is empty"}

    pipe = redis.pipeline()
    skip_keys = {"reason", "failed_at", "original_id"}
    for entry_id, data in entries:
        replay_data = {k: v for k, v in data.items() if k not in skip_keys}
        pipe.xadd("logs_stream", replay_data, maxlen=500_000, approximate=True)
        pipe.xdel("logs_dlq", entry_id)
    await pipe.execute()
    return {"replayed": len(entries)}
