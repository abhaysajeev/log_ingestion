"""
FastAPI Log Ingestion API.

  GET  /health                     queue and dead-letter depths
  POST /ingest/batch               up to 100 logs; X-API-Key picks the project
  GET  /p/<slug>/                  that project's dashboard (the page itself
                                   holds nothing; its data calls need the token)
  GET  /p/<slug>/api/meta          project name + the profile the page draws
  GET  /p/<slug>/api/logs          one page of logs, newest first, filtered
  GET  /p/<slug>/api/logs/<id>     one log, in full
  GET  /p/<slug>/api/suggest       values seen lately, for a filter box
  GET  /p/<slug>/api/export        the filtered logs as a JSON download
  GET  /admin/dlq, POST /admin/dlq/replay   (X-Admin-Token)

Every /p/<slug>/api/ call needs that project's X-Dashboard-Token and only ever
reads that project's logs (profiles.build_query fences the query).
"""

import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pymongo
import uvloop
from bson import ObjectId
from bson.errors import InvalidId
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis

import projects as project_registry
from database import RETENTION_DAYS, ensure_indexes
from dependencies import (
    check_queue_pressure,
    dashboard_project,
    deduct_rate_limit,
    ingest_project,
    verify_admin_token,
)
from middleware import TracingMiddleware
from models import MAX_BATCH, BatchIngestPayload
from profiles import PROFILES, BadFilter, build_query, public, suggest_field

uvloop.install()  # before any event loop exists

STATIC_DIR = Path(__file__).parent / "static"
PER_PAGE_MAX = 200
QUERY_MS = 15_000          # a dashboard query gives up rather than tie up MongoDB
COUNT_MS = 5_000           # past this the page says "many" instead of a number
SUGGEST_DAYS = 30
EXPORT_MAX = 50_000

_indexes_created = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _indexes_created
    app.state.projects = project_registry.load()
    app.state.redis = Redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
    mongo_client = AsyncIOMotorClient(os.getenv("MONGODB_URL"), maxPoolSize=10, minPoolSize=2)
    app.state.mongo_col = mongo_client["logsdb"]["logs"]
    if not _indexes_created:
        await ensure_indexes(app.state.mongo_col)
        _indexes_created = True
    try:
        await app.state.redis.xgroup_create("logs_stream", "workers", id="0", mkstream=True)
    except Exception:
        pass  # the group already exists
    yield
    await app.state.redis.aclose()
    mongo_client.close()


app = FastAPI(lifespan=lifespan, title="Log Ingestion API", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(TracingMiddleware)


def _plain(doc: dict) -> dict:
    """A MongoDB document as JSON-safe values."""
    doc["id"] = str(doc.pop("_id"))
    received = doc.get("received_at")
    if isinstance(received, datetime):
        doc["received_at"] = (received if received.tzinfo else received.replace(tzinfo=timezone.utc)).isoformat()
    return doc


def _query(project, request: Request) -> dict:
    try:
        return build_query(project.profile, project.slug, request.query_params)
    except BadFilter as error:
        raise HTTPException(status_code=400, detail=str(error)) from error


# ── Health and ingest ─────────────────────────────────────────────────────────

@app.get("/health")
async def health(request: Request):
    redis: Redis = request.app.state.redis
    queue_len = await redis.xlen("logs_stream")
    dlq_len = await redis.xlen("logs_dlq") if await redis.exists("logs_dlq") else 0
    return {"status": "ok", "queue_depth": queue_len, "dlq_depth": dlq_len,
            "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/ingest/batch")
async def ingest_batch(
    request: Request,
    batch: BatchIngestPayload,
    project=Depends(ingest_project),
    _queue: None = Depends(check_queue_pressure),
):
    """Queue a batch to the Redis stream; the workers store it. The project is
    the key's, whatever `project_id` the body carries."""
    trace_id = request.state.trace_id
    received_at = datetime.now(timezone.utc).timestamp()
    if not batch.logs:
        return {"accepted": 0, "trace_id": trace_id}
    if len(batch.logs) > MAX_BATCH:
        raise HTTPException(status_code=400, detail=f"Max {MAX_BATCH} logs per batch")
    await deduct_rate_limit(request, project, len(batch.logs))

    pipe = request.app.state.redis.pipeline()
    for entry in batch.logs:
        data = entry.model_dump()
        data["project_id"] = project.slug
        pipe.xadd("logs_stream",
                  {"trace_id": trace_id, "received_at": str(received_at), "data": json.dumps(data, default=str)},
                  maxlen=500_000, approximate=True)
    await pipe.execute()
    return {"accepted": len(batch.logs), "trace_id": trace_id,
            "queued_at": datetime.fromtimestamp(received_at, tz=timezone.utc).isoformat()}


# ── A project's dashboard ─────────────────────────────────────────────────────

@app.get("/p/{slug}")
async def dashboard_no_slash(slug: str):
    return RedirectResponse(f"/p/{slug}/", status_code=308)


@app.get("/p/{slug}/")
async def dashboard(request: Request, slug: str):
    if slug not in request.app.state.projects:
        raise HTTPException(status_code=404, detail="No such project")
    return FileResponse(STATIC_DIR / "dashboard.html", media_type="text/html",
                        headers={"Cache-Control": "no-store", "Referrer-Policy": "no-referrer",
                                 "X-Frame-Options": "DENY", "X-Content-Type-Options": "nosniff"})


@app.get("/p/{slug}/api/meta")
async def meta(project=Depends(dashboard_project)):
    return {"project": {"slug": project.slug, "name": project.name},
            "profile": public(project.profile), "retention_days": RETENTION_DAYS}


@app.get("/p/{slug}/api/logs")
async def list_logs(request: Request, page: int = 1, per_page: int = 50, project=Depends(dashboard_project)):
    col = request.app.state.mongo_col
    query = _query(project, request)
    per_page = min(max(per_page, 1), PER_PAGE_MAX)
    page = min(max(page, 1), 100_000 // per_page)
    # Bodies the list never shows stay in MongoDB until a row is opened.
    left_out = {path: 0 for path in PROFILES[project.profile].get("list_exclude", [])} or None
    cursor = (col.find(query, left_out)
              .sort([("received_at", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)])
              .skip((page - 1) * per_page).limit(per_page).max_time_ms(QUERY_MS))
    try:
        items = [_plain(doc) async for doc in cursor]
    except pymongo.errors.ExecutionTimeout as error:
        raise HTTPException(status_code=503, detail="That search took too long -- narrow the dates") from error
    try:
        total = await col.count_documents(query, maxTimeMS=COUNT_MS)
    except pymongo.errors.ExecutionTimeout:
        total = None            # the page shows "many" and still pages forward
    return {"page": page, "per_page": per_page, "total": total, "items": items}


@app.get("/p/{slug}/api/logs/{log_id}")
async def one_log(request: Request, log_id: str, project=Depends(dashboard_project)):
    try:
        oid = ObjectId(log_id)
    except InvalidId as error:
        raise HTTPException(status_code=404, detail="No such log") from error
    doc = await request.app.state.mongo_col.find_one({"_id": oid, "project_id": project.slug})
    if doc is None:
        raise HTTPException(status_code=404, detail="No such log")
    return _plain(doc)


@app.get("/p/{slug}/api/suggest")
async def suggest(request: Request, filter: str, prefix: str = "", project=Depends(dashboard_project)):
    """Up to 20 values of a filter's field seen in the last 30 days, most
    frequent first, optionally starting with `prefix`."""
    field = suggest_field(project.profile, filter)
    if field is None:
        raise HTTPException(status_code=400, detail="That filter has no suggestions")
    match = {"project_id": project.slug,
             "received_at": {"$gte": datetime.now(timezone.utc) - timedelta(days=SUGGEST_DAYS)},
             field: {"$type": "string", "$ne": ""}}
    if prefix.strip():
        match[field]["$regex"] = f"^{re.escape(prefix.strip())}"
        match[field]["$options"] = "i"
    pipeline = [{"$match": match}, {"$group": {"_id": f"${field}", "n": {"$sum": 1}}},
                {"$sort": {"n": -1}}, {"$limit": 20}]
    try:
        rows = await request.app.state.mongo_col.aggregate(pipeline, maxTimeMS=COUNT_MS).to_list(20)
    except pymongo.errors.ExecutionTimeout:
        rows = []
    return {"values": [row["_id"] for row in rows]}


@app.get("/p/{slug}/api/export")
async def export(request: Request, project=Depends(dashboard_project)):
    """The filtered logs, newest first, as a JSON file (at most 50,000)."""
    col = request.app.state.mongo_col
    query = _query(project, request)

    async def stream():
        yield "[\n"
        first = True
        cursor = col.find(query).sort([("received_at", pymongo.DESCENDING), ("_id", pymongo.DESCENDING)]) \
            .limit(EXPORT_MAX)
        async for doc in cursor:
            yield ("  " if first else ",\n  ") + json.dumps(_plain(doc), default=str)
            first = False
        yield "\n]\n"

    name = f"{project.slug}-logs-{datetime.now(timezone.utc):%Y%m%d-%H%M}.json"
    return StreamingResponse(stream(), media_type="application/json",
                             headers={"Content-Disposition": f'attachment; filename="{name}"'})


# ── Admin ─────────────────────────────────────────────────────────────────────

@app.get("/admin/dlq")
async def inspect_dlq(request: Request, limit: int = 50, _: None = Depends(verify_admin_token)):
    entries = await request.app.state.redis.xrange("logs_dlq", count=min(limit, 500))
    return {"count": len(entries), "entries": entries}


@app.post("/admin/dlq/replay")
async def replay_dlq(request: Request, _: None = Depends(verify_admin_token)):
    redis: Redis = request.app.state.redis
    entries = await redis.xrange("logs_dlq", count=1000)
    if not entries:
        return {"replayed": 0, "message": "DLQ is empty"}
    pipe = redis.pipeline()
    skip_keys = {"reason", "failed_at", "original_id"}
    for entry_id, data in entries:
        pipe.xadd("logs_stream", {k: v for k, v in data.items() if k not in skip_keys},
                  maxlen=500_000, approximate=True)
        pipe.xdel("logs_dlq", entry_id)
    await pipe.execute()
    return {"replayed": len(entries)}
