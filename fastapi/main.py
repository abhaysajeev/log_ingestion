"""
FastAPI Log Ingestion API.

  GET  /health                     200 only when logs are really being stored:
                                   Redis up and below its memory limit, MongoDB
                                   up, workers alive and keeping up (503 + the
                                   failing checks otherwise). For an uptime
                                   monitor.
  GET  /livez                      the process answers (Docker's healthcheck)
  POST /ingest/batch               up to 100 logs; X-API-Key picks the project
  GET  /p/<slug>/                  that project's dashboard (the page itself
                                   holds nothing; its data calls need the token)
  GET  /p/<slug>/api/meta          project name + the profile the page draws
  GET  /p/<slug>/api/logs          one page of logs, newest first, filtered
  GET  /p/<slug>/api/logs/<id>     one log, in full
  GET  /p/<slug>/api/suggest       values seen lately, for a filter box
  GET  /p/<slug>/api/export        the filtered logs as a JSON download
  GET  /admin/dlq, POST /admin/dlq/replay   (X-Admin-Token) -- the
                                   dead_letters collection

Every /p/<slug>/api/ call needs that project's X-Dashboard-Token and only ever
reads that project's logs (profiles.build_query fences the query).

Any Redis failure answers 503 with Retry-After -- never a bare 500 -- so a
sender knows to try again (2 Nov 2026: a full Redis made every ingest a 500).
"""

import json
import logging
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
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from motor.motor_asyncio import AsyncIOMotorClient
from redis.asyncio import Redis
from redis.exceptions import RedisError

import projects as project_registry
from database import RETENTION_DAYS, ensure_indexes
from dependencies import (
    QUEUE_BACKPRESSURE_LIMIT,
    REDIS_MEMORY_LIMIT,
    check_queue_pressure,
    dashboard_project,
    deduct_rate_limit,
    ingest_project,
    redis_memory,
    refund_rate_limit,
    verify_admin_token,
)
from middleware import TracingMiddleware
from models import BatchIngestPayload
from profiles import PROFILES, BadFilter, build_query, pills_fields, public, suggest_field

uvloop.install()  # before any event loop exists

log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"
PER_PAGE_MAX = 200
QUERY_MS = 15_000          # a dashboard query gives up rather than tie up MongoDB
COUNT_MS = 5_000           # past this the page says "many" instead of a number
SUGGEST_DAYS = 30
EXPORT_MAX = 50_000

MAIN_STREAM = "logs_stream"
CONSUMER_GROUP = "workers"
OLD_DLQ_STREAM = "logs_dlq"            # dead letters before 2 Nov 2026, moved to MongoDB at start
# The hard cap on the stream: ~15,000 x 5 KB fits in Redis's 128 MB. Back-
# pressure (429) starts well before it; the cap only guards against a bug.
STREAM_MAXLEN = int(os.getenv("STREAM_MAXLEN", "15000"))
STUCK_PENDING_MS = int(os.getenv("HEALTH_STUCK_PENDING_MS", "300000"))   # 5 min
REPLAYABLE = ("max_deliveries",)       # parse errors and invalid documents would only fail again
DLQ_REPLAY_MAX = 1000

_indexes_created = False


async def move_old_dead_letters(redis: Redis, dead) -> int:
    """Dead letters from before 2 Nov 2026 lived in the Redis stream logs_dlq,
    which could fill Redis by itself. Move them into the dead_letters
    collection and delete the stream. Idempotent (upsert by stream id), safe
    for several gunicorn workers at once."""
    moved = 0
    while await redis.exists(OLD_DLQ_STREAM):
        entries = await redis.xrange(OLD_DLQ_STREAM, count=500)
        if not entries:
            await redis.delete(OLD_DLQ_STREAM)
            break
        for entry_id, data in entries:
            reason = data.get("reason", "")
            sid = data.get("original_id") or entry_id
            await dead.replace_one({"stream_id": sid}, {
                "stream_id": sid,
                "project_id": None,
                "reason": "parse_error" if reason.startswith("parse_error") else "max_deliveries",
                "error": reason[:2000],
                "entry": {k: v for k, v in data.items() if k not in ("reason", "failed_at", "original_id")},
                "at": datetime.now(timezone.utc),
            }, upsert=True)
        await redis.xdel(OLD_DLQ_STREAM, *[entry_id for entry_id, _ in entries])
        moved += len(entries)
    if moved:
        log.info("moved %s dead letters from the %s stream to MongoDB", moved, OLD_DLQ_STREAM)
    return moved


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _indexes_created
    app.state.projects = project_registry.load()
    # Short timeouts: a stalled Redis answers 503 in seconds instead of
    # holding the request until nginx gives up.
    app.state.redis = Redis.from_url(os.getenv("REDIS_URL"), decode_responses=True, socket_timeout=2,
                                     socket_connect_timeout=2, health_check_interval=30)
    mongo_client = AsyncIOMotorClient(os.getenv("MONGODB_URL"), maxPoolSize=10, minPoolSize=2,
                                      serverSelectionTimeoutMS=5000)
    app.state.mongo_col = mongo_client[os.getenv("MONGO_DB", "logsdb")]["logs"]
    app.state.dead_col = app.state.mongo_col.database["dead_letters"]
    if not _indexes_created:
        await ensure_indexes(app.state.mongo_col)
        _indexes_created = True
    try:
        await app.state.redis.xgroup_create(MAIN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass  # the group already exists
    try:
        await move_old_dead_letters(app.state.redis, app.state.dead_col)
    except Exception as error:          # never block start-up on the move
        log.warning("moving old dead letters failed: %s", error)
    yield
    await app.state.redis.aclose()
    mongo_client.close()


app = FastAPI(lifespan=lifespan, title="Log Ingestion API", docs_url=None, redoc_url=None, openapi_url=None)
app.add_middleware(TracingMiddleware)


@app.exception_handler(RedisError)
async def redis_unavailable(request: Request, error: RedisError):
    """Redis is down, full or slow: tell the sender to retry, never a 500."""
    log.warning("redis unavailable on %s: %s", request.url.path, error)
    return JSONResponse(status_code=503, headers={"Retry-After": "15"},
                        content={"detail": {"error": "queue_unavailable", "retry_after": 15}})


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

async def _check(checks: dict, name: str, probe) -> None:
    """Run one health probe; a probe returns (ok, details) or raises."""
    try:
        ok, details = await probe()
    except Exception as error:
        ok, details = False, {"error": f"{type(error).__name__}: {error}"[:300]}
    checks[name] = {"ok": ok, **details}


@app.get("/livez")
async def livez():
    """The process is up -- Docker's healthcheck. Says nothing about Redis or
    MongoDB (see /health), so nginx still starts while they recover."""
    return {"status": "ok"}


@app.get("/health")
async def health(request: Request):
    """200 only when a log sent now would be stored: for the uptime monitor.
    503 lists the failing checks."""
    redis: Redis = request.app.state.redis
    checks: dict = {}

    async def redis_up():
        await redis.ping()
        used, limit = await redis_memory(redis)
        share = used / limit if limit else 0.0
        return share < REDIS_MEMORY_LIMIT, {"used_memory": used, "maxmemory": limit, "share": round(share, 3)}

    async def mongo_up():
        await request.app.state.mongo_col.database.command("ping")
        return True, {}

    async def backlog():
        depth = await redis.xlen(MAIN_STREAM)
        group = next((g for g in await redis.xinfo_groups(MAIN_STREAM) if g.get("name") == CONSUMER_GROUP), {})
        pending = int(group.get("pending") or 0)
        oldest = await redis.xpending_range(MAIN_STREAM, CONSUMER_GROUP, min="-", max="+", count=1)
        idle_ms = int(oldest[0]["time_since_delivered"]) if oldest else 0
        ok = depth <= QUEUE_BACKPRESSURE_LIMIT and idle_ms <= STUCK_PENDING_MS
        return ok, {"queue_depth": depth, "lag": group.get("lag"), "pending": pending,
                    "oldest_pending_idle_ms": idle_ms}

    async def workers():
        alive = [key async for key in redis.scan_iter(match="worker:hb:*", count=100)]
        return bool(alive), {"alive": len(alive)}

    async def dead_letters():
        return True, {"count": await request.app.state.dead_col.estimated_document_count()}

    for name, probe in (("redis", redis_up), ("mongodb", mongo_up), ("backlog", backlog),
                        ("workers", workers), ("dead_letters", dead_letters)):
        await _check(checks, name, probe)
    healthy = all(c["ok"] for c in checks.values())
    return JSONResponse(status_code=200 if healthy else 503, content={
        "status": "ok" if healthy else "failing", "checks": checks,
        "timestamp": datetime.now(timezone.utc).isoformat()})


@app.post("/ingest/batch")
async def ingest_batch(
    request: Request,
    batch: BatchIngestPayload,
    project=Depends(ingest_project),
    _queue: None = Depends(check_queue_pressure),
):
    """Queue a batch to the Redis stream; the workers store it. The project is
    the key's, whatever `project_id` the body carries. At most 100 logs
    (models.MAX_BATCH; more is a 422)."""
    trace_id = request.state.trace_id
    received_at = datetime.now(timezone.utc).timestamp()
    if not batch.logs:
        return {"accepted": 0, "trace_id": trace_id}
    redis: Redis = request.app.state.redis
    quota_key = await deduct_rate_limit(request, project, len(batch.logs))

    pipe = redis.pipeline()
    for entry in batch.logs:
        data = entry.model_dump()
        data["project_id"] = project.slug
        pipe.xadd(MAIN_STREAM,
                  {"trace_id": trace_id, "received_at": str(received_at), "data": json.dumps(data, default=str)},
                  maxlen=STREAM_MAXLEN, approximate=True)
    try:
        await pipe.execute()
    except RedisError:
        await refund_rate_limit(redis, quota_key, len(batch.logs))
        raise                                   # -> 503 (redis_unavailable)
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
async def meta(request: Request, project=Depends(dashboard_project)):
    # A pills switch (BYKY's environment) shows only the values this project's
    # own logs carry -- byky-qa: qa | dev, byky-live: live. Both lookups are
    # fenced to the project and walk the project_env_newest index.
    col = request.app.state.mongo_col
    present = {}
    for key, field in pills_fields(project.profile).items():
        fence = {"project_id": project.slug}
        try:
            values = await col.distinct(field, fence, maxTimeMS=COUNT_MS)
            without = await col.find_one({**fence, field: None}, {"_id": 1}, max_time_ms=COUNT_MS)
        except pymongo.errors.ExecutionTimeout:
            continue            # too slow: the switch shows every declared option
        present[key] = (values, without is not None)
    return {"project": {"slug": project.slug, "name": project.name},
            "profile": public(project.profile, present), "retention_days": RETENTION_DAYS}


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

def _dead_letter_row(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    at = doc.get("at")
    if isinstance(at, datetime):
        doc["at"] = (at if at.tzinfo else at.replace(tzinfo=timezone.utc)).isoformat()
    return doc


@app.get("/admin/dlq")
async def inspect_dlq(request: Request, limit: int = 50, reason: str | None = None,
                      _: None = Depends(verify_admin_token)):
    """The newest dead letters (logs the workers could never store)."""
    limit = min(max(limit, 1), DLQ_REPLAY_MAX)
    query = {"reason": reason} if reason else {}
    dead = request.app.state.dead_col
    rows = [_dead_letter_row(doc) async for doc in dead.find(query).sort("at", pymongo.DESCENDING).limit(limit)]
    return {"total": await dead.count_documents(query), "count": len(rows), "entries": rows}


@app.post("/admin/dlq/replay")
async def replay_dlq(request: Request, _: None = Depends(verify_admin_token),
                     _queue: None = Depends(check_queue_pressure)):
    """Queue replayable dead letters again (at most 1,000 per call; only
    REPLAYABLE reasons -- a parse error or an invalid document would just fail
    again). Each keeps its original stream id as origin_id, so a log that was
    in fact stored before is not stored twice."""
    redis: Redis = request.app.state.redis
    dead = request.app.state.dead_col
    rows = await dead.find({"reason": {"$in": list(REPLAYABLE)}}).limit(DLQ_REPLAY_MAX).to_list(DLQ_REPLAY_MAX)
    if not rows:
        return {"replayed": 0, "message": "Nothing replayable"}
    pipe = redis.pipeline(transaction=False)
    for row in rows:
        pipe.xadd(MAIN_STREAM, {**row["entry"], "origin_id": row["stream_id"]},
                  maxlen=STREAM_MAXLEN, approximate=True)
    results = await pipe.execute(raise_on_error=False)
    queued = [row["_id"] for row, result in zip(rows, results, strict=True) if not isinstance(result, Exception)]
    if queued:
        await dead.delete_many({"_id": {"$in": queued}})
    return {"replayed": len(queued), "failed": len(rows) - len(queued)}
