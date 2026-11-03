"""
FastAPI dependencies: authentication, rate limiting, backpressure.

  ingest_project         X-API-Key -> the project it belongs to (401 if none)
  dashboard_project      /p/<slug>/api/... + X-Dashboard-Token -> that project
                         (404 for an unknown slug, 401 for a wrong token)
  deduct_rate_limit      logs per minute, per project (refund_rate_limit gives
                         them back when the batch could not be queued)
  check_queue_pressure   429 while the stream backlog is too deep, or Redis
                         memory is near its limit
  verify_admin_token     /admin/*
"""

import hmac
import os
import time

from fastapi import Depends, Header, HTTPException, Request
from redis.asyncio import Redis

from projects import Project, by_ingest_key, token_opens

RATE_LIMIT_PER_PROJECT: int = int(os.getenv("RATE_LIMIT_PER_PROJECT", "60000"))
# The workers XDEL what they store, so XLEN is the backlog of logs not yet in
# MongoDB. 10,000 x ~5 KB fits well inside Redis's 128 MB.
QUEUE_BACKPRESSURE_LIMIT: int = int(os.getenv("QUEUE_BACKPRESSURE_LIMIT", "10000"))
# Refuse new logs (429, retry later) before Redis is full -- a full Redis
# fails every write, the rate limiter's included (2 Nov 2026).
REDIS_MEMORY_LIMIT: float = float(os.getenv("REDIS_MEMORY_LIMIT", "0.8"))
ADMIN_TOKEN: str = os.getenv("ADMIN_TOKEN", "")
# 2x the 60-second window: the key outlives its window under clock skew.
_RATE_LIMIT_TTL = 120
_MEMORY_CACHE_SECONDS = 1.0
_memory = {"at": 0.0, "used": 0, "max": 0}


async def get_redis(request: Request) -> Redis:
    return request.app.state.redis


async def ingest_project(
    request: Request,
    api_key: str = Header("", alias="X-API-Key"),
) -> Project:
    project = by_ingest_key(request.app.state.projects, api_key)
    if project is None:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return project


async def dashboard_project(
    request: Request,
    slug: str,
    token: str = Header("", alias="X-Dashboard-Token"),
) -> Project:
    project = request.app.state.projects.get(slug)
    if project is None:
        raise HTTPException(status_code=404, detail="No such project")
    if not token_opens(project, token):
        raise HTTPException(status_code=401, detail="Invalid or missing dashboard token")
    return project


async def verify_admin_token(token: str = Header("", alias="X-Admin-Token")):
    if not ADMIN_TOKEN or not hmac.compare_digest(ADMIN_TOKEN.encode(), token.encode()):
        raise HTTPException(status_code=403, detail="Invalid or missing admin token")


async def deduct_rate_limit(request: Request, project: Project, batch_size: int) -> str:
    """Logs per minute per project -- one busy project cannot crowd out the
    rest. Returns the counter's key, for refund_rate_limit."""
    redis: Redis = request.app.state.redis
    window = int(time.time() // 60)
    key = f"ratelimit:project:{project.slug}:{window}"
    pipe = redis.pipeline()
    pipe.incrby(key, batch_size)
    pipe.expire(key, _RATE_LIMIT_TTL)
    count, _ = await pipe.execute()
    if count > RATE_LIMIT_PER_PROJECT:
        reset_in = 60 - (int(time.time()) % 60)
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": str(reset_in)},
            detail={"error": "rate_limit_exceeded", "project": project.slug,
                    "limit_per_minute": RATE_LIMIT_PER_PROJECT, "resets_in_seconds": reset_in},
        )
    return key


async def refund_rate_limit(redis: Redis, key: str, batch_size: int) -> None:
    """Give a batch's quota back when it could not be queued, so a sender
    retrying through a Redis hiccup does not use up its minute. Best effort."""
    try:
        await redis.decrby(key, batch_size)
    except Exception:
        pass


async def redis_memory(redis: Redis) -> tuple[int, int]:
    """(used_memory, maxmemory) in bytes, at most a second old. maxmemory 0
    means unlimited."""
    now = time.monotonic()
    if now - _memory["at"] >= _MEMORY_CACHE_SECONDS:
        info = await redis.info("memory")
        _memory.update(at=now, used=int(info.get("used_memory", 0)), max=int(info.get("maxmemory", 0)))
    return _memory["used"], _memory["max"]


def _queue_full(reason: str, **numbers):
    raise HTTPException(
        status_code=429,
        headers={"Retry-After": "30"},
        detail={"error": "queue_full", "reason": reason, "retry_after": 30, **numbers},
    )


async def check_queue_pressure(redis: Redis = Depends(get_redis)):
    """429 while the backlog (logs not yet in MongoDB) is over its limit, or
    Redis is using more than REDIS_MEMORY_LIMIT of its maxmemory."""
    queue_len = await redis.xlen("logs_stream")
    if queue_len > QUEUE_BACKPRESSURE_LIMIT:
        _queue_full("backlog", queue_depth=queue_len)
    used, limit = await redis_memory(redis)
    if limit and used >= limit * REDIS_MEMORY_LIMIT:
        _queue_full("redis_memory", used_memory=used, maxmemory=limit)
