"""
FastAPI dependencies: authentication, rate limiting, backpressure.

  ingest_project         X-API-Key -> the project it belongs to (401 if none)
  dashboard_project      /p/<slug>/api/... + X-Dashboard-Token -> that project
                         (404 for an unknown slug, 401 for a wrong token)
  deduct_rate_limit      logs per minute, per project
  check_queue_pressure   429 while the Redis stream is too deep
  verify_admin_token     /admin/*
"""

import hmac
import os
import time

from fastapi import Depends, Header, HTTPException, Request
from redis.asyncio import Redis

from projects import Project, by_ingest_key, token_opens

RATE_LIMIT_PER_PROJECT: int = int(os.getenv("RATE_LIMIT_PER_PROJECT", "60000"))
QUEUE_BACKPRESSURE_LIMIT: int = int(os.getenv("QUEUE_BACKPRESSURE_LIMIT", "50000"))
ADMIN_TOKEN: str = os.getenv("ADMIN_TOKEN", "")
# 2x the 60-second window: the key outlives its window under clock skew.
_RATE_LIMIT_TTL = 120


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


async def deduct_rate_limit(request: Request, project: Project, batch_size: int):
    """Logs per minute per project -- one busy project cannot crowd out the rest."""
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


async def check_queue_pressure(redis: Redis = Depends(get_redis)):
    queue_len = await redis.xlen("logs_stream")
    if queue_len > QUEUE_BACKPRESSURE_LIMIT:
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": "30"},
            detail={"error": "queue_full", "queue_depth": queue_len, "retry_after": 30},
        )
