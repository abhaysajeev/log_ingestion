"""
FastAPI dependency functions for the log ingestion API.

Simplified authentication model:
  - Single API_KEY for all devices/clients (internal system)
  - Rate limiting per device_id (from request body, applied in route)
  - Queue backpressure check (Redis stream depth)
  - Admin token for /admin/* endpoints

Auth flow:
  1. verify_api_key      — checks X-API-Key header against single API_KEY env var
  2. check_queue_pressure — rejects if Redis queue too deep
  3. deduct_rate_limit    — called in route after body parse, rate limits by device_id
  4. verify_admin_token   — protects /admin/* endpoints
"""

import os
import time

from fastapi import Depends, Header, HTTPException, Request
from redis.asyncio import Redis


# ── Configuration from environment ────────────────────────────────────────────

API_KEY: str = os.getenv("API_KEY", "")
RATE_LIMIT_PER_DEVICE: int = int(os.getenv("RATE_LIMIT_PER_DEVICE", "5000"))
QUEUE_BACKPRESSURE_LIMIT: int = int(os.getenv("QUEUE_BACKPRESSURE_LIMIT", "50000"))
ADMIN_TOKEN: str = os.getenv("ADMIN_TOKEN", "")
DASHBOARD_TOKEN: str = os.getenv("DASHBOARD_TOKEN", "")


# ── Internal helper ───────────────────────────────────────────────────────────

async def get_redis(request: Request) -> Redis:
    """Retrieve the shared Redis connection from app state."""
    return request.app.state.redis


# ── Layer 0: Dashboard Token Authentication ───────────────────────────────────

async def verify_dashboard_token(
    token: str = Header(..., alias="X-Dashboard-Token"),
):
    """
    Protects read endpoints (/logs, /logs/distinct, /logs/export).
    Checked by the browser dashboard — token is stored in sessionStorage
    and sent with every API request.

    Returns 401 if the token is missing or does not match DASHBOARD_TOKEN.
    """
    if not DASHBOARD_TOKEN or token != DASHBOARD_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing dashboard token")




# ── Layer 1: API Key Authentication ──────────────────────────────────────────

async def verify_api_key(
    api_key: str = Header(..., alias="X-API-Key"),
):
    """
    Validates X-API-Key header against the single API_KEY env var.
    All devices share the same key — this is an internal system.
    Returns 401 if the key doesn't match.
    """
    if not API_KEY or api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ── Layer 2: Admin Token Authentication ──────────────────────────────────────

async def verify_admin_token(
    token: str = Header(..., alias="X-Admin-Token"),
):
    """
    Protects /admin/* endpoints. Set ADMIN_TOKEN in .env.
    Returns 403 if token is missing, empty, or doesn't match.
    """
    if not ADMIN_TOKEN or token != ADMIN_TOKEN:
        raise HTTPException(status_code=403, detail="Invalid or missing admin token")


# ── Layer 3: Per-Device Rate Limiting ────────────────────────────────────────

async def deduct_rate_limit(request: Request, device_id: str, batch_size: int):
    """
    Per-device rate limit measured in LOGS per minute.
    Called from ingest_batch AFTER body is parsed and device_id is known.

    Uses INCRBY(batch_size) to atomically deduct the actual log count.
    A batch of 50 logs from one device consumes 50 of that device's quota.

    Args:
        request:    FastAPI request (to access Redis)
        device_id:  the device identifier to rate-limit against
        batch_size: number of logs in the batch
    """
    redis: Redis = request.app.state.redis
    window = int(time.time() // 60)
    key = f"ratelimit:device:{device_id}:{window}"

    count = await redis.incrby(key, batch_size)
    if count == batch_size:
        # First write in this window — set expiry (120s for safety margin)
        await redis.expire(key, 120)

    if count > RATE_LIMIT_PER_DEVICE:
        reset_in = 60 - (int(time.time()) % 60)
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": str(reset_in)},
            detail={
                "error":             "rate_limit_exceeded",
                "device_id":         device_id,
                "limit_per_minute":  RATE_LIMIT_PER_DEVICE,
                "current_usage":     count,
                "resets_in_seconds": reset_in,
            },
        )


# ── Layer 4: Queue Backpressure ──────────────────────────────────────────────

async def check_queue_pressure(
    redis: Redis = Depends(get_redis),
):
    """
    Checks Redis stream depth before accepting any ingest request.
    If the queue exceeds QUEUE_BACKPRESSURE_LIMIT (default 50,000), returns 429.
    """
    queue_len = await redis.xlen("logs_stream")
    if queue_len > QUEUE_BACKPRESSURE_LIMIT:
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": "30"},
            detail={
                "error":       "queue_full",
                "queue_depth": queue_len,
                "retry_after": 30,
            },
        )
