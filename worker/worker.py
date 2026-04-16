"""
Async log ingestion worker — consumes from Redis Stream, writes to MongoDB.

This module runs two concurrent async tasks:

1. worker_loop  — reads new messages from the Redis Stream consumer group,
                  batches them, and bulk-inserts into MongoDB every BATCH_SIZE
                  messages or FLUSH_INTERVAL seconds (whichever comes first).

2. janitor_loop — runs every 30 seconds, claims messages stuck in the Pending
                  Entries List (PEL) for > 60 seconds from crashed workers via
                  XAUTOCLAIM. Checks delivery count on reclaimed messages and
                  routes poison pills to the dead-letter queue (DLQ) after
                  MAX_RETRIES attempts.

Architecture fixes applied:
  #2  — Consumer name includes os.getpid() — unique per Docker replica
  #6  — DLQ writes use maxlen=10,000 — cannot grow unbounded
  #11 — batch/msg_ids only cleared on insert SUCCESS — not in finally block
  #12 — asyncio.get_running_loop() used throughout (not deprecated get_event_loop)
  #13 — Motor connection pool capped at 5 per worker (shared budget with FastAPI)

Data flow:
  Redis Stream → XREADGROUP → build_document() → batch[] → insert_many() → XACK
                                                         ↓ (on parse error)
                                                     route_to_dlq()

Document structure written to MongoDB:
  {
    trace_id:       "uuid4"              — from FastAPI middleware
    received_at:    datetime (UTC)       — server ingestion time (authoritative)
    project_id:     "proj_123"           — which project this log belongs to
    device_id:      "device_abc"         — which device sent this log
    form_data:      { ... }              — free-form JSON, client-defined
    log:            { ... }              — free-form JSON, client-defined
  }
"""

import asyncio
import json
import logging
import os
import smtplib
import socket
from datetime import datetime, timezone
from email.mime.text import MIMEText

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError
from redis.asyncio import Redis

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WORKER] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

# ── Configuration from environment ────────────────────────────────────────────

REDIS_URL      = os.getenv("REDIS_URL", "redis://localhost:6379")
MONGODB_URL    = os.getenv("MONGODB_URL")
BATCH_SIZE     = int(os.getenv("WORKER_BATCH_SIZE", "500"))
FLUSH_INTERVAL = float(os.getenv("WORKER_FLUSH_INTERVAL_SECONDS", "5"))
MAX_RETRIES    = int(os.getenv("WORKER_MAX_RETRIES", "3"))
MAIN_STREAM    = os.getenv("MAIN_STREAM", "logs_stream")
DLQ_STREAM     = os.getenv("DLQ_STREAM", "logs_dlq")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")

# ── Email Alert Configuration ──────────────────────────────────────────────────────
SMTP_HOST           = os.getenv("SMTP_HOST", "")
SMTP_PORT           = int(os.getenv("SMTP_PORT", "587"))   # 587=STARTTLS, 465=SSL
SMTP_USER           = os.getenv("SMTP_USER", "")           # sender email
SMTP_PASSWORD       = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO      = os.getenv("ALERT_EMAIL_TO", "")      # recipient(s), comma-sep
ALERT_DLQ_THRESHOLD = int(os.getenv("ALERT_DLQ_THRESHOLD", "10"))


# fix #2: include PID so two replicas on same host get unique consumer names
# Without this, two Docker containers sharing a hostname would fight over the
# same PEL entries, causing duplicate processing and ACK races.
CONSUMER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"


# ── Email Alert ────────────────────────────────────────────────────────────

_last_alert_time: float = 0.0   # throttle — at most one alert per 30 min


def send_email_alert(subject: str, body: str) -> None:
    """
    Send an alert email via SMTP using company credentials.
    Silently skips if SMTP_HOST, SMTP_USER, or ALERT_EMAIL_TO are not set.
    Supports STARTTLS (port 587, default) and implicit SSL (port 465).
    Throttled to at most one email per 30 minutes to prevent inbox flooding.
    """
    import time
    global _last_alert_time

    if not SMTP_HOST or not SMTP_USER or not ALERT_EMAIL_TO:
        return   # Not configured — skip silently

    now = time.time()
    if now - _last_alert_time < 1800:   # 30-minute cooldown
        log.debug("Email alert throttled — skipping")
        return
    _last_alert_time = now

    recipients = [r.strip() for r in ALERT_EMAIL_TO.split(",") if r.strip()]
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)

    try:
        if SMTP_PORT == 465:
            # Implicit SSL (SMTPS)
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, recipients, msg.as_string())
        else:
            # STARTTLS (port 587 or 25)
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, recipients, msg.as_string())
        log.info(f"Alert email sent to {recipients}")
    except Exception as exc:
        log.warning(f"Alert email failed: {exc}")


# ── Document Builder ──────────────────────────────────────────────────────────

def build_document(entry: dict) -> dict:
    """
    Build a MongoDB document from a Redis Stream entry.

    Maps the client's payload fields (project_id, device_id, form_data, log)
    directly into the MongoDB document. Worker adds server-side metadata
    (trace_id, received_at).

    received_at is stored as an epoch float in Redis (set by FastAPI) and
    converted to a proper datetime here for MongoDB TTL index compatibility.

    Args:
        entry: dict from Redis XREADGROUP with keys:
               trace_id, received_at (epoch str), data (JSON str)

    Returns:
        dict ready for MongoDB insert_many()
    """
    raw_data = json.loads(entry["data"])
    received_ts = float(entry["received_at"])
    return {
        "trace_id":    entry.get("trace_id"),
        "received_at": datetime.fromtimestamp(received_ts, tz=timezone.utc),
        "project_id":  raw_data.get("project_id"),
        "device_id":   raw_data.get("device_id"),
        "form_data":   raw_data.get("form_data", {}),
        "log":         raw_data.get("log", {}),
    }


# ── Dead-Letter Queue ────────────────────────────────────────────────────────

async def route_to_dlq(redis: Redis, msg_id: str, entry: dict, reason: str):
    """
    Move a failed message to the dead-letter queue.

    Called when:
      - build_document() throws a parse error (malformed JSON, missing fields)
      - Janitor detects delivery count > MAX_RETRIES on a reclaimed message

    The DLQ entry preserves the original data plus failure metadata:
      - original_id: the Redis Stream message ID
      - failed_at:   ISO timestamp of when routing occurred
      - reason:      human-readable failure reason

    fix #6: DLQ capped at 10,000 entries via maxlen to prevent unbounded growth
    on a bad deployment. FIFO — oldest entries trimmed when cap is hit.

    After XADD to DLQ, the message is XACK'd from the main stream so the
    consumer group stops tracking it.
    """
    await redis.xadd(
        DLQ_STREAM,
        {
            "original_id": msg_id,
            "failed_at":   datetime.now(timezone.utc).isoformat(),
            "reason":      reason,
            **entry,
        },
        maxlen=10_000,      # fix #6: prevent unbounded DLQ growth
        approximate=True,
    )
    await redis.xack(MAIN_STREAM, CONSUMER_GROUP, msg_id)
    log.warning(f"Routed {msg_id} to DLQ: {reason}")


# ── Batch Insert ──────────────────────────────────────────────────────────────

async def insert_batch(col, docs: list[dict]) -> int:
    """
    Bulk insert documents into MongoDB with ordered=False.

    ordered=False means MongoDB attempts ALL inserts even if some fail.
    This maximizes throughput — a single bad document doesn't block the batch.

    Duplicate key errors (code 11000) are silently ignored — this provides
    idempotency. If a worker crashes after insert but before XACK, the
    janitor reclaims the message. On retry, the duplicate insert is harmless.

    All other errors (disk full, auth failure, etc.) are re-raised so the
    caller does NOT clear the batch — messages stay in PEL for retry.

    Args:
        col: AsyncIOMotorCollection for the 'logs' collection
        docs: list of dicts from build_document()

    Returns:
        Number of successfully inserted documents
    """
    try:
        result = await col.insert_many(docs, ordered=False)
        return len(result.inserted_ids)
    except BulkWriteError as e:
        inserted = e.details.get("nInserted", 0)
        real_errors = [
            err for err in e.details.get("writeErrors", [])
            if err.get("code") != 11000
        ]
        if real_errors:
            raise   # re-raise — caller keeps batch intact for retry
        return inserted


# ── Main Consumer Loop ────────────────────────────────────────────────────────

async def worker_loop(redis: Redis, col):
    """
    Main consumer loop — reads from Redis Stream and bulk-inserts to MongoDB.

    Batching strategy:
      - Accumulate up to BATCH_SIZE (500) documents before flushing
      - OR flush every FLUSH_INTERVAL (5) seconds if batch is non-empty
      - Whichever threshold is hit first triggers the flush

    Why batching matters:
      insert_many(500) is ~50x faster than 500 individual inserts because
      it's a single MongoDB wire protocol round-trip with bulk write command.

    Error handling (fix #11):
      On insert failure, batch[] and msg_ids[] are NOT cleared. The messages
      remain both in local memory for immediate retry AND in the Redis PEL.
      If this worker dies, XAUTOCLAIM in the janitor reclaims them after 60s.

    XREADGROUP semantics:
      - ">" means "give me only new, undelivered messages"
      - block=1000 means "wait up to 1 second for new messages"
      - When no messages arrive, the loop checks flush timer and retries
    """
    batch: list[dict] = []
    msg_ids: list[str] = []
    loop = asyncio.get_running_loop()   # fix #12: not get_event_loop()
    last_flush = loop.time()

    log.info(f"Worker {CONSUMER_NAME} started — reading from {MAIN_STREAM}")

    while True:
        try:
            # Read new messages from the consumer group
            # block=1000ms — returns empty after 1s if no new messages,
            # allowing the flush timer check to run
            messages = await redis.xreadgroup(
                CONSUMER_GROUP,
                CONSUMER_NAME,
                {MAIN_STREAM: ">"},
                count=BATCH_SIZE,
                block=1000,
            )

            if messages:
                for _, entries in messages:
                    for msg_id, entry in entries:
                        try:
                            doc = build_document(entry)
                            batch.append(doc)
                            msg_ids.append(msg_id)
                        except Exception as e:
                            # Malformed data — can never succeed, send to DLQ immediately
                            await route_to_dlq(
                                redis, msg_id, entry, f"parse_error: {e}"
                            )

            # Check if either flush condition is met
            elapsed = loop.time() - last_flush   # fix #12
            if batch and (len(batch) >= BATCH_SIZE or elapsed >= FLUSH_INTERVAL):
                # fix #11: only clear batch and msg_ids on success
                try:
                    inserted = await insert_batch(col, batch)
                    # XACK all messages in one call — tells Redis these are processed
                    await redis.xack(MAIN_STREAM, CONSUMER_GROUP, *msg_ids)
                    log.info(
                        f"Inserted {inserted}/{len(batch)} docs, "
                        f"ACK'd {len(msg_ids)} messages"
                    )
                    # Clear only after confirmed success
                    batch = []
                    msg_ids = []
                    last_flush = loop.time()
                except Exception as e:
                    log.error(
                        f"Batch insert failed: {e} — "
                        f"keeping {len(batch)} messages in local batch, "
                        f"they remain in PEL for retry"
                    )
                    # Do NOT clear batch or msg_ids — retry on next iteration
                    # XAUTOCLAIM janitor will reclaim after 60s if this worker dies
                    last_flush = loop.time()  # reset timer to avoid tight retry loop

        except Exception as e:
            log.error(f"Worker loop error: {e}")
            await asyncio.sleep(1)


# ── Janitor Loop ──────────────────────────────────────────────────────────────

async def janitor_loop(redis: Redis):
    """
    Periodic maintenance task — reclaims stuck messages from crashed workers.

    Runs every 30 seconds. Uses XAUTOCLAIM to transfer ownership of messages
    that have been pending for > 60 seconds (min_idle_time=60000ms).

    If a worker crashes between XREADGROUP and XACK, the message stays in the
    Pending Entries List (PEL) indefinitely. Without the janitor, those logs
    would be permanently lost. This is the zero-data-loss guarantee.

    For each reclaimed message, the janitor checks the delivery count via
    XPENDING. If a message has been delivered > MAX_RETRIES times, it's a
    "poison pill" — something about it causes every worker to fail. These
    are routed to the DLQ to prevent infinite retry loops.

    XAUTOCLAIM parameters:
      - min_idle_time=60000: only claim messages pending > 60 seconds
      - start_id="0-0": scan from the beginning of the PEL
      - count=100: process up to 100 stuck messages per cycle
    """
    log.info("Janitor started")
    loop = asyncio.get_running_loop()   # fix #12

    while True:
        await asyncio.sleep(30)
        try:
            # XAUTOCLAIM returns: [next_start_id, [(msg_id, data), ...], [deleted_ids]]
            result = await redis.xautoclaim(
                MAIN_STREAM,
                CONSUMER_GROUP,
                CONSUMER_NAME,
                min_idle_time=60_000,
                start_id="0-0",
                count=100,
            )
            claimed = result[1] if result and len(result) > 1 else []
            if claimed:
                log.info(f"Janitor reclaimed {len(claimed)} stuck messages")

                # Check delivery count on reclaimed messages — route to DLQ if exceeded
                for msg_id, entry in claimed:
                    # XPENDING with range [msg_id, msg_id] returns info for this message
                    pending_info = await redis.xpending_range(
                        MAIN_STREAM, CONSUMER_GROUP,
                        min=msg_id, max=msg_id, count=1
                    )
                    times_delivered = (
                        pending_info[0]["times_delivered"] if pending_info else 1
                    )
                    if times_delivered > MAX_RETRIES:
                        await route_to_dlq(
                            redis, msg_id, entry,
                            f"max_retries ({MAX_RETRIES}) exceeded after reclaim"
                        )

        except Exception as e:
            log.error(f"Janitor error: {e}")

        # ── DLQ depth alert ───────────────────────────────────────────────────
        try:
            dlq_depth = await redis.xlen(DLQ_STREAM)
            if dlq_depth >= ALERT_DLQ_THRESHOLD:
                hostname = socket.gethostname()
                subject = f"[Log Ingestion] DLQ Alert — {dlq_depth} failed messages"
                body = (
                    f"WARNING: Dead-Letter Queue depth has reached {dlq_depth}\n"
                    f"Threshold : {ALERT_DLQ_THRESHOLD}\n"
                    f"Host      : {hostname}\n\n"
                    f"Action    : Check /admin/dlq and investigate failed messages.\n"
                    f"Time      : {datetime.now(timezone.utc).isoformat()}\n"
                )
                send_email_alert(subject, body)
                log.warning(f"DLQ depth {dlq_depth} >= threshold {ALERT_DLQ_THRESHOLD} — alert triggered")
        except Exception as e:
            log.error(f"DLQ depth check error: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────────

async def main():
    """
    Initialize connections and run worker + janitor concurrently.

    Both tasks run in the same asyncio event loop via asyncio.gather().
    If either task fails fatally, the container restarts via Docker's
    restart: unless-stopped policy.

    Redis: single shared connection with decode_responses=True
    MongoDB: Motor async client with connection pool capped at 5 (fix #13)
    Consumer group: created idempotently on startup (same as FastAPI lifespan)
    """
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    # fix #13: cap Motor connection pool — 5 per worker is sufficient
    # Total MongoDB connections: 10 (FastAPI) + 5×2 (2 workers) = 20
    mongo_client = AsyncIOMotorClient(
        MONGODB_URL,
        maxPoolSize=5,
        minPoolSize=1,
    )
    col = mongo_client["logsdb"]["logs"]

    # Create consumer group if it doesn't exist (idempotent)
    # Same group created by FastAPI lifespan — whichever starts first wins
    try:
        await redis.xgroup_create(
            MAIN_STREAM, CONSUMER_GROUP, id="0", mkstream=True
        )
    except Exception:
        pass  # Group already exists — expected on every restart

    # Run both loops concurrently — gather() propagates exceptions
    await asyncio.gather(
        worker_loop(redis, col),
        janitor_loop(redis),
    )


if __name__ == "__main__":
    asyncio.run(main())
