"""
Async log ingestion worker -- consumes the Redis stream, stores logs in MongoDB.

Two concurrent tasks:

1. worker_loop  -- on start, finishes this consumer's own unacknowledged
                   messages (a restart of the same container); then reads new
                   ones, batches them and stores them every BATCH_SIZE messages
                   or FLUSH_INTERVAL seconds.
2. janitor_loop -- every 30 s takes over messages another consumer left
                   pending for over a minute (a crashed or recreated worker)
                   and stores them the same way.

A stored, duplicate or dead-lettered message is XACK'd **and XDEL'd**: the
stream only ever holds logs not yet in MongoDB. (Until 2 Nov 2026 messages were
only XACK'd; the stream kept every log ever sent until Redis's 128 MB filled
and every ingest failed.)

Failures:
  - MongoDB down or slow: the batch is kept, nothing new is read, and the same
    batch is retried with backoff (1 s .. 30 s). Memory stays flat; nothing is
    dead-lettered for waiting.
  - A log MongoDB can never store (too deep, int too big, ...): only that log
    goes to the `dead_letters` collection; the rest of its batch is stored.
  - A message that keeps crashing workers: dead-lettered once it has been
    delivered more than MAX_DELIVERIES times.

Every log's MongoDB _id is derived from its stream id (stream_object_id), so a
message stored twice -- a redelivery, a takeover, a replay -- is one document.

Document written to `logs`:
  {
    _id:          ObjectId from the stream id
    trace_id:     "uuid4"              -- from FastAPI middleware
    received_at:  datetime (UTC)       -- server ingestion time (authoritative)
    project_id:   "byky-qa"            -- from the API key
    device_id:    "device_abc"
    form_data:    { ... }              -- free-form JSON, client-defined
    log:          { ... }              -- free-form JSON, client-defined
  }
"""

import asyncio
import hashlib
import json
import logging
import os
import smtplib
import socket
import struct
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText

from bson import ObjectId
from bson.errors import InvalidDocument
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError, DuplicateKeyError, WriteError
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
MONGO_DB       = os.getenv("MONGO_DB", "logsdb")
BATCH_SIZE     = int(os.getenv("WORKER_BATCH_SIZE", "500"))
FLUSH_INTERVAL = float(os.getenv("WORKER_FLUSH_INTERVAL_SECONDS", "5"))
MAX_DELIVERIES = int(os.getenv("WORKER_MAX_RETRIES", "3"))
MAIN_STREAM    = os.getenv("MAIN_STREAM", "logs_stream")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "workers")
CLAIM_IDLE_MS  = int(os.getenv("WORKER_CLAIM_IDLE_MS", "60000"))
JANITOR_EVERY  = float(os.getenv("WORKER_JANITOR_SECONDS", "30"))
BACKOFF_MAX    = float(os.getenv("WORKER_BACKOFF_MAX_SECONDS", "30"))
HEARTBEAT_TTL  = 30          # /health counts live workers by these keys
HEARTBEAT_EVERY = 10

# ── Email Alert Configuration ─────────────────────────────────────────────────
SMTP_HOST           = os.getenv("SMTP_HOST", "")
SMTP_PORT           = int(os.getenv("SMTP_PORT", "587"))   # 587=STARTTLS, 465=SSL
SMTP_USER           = os.getenv("SMTP_USER", "")           # sender email
SMTP_PASSWORD       = os.getenv("SMTP_PASSWORD", "")
ALERT_EMAIL_TO      = os.getenv("ALERT_EMAIL_TO", "")      # recipient(s), comma-sep
ALERT_DLQ_THRESHOLD = int(os.getenv("ALERT_DLQ_THRESHOLD", "10"))

# Unique per container: two replicas on one host never share a pending list.
CONSUMER_NAME = f"worker-{socket.gethostname()}-{os.getpid()}"
HEARTBEAT_KEY = f"worker:hb:{CONSUMER_NAME}"

# Dead-letter reasons. Only REPLAYABLE ones are worth sending again.
PARSE_ERROR      = "parse_error"
INVALID_DOCUMENT = "invalid_document"
MAX_DELIVERED    = "max_deliveries"

# Set while a batch keeps failing to store: the janitor waits too, or it would
# take over this worker's own waiting messages and count them as failures.
_storing_fails = False


# ── Email Alert ───────────────────────────────────────────────────────────────

_last_alert_time: float = 0.0   # throttle -- at most one alert per 30 min


def send_email_alert(subject: str, body: str) -> None:
    """Send an alert email via SMTP. Silently skips if SMTP is not configured.
    Blocking: call it through asyncio.to_thread."""
    global _last_alert_time

    if not SMTP_HOST or not SMTP_USER or not ALERT_EMAIL_TO:
        return

    now = time.time()
    if now - _last_alert_time < 1800:
        log.debug("Email alert throttled -- skipping")
        return
    _last_alert_time = now

    recipients = [r.strip() for r in ALERT_EMAIL_TO.split(",") if r.strip()]
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ", ".join(recipients)

    try:
        if SMTP_PORT == 465:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, recipients, msg.as_string())
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
                server.ehlo()
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, recipients, msg.as_string())
        log.info(f"Alert email sent to {recipients}")
    except Exception as exc:
        log.warning(f"Alert email failed: {exc}")


# ── Documents ─────────────────────────────────────────────────────────────────

def stream_object_id(stream_id: str) -> ObjectId:
    """The ObjectId a stream message is stored under: its stream time (so the
    dashboard's newest-first order holds) plus 8 bytes of a hash of the whole
    id. The same message always maps to the same _id."""
    ms = int(stream_id.split("-", 1)[0])
    digest = hashlib.blake2b(stream_id.encode(), digest_size=8).digest()
    return ObjectId(struct.pack(">I", (ms // 1000) & 0xFFFFFFFF) + digest)


def origin_id(msg_id: str, entry: dict) -> str:
    """The id the log was first queued under -- a replayed dead letter carries
    it as origin_id, so it is still stored as the same document."""
    return entry.get("origin_id") or msg_id


def build_document(msg_id: str, entry: dict) -> dict:
    """A MongoDB document from a stream entry {trace_id, received_at, data}.
    Raises on anything malformed (a parse error: dead-lettered)."""
    raw_data = json.loads(entry["data"])
    if not isinstance(raw_data, dict):
        raise ValueError("data is not a JSON object")
    received_ts = float(entry["received_at"])
    return {
        "_id":         stream_object_id(origin_id(msg_id, entry)),
        "trace_id":    entry.get("trace_id"),
        "received_at": datetime.fromtimestamp(received_ts, tz=timezone.utc),
        "project_id":  raw_data.get("project_id"),
        "device_id":   raw_data.get("device_id"),
        "form_data":   raw_data.get("form_data", {}),
        "log":         raw_data.get("log", {}),
    }


# ── Dead letters (MongoDB) ────────────────────────────────────────────────────

async def dead_letter(dead, msg_id: str, entry: dict, reason: str, error: str = "") -> None:
    """Keep a message that can't be stored in `dead_letters`, keyed by its
    original stream id (a second failure of the same message is one row).
    Raises if MongoDB can't take it -- the caller then leaves the message
    pending rather than lose it."""
    sid = origin_id(msg_id, entry)
    project = None
    try:
        project = json.loads(entry.get("data") or "{}").get("project_id")
    except Exception:
        pass
    await dead.replace_one(
        {"stream_id": sid},
        {
            "stream_id":  sid,
            "project_id": project,
            "reason":     reason,
            "error":      str(error)[:2000],
            "entry":      {k: v for k, v in entry.items() if k != "origin_id"},
            "at":         datetime.now(timezone.utc),
        },
        upsert=True,
    )
    log.warning(f"Dead-lettered {msg_id}: {reason} {str(error)[:200]}")


# ── Acknowledge ───────────────────────────────────────────────────────────────

async def done(redis: Redis, msg_ids: list[str]) -> None:
    """XACK + XDEL in one round trip: the message is finished and leaves the
    stream (and Redis memory)."""
    if not msg_ids:
        return
    pipe = redis.pipeline(transaction=False)
    pipe.xack(MAIN_STREAM, CONSUMER_GROUP, *msg_ids)
    pipe.xdel(MAIN_STREAM, *msg_ids)
    await pipe.execute()


# ── Storing ───────────────────────────────────────────────────────────────────

async def store(col, docs: list[dict]) -> dict[int, str]:
    """Insert `docs`; returns {index: error} for documents MongoDB can never
    store. Duplicates (11000) count as stored. Raises on anything that may
    work later (MongoDB down, a write-concern error) -- nothing is lost, the
    caller retries the same documents."""
    if not docs:
        return {}
    try:
        await col.insert_many(docs, ordered=False)
        return {}
    except BulkWriteError as e:
        if e.details.get("writeConcernErrors"):
            raise
        return {err["index"]: err.get("errmsg", "")
                for err in e.details.get("writeErrors", []) if err.get("code") != 11000}
    except (InvalidDocument, OverflowError):
        # Refused by the driver before anything was sent: find the bad ones.
        bad = {}
        for i, doc in enumerate(docs):
            try:
                await col.insert_one(doc)
            except DuplicateKeyError:
                pass
            except (InvalidDocument, OverflowError, WriteError) as err:
                bad[i] = str(err)
        return bad


async def store_messages(redis: Redis, col, dead, messages: list) -> int:
    """Store one read's messages, dead-letter what can never be stored, then
    ack + delete everything finished. Returns how many were stored.

    Raises (nothing acknowledged) if MongoDB is unavailable."""
    docs, doc_ids, finished = [], [], []
    for msg_id, entry in messages:
        if not entry:                      # deleted from the stream already
            finished.append(msg_id)
            continue
        try:
            docs.append(build_document(msg_id, entry))
            doc_ids.append((msg_id, entry))
        except Exception as e:
            try:
                await dead_letter(dead, msg_id, entry, PARSE_ERROR, repr(e))
                finished.append(msg_id)
            except Exception as dl_error:  # left pending; the janitor retries it
                log.error(f"Could not dead-letter {msg_id}: {dl_error}")

    bad = await store(col, docs)           # raises: caller keeps the batch
    for i, (msg_id, entry) in enumerate(doc_ids):
        if i in bad:
            try:
                await dead_letter(dead, msg_id, entry, INVALID_DOCUMENT, bad[i])
            except Exception as dl_error:
                log.error(f"Could not dead-letter {msg_id}: {dl_error}")
                continue
        finished.append(msg_id)
    await done(redis, finished)
    return len(docs) - len(bad)


# ── Heartbeat ─────────────────────────────────────────────────────────────────

_last_heartbeat = 0.0
HEARTBEAT_FILE = os.getenv("WORKER_HEARTBEAT_FILE", "/tmp/heartbeat")


async def heartbeat(redis: Redis) -> None:
    """The loop is alive: touch HEARTBEAT_FILE (the container healthcheck reads
    its age) and refresh worker:hb:<name> (/health counts these)."""
    global _last_heartbeat
    now = time.monotonic()
    if now - _last_heartbeat >= HEARTBEAT_EVERY:
        try:
            with open(HEARTBEAT_FILE, "w") as f:
                f.write(datetime.now(timezone.utc).isoformat())
        except OSError:
            pass
        _last_heartbeat = now
        await redis.set(HEARTBEAT_KEY, datetime.now(timezone.utc).isoformat(), ex=HEARTBEAT_TTL)


# ── Main consumer loop ────────────────────────────────────────────────────────

async def recover_own_pending(redis: Redis, col, dead) -> None:
    """Messages this consumer read but never finished (the container was
    restarted mid-batch): store them before reading anything new."""
    start = "0"
    while True:
        result = await redis.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {MAIN_STREAM: start}, count=BATCH_SIZE)
        entries = result[0][1] if result else []
        if not entries:
            return
        stored = await store_messages(redis, col, dead, entries)
        log.info(f"Recovered {stored}/{len(entries)} of this worker's pending messages")
        start = entries[-1][0]


async def worker_loop(redis: Redis, col, dead):
    """Read, batch, store. While storing fails, read nothing new and retry the
    same batch with exponential backoff."""
    global _storing_fails
    batch: list = []
    loop = asyncio.get_running_loop()
    last_flush = loop.time()
    failures = 0

    log.info(f"Worker {CONSUMER_NAME} started -- reading from {MAIN_STREAM}")
    while True:
        try:
            await heartbeat(redis)
            if failures == 0 and len(batch) < BATCH_SIZE:
                result = await redis.xreadgroup(
                    CONSUMER_GROUP, CONSUMER_NAME, {MAIN_STREAM: ">"},
                    count=BATCH_SIZE - len(batch), block=1000,
                )
                for _, entries in result or []:
                    batch.extend(entries)

            due = len(batch) >= BATCH_SIZE or loop.time() - last_flush >= FLUSH_INTERVAL
            if batch and (due or failures):
                try:
                    stored = await store_messages(redis, col, dead, batch)
                except Exception as e:
                    failures += 1
                    _storing_fails = True
                    wait = min(BACKOFF_MAX, 2 ** (failures - 1))
                    log.error(f"Storing {len(batch)} messages failed ({e}); retry {failures} in {wait:.0f}s")
                    await asyncio.sleep(wait)
                    continue
                log.info(f"Stored {stored}/{len(batch)} messages")
                batch, failures, _storing_fails = [], 0, False
                last_flush = loop.time()
            elif not batch:
                last_flush = loop.time()
        except Exception as e:                 # Redis trouble: keep the batch, try again
            log.error(f"Worker loop error: {e}")
            await asyncio.sleep(1)


# ── Janitor loop ──────────────────────────────────────────────────────────────

async def take_over_stuck(redis: Redis, col, dead) -> int:
    """XAUTOCLAIM messages pending over CLAIM_IDLE_MS with any consumer, and
    store them. A message delivered more than MAX_DELIVERIES times is the one
    that keeps killing workers: dead-letter it instead. Follows XAUTOCLAIM's
    cursor across the whole pending list, at most 10 pages per run."""
    cursor, handled = "0-0", 0
    for _ in range(10):
        result = await redis.xautoclaim(MAIN_STREAM, CONSUMER_GROUP, CONSUMER_NAME,
                                        min_idle_time=CLAIM_IDLE_MS, start_id=cursor, count=100)
        cursor = result[0] if result else "0-0"
        claimed = [(i, e) for i, e in (result[1] if result and len(result) > 1 else [])]
        if claimed:
            pending = await redis.xpending_range(MAIN_STREAM, CONSUMER_GROUP, min=claimed[0][0],
                                                 max=claimed[-1][0], count=len(claimed),
                                                 consumername=CONSUMER_NAME)
            deliveries = {p["message_id"]: p["times_delivered"] for p in pending}
            normal, finished = [], []
            for msg_id, entry in claimed:
                if entry and deliveries.get(msg_id, 1) > MAX_DELIVERIES:
                    try:
                        await dead_letter(dead, msg_id, entry, MAX_DELIVERED,
                                          f"delivered {deliveries[msg_id]} times")
                        finished.append(msg_id)
                    except Exception as e:
                        log.error(f"Could not dead-letter {msg_id}: {e}")
                else:
                    normal.append((msg_id, entry))
            await done(redis, finished)
            if normal:
                await store_messages(redis, col, dead, normal)   # raises if MongoDB is down
            handled += len(claimed)
            log.info(f"Janitor took over {len(claimed)} stuck messages")
        if cursor in ("0-0", "0"):
            break
    return handled


async def alert_on_dead_letters(dead, since: datetime) -> datetime:
    """Email when at least ALERT_DLQ_THRESHOLD new dead letters arrived since
    the last check (growth, not depth: an old pile doesn't email forever)."""
    now = datetime.now(timezone.utc)
    new = await dead.count_documents({"at": {"$gte": since}})
    if new >= ALERT_DLQ_THRESHOLD:
        subject = f"[Log Ingestion] {new} logs could not be stored"
        body = (
            f"{new} logs were moved to dead_letters since {since.isoformat()}.\n"
            f"Threshold : {ALERT_DLQ_THRESHOLD}\n"
            f"Host      : {socket.gethostname()}\n\n"
            f"Action    : check /admin/dlq.\n"
        )
        await asyncio.to_thread(send_email_alert, subject, body)
        log.warning(f"{new} new dead letters >= {ALERT_DLQ_THRESHOLD} -- alert triggered")
    return now


async def janitor_loop(redis: Redis, col, dead):
    log.info("Janitor started")
    since = datetime.now(timezone.utc)
    while True:
        await asyncio.sleep(JANITOR_EVERY)
        if _storing_fails:
            continue                            # MongoDB is down: nothing to gain
        try:
            await take_over_stuck(redis, col, dead)
        except Exception as e:
            log.error(f"Janitor error: {e}")
        try:
            since = await alert_on_dead_letters(dead, since)
        except Exception as e:
            log.error(f"Dead-letter check error: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def connect():
    redis = Redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=10,
                           socket_connect_timeout=5, health_check_interval=30)
    mongo = AsyncIOMotorClient(MONGODB_URL, maxPoolSize=5, minPoolSize=1, serverSelectionTimeoutMS=5000)
    db = mongo[MONGO_DB]
    return redis, db["logs"], db["dead_letters"]


async def main():
    redis, col, dead = connect()
    try:
        await redis.xgroup_create(MAIN_STREAM, CONSUMER_GROUP, id="0", mkstream=True)
    except Exception:
        pass  # the group already exists
    while True:
        try:
            await recover_own_pending(redis, col, dead)
            break
        except Exception as e:                   # MongoDB not up yet: wait
            log.error(f"Recovering pending messages failed: {e}; retrying in 5s")
            await asyncio.sleep(5)
    await asyncio.gather(worker_loop(redis, col, dead), janitor_loop(redis, col, dead))


if __name__ == "__main__":
    asyncio.run(main())
