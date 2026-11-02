"""
Tracing middleware for the log ingestion API.

Generates a UUID4 trace ID for every request, or accepts an existing one
from the X-Trace-ID header. The trace ID follows the log through the
entire pipeline: FastAPI → Redis Stream → Worker → MongoDB document.

The trace ID is always echoed back in the response header so the Android
SDK can log it for client-side correlation.

Implemented as a pure ASGI middleware (not BaseHTTPMiddleware) to avoid
Starlette's response-body buffering, which serialises requests under high
concurrency.
"""

import uuid

MAX_TRACE_ID = 64


def trace_id_from(raw: bytes | None) -> str:
    """The client's X-Trace-ID, cleaned: undecodable bytes replaced, at most
    MAX_TRACE_ID characters. A missing or blank one gets a fresh uuid4."""
    value = raw.decode("utf-8", errors="replace").strip()[:MAX_TRACE_ID] if raw else ""
    return value or str(uuid.uuid4())


class TracingMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        headers_map = {k: v for k, v in scope.get("headers", [])}
        trace_id = trace_id_from(headers_map.get(b"x-trace-id"))

        scope.setdefault("state", {})
        scope["state"]["trace_id"] = trace_id

        trace_header = (b"x-trace-id", trace_id.encode())

        async def send_with_trace(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                headers.append(trace_header)
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_trace)
