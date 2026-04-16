"""
Tracing middleware for the log ingestion API.

Generates a UUID4 trace ID for every request, or accepts an existing one
from the X-Trace-ID header. The trace ID follows the log through the
entire pipeline: FastAPI → Redis Stream → Worker → MongoDB document.

The trace ID is always echoed back in the response header so the Android
SDK can log it for client-side correlation.
"""

import uuid

from starlette.middleware.base import BaseHTTPMiddleware


class TracingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        # Accept existing trace ID from client or generate a new one
        trace_id = request.headers.get("X-Trace-ID") or str(uuid.uuid4())
        request.state.trace_id = trace_id

        response = await call_next(request)

        # Always echo trace ID back — Android SDK must log this for correlation
        response.headers["X-Trace-ID"] = trace_id
        return response
