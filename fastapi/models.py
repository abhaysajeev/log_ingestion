"""
Request bodies for POST /ingest/batch.

The project a log is stored under comes from the API key, never from the body:
`project_id` is still accepted (older senders post it) but ignored.
`form_data` and `log` are free-form JSON, stored as sent -- except what
MongoDB could never store, refused here with 422 rather than accepted and
lost later: nesting deeper than MAX_DEPTH, or an integer outside 64 bits.
"""

from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator

MAX_BATCH = 100
MAX_DEPTH = 20                       # MongoDB's own limit is 100 levels
INT64_MIN, INT64_MAX = -(2 ** 63), 2 ** 63 - 1


def check_storable(value: Any, depth: int = 1) -> None:
    """Raise ValueError for what BSON/MongoDB can't take. One pass over the
    value; the depth cap also bounds the recursion."""
    if depth > MAX_DEPTH:
        raise ValueError(f"nested deeper than {MAX_DEPTH} levels")
    if isinstance(value, dict):
        for item in value.values():
            check_storable(item, depth + 1)
    elif isinstance(value, list):
        for item in value:
            check_storable(item, depth + 1)
    elif isinstance(value, int) and not isinstance(value, bool) and not INT64_MIN <= value <= INT64_MAX:
        raise ValueError("an integer is outside the 64-bit range")


class LogEntry(BaseModel):
    project_id: Optional[str] = None        # ignored: the API key decides the project
    device_id: str
    form_data: dict[str, Any] = {}
    log: dict[str, Any] = {}

    @field_validator("device_id")
    @classmethod
    def device_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("device_id must not be empty")
        return v.strip()[:200]

    @field_validator("form_data", "log")
    @classmethod
    def storable(cls, v: dict) -> dict:
        check_storable(v)
        return v


class BatchIngestPayload(BaseModel):
    logs: list[LogEntry] = Field(max_length=MAX_BATCH)
