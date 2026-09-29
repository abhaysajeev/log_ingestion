"""
Request bodies for POST /ingest/batch.

The project a log is stored under comes from the API key, never from the body:
`project_id` is still accepted (older senders post it) but ignored.
`form_data` and `log` are free-form JSON, stored as sent.
"""

from typing import Any, Optional

from pydantic import BaseModel, field_validator

MAX_BATCH = 100


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


class BatchIngestPayload(BaseModel):
    logs: list[LogEntry]
