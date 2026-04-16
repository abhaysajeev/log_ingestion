"""
Pydantic models for the log ingestion API.

LogEntry: A single log event sent by a device. Matches the client's actual
payload format — project_id, device_id, form_data (JSON), log (JSON).

The backend does NOT define what clients put inside form_data or log —
those are free-form JSON objects stored raw without validation.

BatchIngestPayload: The top-level request body for POST /ingest/batch.
Max 100 logs per batch (enforced in the route, not here, to return a
friendly error message instead of a Pydantic validation error).
"""

from typing import Any, Optional

from pydantic import BaseModel, field_validator


class LogEntry(BaseModel):
    project_id: str                         # which project this log belongs to
    device_id: str                          # which device sent this log
    form_data: dict[str, Any] = {}          # free-form JSON — client-defined
    log: dict[str, Any] = {}                # free-form JSON — client-defined

    @field_validator("project_id")
    @classmethod
    def project_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("project_id must not be empty")
        return v.strip()

    @field_validator("device_id")
    @classmethod
    def device_id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("device_id must not be empty")
        return v.strip()


class BatchIngestPayload(BaseModel):
    logs: list[LogEntry]
