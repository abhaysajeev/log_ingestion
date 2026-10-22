"""
Dashboard profiles: the filters and columns a project's dashboard shows.

A project names its profile in projects.json. The dashboard asks for it
(/p/<slug>/api/meta) and draws itself from it, so a new kind of project needs
only a new entry here -- no dashboard change.

Only filters declared here ever reach MongoDB: build_query() turns each known
filter into a fixed query shape, escapes text, and ignores any other
parameter. A dashboard user cannot query a field the profile does not offer.

Every query is fenced to one project and sorted on (received_at, _id) -- the
`project_newest` index -- so the newest page is an index walk, not a sort.

Filter kinds:
  select        exact match on one of `options`
  status_class  "2xx" / "4xx" / ... on a numeric status field
  exact         exact string match (case kept)
  iexact        whole-string match, any case
  contains      substring, any case
  min_number    field >= the number given
  device        installation id, or the numeric device number
  pills         one of `options`, drawn as buttons above the filters; "all"
                (or nothing) = every value. `missing` names the option that
                also matches logs without the field. `options` is the
                allow-list and the order; the dashboard shows only those the
                project's own logs carry (main.py /api/meta), so each project
                sees just its own environments.
"""

import re
from datetime import datetime, timedelta, timezone

STATUS_CLASSES = {"2xx": (200, 299), "3xx": (300, 399), "4xx": (400, 499), "5xx": (500, 599)}

PROFILES = {
    # BYKY's own device API requests (byky_django apps/monitoring/shipper.py).
    "byky": {
        "title": "Device requests",
        "list_exclude": ["form_data", "log.response"],
        "filters": [
            # Which BYKY copy sent the log: qa (:8007, older app builds), dev
            # (:8008) or live (production) -- byky_django LOG_ENVIRONMENT.
            # Logs sent before it was added (14 Oct 2026) carry none: they are
            # qa's. live ships to its own project (byky-live), so byky-qa's
            # switch shows qa | dev and byky-live's shows live: each draws only
            # the values its own logs carry (present_pills below).
            {"key": "env", "label": "Environment", "kind": "pills", "field": "log.environment",
             "options": ["qa", "dev", "live"], "default": "qa", "missing": "qa"},
            {"key": "request_id", "label": "Request ID", "kind": "exact", "field": "log.request_id",
             "placeholder": "X-Request-ID", "wide": True},
            {"key": "app", "label": "App", "kind": "select", "field": "log.app",
             "options": ["operator", "manager", "employee"]},
            {"key": "status", "label": "Status", "kind": "status_class", "field": "log.status",
             "options": list(STATUS_CLASSES)},
            {"key": "code", "label": "Code", "kind": "exact", "field": "log.code",
             "placeholder": "e.g. invalid_credentials", "suggest": True},
            {"key": "path", "label": "Endpoint", "kind": "contains", "field": "log.path",
             "placeholder": "e.g. auth/login", "suggest": True},
            {"key": "device", "label": "Device", "kind": "device", "field": "device_id",
             "number_field": "log.device_no", "placeholder": "Device no. or installation ID"},
            {"key": "user", "label": "User", "kind": "iexact", "field": "log.username",
             "placeholder": "Username", "suggest": True},
            {"key": "branch", "label": "Station", "kind": "contains", "field": "log.branch",
             "placeholder": "Station name", "suggest": True},
            {"key": "slow", "label": "Slower than (ms)", "kind": "min_number", "field": "log.duration_ms",
             "placeholder": "e.g. 1000"},
        ],
        "columns": [
            {"label": "Time", "path": "log.at", "format": "time"},
            {"label": "Env", "path": "log.environment", "format": "tag", "fallback_text": "qa"},
            {"label": "App", "path": "log.app", "format": "tag"},
            {"label": "Request", "path": "log.path", "format": "request", "method": "log.method"},
            {"label": "Status", "path": "log.status", "format": "status"},
            {"label": "Code", "path": "log.code", "format": "code"},
            {"label": "Time taken", "path": "log.duration_ms", "format": "ms"},
            {"label": "Device", "path": "log.device_no", "format": "device", "fallback": "device_id"},
            {"label": "User", "path": "log.username", "format": "text"},
            {"label": "Station", "path": "log.branch", "format": "text"},
        ],
        "detail": [
            {"label": "Request", "path": "form_data", "format": "json"},
            {"label": "Response", "path": "log.response", "format": "json"},
            {"label": "Details", "path": "log", "format": "fields",
             "omit": ["response"]},
        ],
    },
    # Anything else: the original generic view.
    "generic": {
        "title": "Logs",
        "filters": [
            {"key": "device", "label": "Device", "kind": "exact", "field": "device_id",
             "placeholder": "Device ID", "suggest": True},
            {"key": "trace", "label": "Trace ID", "kind": "exact", "field": "trace_id",
             "placeholder": "Trace ID", "wide": True},
        ],
        "columns": [
            {"label": "Received", "path": "received_at", "format": "time"},
            {"label": "Device", "path": "device_id", "format": "text"},
            {"label": "Form data", "path": "form_data", "format": "summary"},
            {"label": "Log", "path": "log", "format": "summary"},
        ],
        "detail": [
            {"label": "Form data", "path": "form_data", "format": "json"},
            {"label": "Log", "path": "log", "format": "json"},
        ],
    },
}


class BadFilter(ValueError):
    """A filter value that cannot be used (not a number, unknown option)."""


def pills_fields(profile_name: str) -> dict:
    """key -> field of each pills filter, for /api/meta to look up which values
    a project's logs carry."""
    return {f["key"]: f["field"] for f in PROFILES[profile_name]["filters"] if f["kind"] == "pills"}


def present_pills(spec: dict, values, has_missing: bool):
    """A pills filter cut down to the options a project's logs carry, in the
    declared order -- or None (no switch at all) when it carries none. The
    default falls back to the first option present."""
    present = {v for v in values if isinstance(v, str)}
    if has_missing and spec.get("missing"):
        present.add(spec["missing"])
    options = [o for o in spec["options"] if o in present]
    if not options:
        return None
    default = spec.get("default")
    return {**spec, "options": options, "default": default if default in options else options[0]}


def public(profile_name: str, present: dict | None = None) -> dict:
    """What the dashboard needs to draw itself -- no field paths to query with.

    `present` maps a pills filter's key to (values seen, any log without the
    field) for one project; a pills filter not in it keeps every option."""
    profile = PROFILES[profile_name]
    filters = []
    for spec in profile["filters"]:
        if spec["kind"] == "pills" and present and spec["key"] in present:
            spec = present_pills(spec, *present[spec["key"]])
            if spec is None:
                continue
        filters.append({k: v for k, v in spec.items() if k not in ("field", "number_field")})
    return {
        "title": profile["title"],
        "filters": filters,
        "columns": profile["columns"],
        "detail": profile["detail"],
    }


def _day_start(text: str, *, end: bool = False):
    """'2026-09-29' (a whole day, in UTC+4 -- the dashboard's zone) or a full
    ISO datetime. `end` moves a bare day to the start of the next one."""
    try:
        if len(text) == 10:
            day = datetime.fromisoformat(text).replace(tzinfo=timezone(timedelta(hours=4)))
            return day + timedelta(days=1) if end else day
        value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    except ValueError as error:
        raise BadFilter(f"'{text}' is not a date") from error


def build_query(profile_name: str, project_slug: str, params) -> dict:
    """The MongoDB filter for one project's page. Unknown parameters are ignored."""
    query: dict = {"project_id": project_slug}
    when = {}
    if params.get("from"):
        when["$gte"] = _day_start(params["from"])
    if params.get("to"):
        when["$lt"] = _day_start(params["to"], end=True)
    if when:
        query["received_at"] = when

    clauses = []
    for spec in PROFILES[profile_name]["filters"]:
        value = (params.get(spec["key"]) or "").strip()
        if not value:
            continue
        field, kind = spec["field"], spec["kind"]
        if kind == "pills":
            if value == "all":
                continue
            if value not in spec["options"]:
                raise BadFilter(f"{spec['label']}: unknown value '{value}'")
            # The `missing` option also matches logs that never carried the
            # field ($in with None matches a missing field too).
            if spec.get("missing") == value:
                clauses.append({field: {"$in": [value, None]}})
            else:
                clauses.append({field: value})
        elif kind == "select":
            if value not in spec["options"]:
                raise BadFilter(f"{spec['label']}: unknown value '{value}'")
            clauses.append({field: value})
        elif kind == "status_class":
            if value not in STATUS_CLASSES:
                raise BadFilter(f"{spec['label']}: unknown value '{value}'")
            low, high = STATUS_CLASSES[value]
            clauses.append({field: {"$gte": low, "$lte": high}})
        elif kind == "exact":
            clauses.append({field: value})
        elif kind == "iexact":
            clauses.append({field: {"$regex": f"^{re.escape(value)}$", "$options": "i"}})
        elif kind == "contains":
            clauses.append({field: {"$regex": re.escape(value), "$options": "i"}})
        elif kind == "min_number":
            if not value.isdigit():
                raise BadFilter(f"{spec['label']}: a whole number, please")
            clauses.append({field: {"$gte": int(value)}})
        elif kind == "device":
            either = [{field: value}]
            if value.isdigit():
                either.append({spec["number_field"]: int(value)})
            clauses.append({"$or": either})
    if clauses:
        query["$and"] = clauses
    return query


def suggest_field(profile_name: str, key: str):
    """The field a filter suggests values from, or None if it does not."""
    for spec in PROFILES[profile_name]["filters"]:
        if spec["key"] == key and spec.get("suggest"):
            return spec["field"]
    return None
