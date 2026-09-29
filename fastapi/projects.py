"""
Projects: who may write logs, and who may read them.

Each project has its own
  - ingest_key       -- sent as X-API-Key by the project's server; it decides
                        the project a log is stored under (a key can only ever
                        write into its own project, whatever the body says)
  - dashboard_token  -- opens that project's dashboard at /p/<slug>/ and only
                        that project's logs
  - profile          -- which filters and columns its dashboard shows
                        (profiles.py)

They live in a JSON file (PROJECTS_FILE, default /app/config/projects.json),
mounted read-only and kept out of git like .env:

  {
    "byky-qa": {
      "name": "BYKY QA",
      "profile": "byky",
      "ingest_key": "<64 hex chars>",
      "dashboard_token": "<64 hex chars>"
    }
  }

scripts/add_project.py writes an entry with fresh secrets. The file is read at
start-up: restart the fastapi service after changing it.
"""

import hmac
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path

from profiles import PROFILES

PROJECTS_FILE = Path(os.getenv("PROJECTS_FILE", "/app/config/projects.json"))
SLUG = re.compile(r"^[a-z0-9][a-z0-9-]{0,39}$")
# The ingest key is machine to machine: long and random. A dashboard token may
# be anything a person chose (scripts/add_project.py --token) -- only never
# empty, which would open the dashboard to everyone. Guessing a short one is
# slowed by nginx's per-IP limit on /p/ (nginx/*.conf).
MIN_INGEST_KEY_LENGTH = 32


@dataclass(frozen=True)
class Project:
    slug: str
    name: str
    profile: str
    ingest_key: str
    dashboard_token: str


def load(path: Path = PROJECTS_FILE) -> dict[str, Project]:
    """Every project in the file, checked. A bad file stops start-up with a
    message saying what is wrong, rather than running half-configured."""
    if not path.exists():
        raise SystemExit(f"{path} is missing -- create it with scripts/add_project.py")
    raw = json.loads(path.read_text())
    projects, keys = {}, set()
    for slug, entry in raw.items():
        if not SLUG.match(slug):
            raise SystemExit(f"project '{slug}': use lowercase letters, digits and dashes (max 40)")
        project = Project(
            slug=slug,
            name=str(entry.get("name") or slug),
            profile=str(entry.get("profile") or "generic"),
            ingest_key=str(entry.get("ingest_key") or ""),
            dashboard_token=str(entry.get("dashboard_token") or ""),
        )
        if project.profile not in PROFILES:
            raise SystemExit(f"project '{slug}': unknown profile '{project.profile}' "
                             f"(one of {', '.join(sorted(PROFILES))})")
        if len(project.ingest_key) < MIN_INGEST_KEY_LENGTH:
            raise SystemExit(f"project '{slug}': ingest_key must be at least {MIN_INGEST_KEY_LENGTH} characters")
        if not project.dashboard_token:
            raise SystemExit(f"project '{slug}': dashboard_token is empty -- the dashboard would be open to anyone")
        for label, secret in (("ingest_key", project.ingest_key), ("dashboard_token", project.dashboard_token)):
            if secret in keys:
                raise SystemExit(f"project '{slug}': {label} is used twice -- every key and token must be unique")
            keys.add(secret)
        projects[slug] = project
    if not projects:
        raise SystemExit(f"{path} has no projects")
    return projects


def by_ingest_key(projects: dict[str, Project], key: str):
    """The project this ingest key belongs to, or None. Constant-time compare."""
    found = None
    for project in projects.values():
        if hmac.compare_digest(project.ingest_key.encode(), (key or "").encode()):
            found = project
    return found


def token_opens(project: Project, token: str) -> bool:
    return hmac.compare_digest(project.dashboard_token.encode(), (token or "").encode())
