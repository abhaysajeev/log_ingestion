#!/usr/bin/env python3
"""
Add a project (or give an existing one new secrets) in config/projects.json.

  python3 scripts/add_project.py byky-qa "BYKY QA" --profile byky \
      --base-url https://logs.softlandindia.net

Prints the project's ingest key (for the sending server's .env) and its
dashboard link (for the people reading the logs). Restart the fastapi service
afterwards:  sudo docker compose restart fastapi

  --rotate   new secrets for an existing project: the old link and key stop
             working after the restart.
"""

import argparse
import json
import os
import re
import secrets
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILE = ROOT / "config" / "projects.json"
PROFILES = ("byky", "generic")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("slug", help="lowercase letters, digits and dashes, e.g. byky-qa")
    parser.add_argument("name", nargs="?", help="shown on the dashboard, e.g. 'BYKY QA'")
    parser.add_argument("--profile", choices=PROFILES, default="generic")
    parser.add_argument("--base-url", default="https://logs.softlandindia.net")
    parser.add_argument("--rotate", action="store_true", help="replace an existing project's secrets")
    args = parser.parse_args()

    if not re.match(r"^[a-z0-9][a-z0-9-]{0,39}$", args.slug):
        sys.exit("slug: lowercase letters, digits and dashes (max 40)")
    projects = json.loads(FILE.read_text()) if FILE.exists() else {}
    if args.slug in projects and not args.rotate:
        sys.exit(f"'{args.slug}' exists already -- add --rotate to give it new secrets")

    entry = projects.get(args.slug, {})
    entry.update({
        "name": args.name or entry.get("name") or args.slug,
        "profile": args.profile if (args.profile != "generic" or "profile" not in entry) else entry["profile"],
        "ingest_key": secrets.token_hex(32),
        "dashboard_token": secrets.token_hex(32),
    })
    projects[args.slug] = entry

    FILE.parent.mkdir(exist_ok=True)
    FILE.write_text(json.dumps(projects, indent=2) + "\n")
    os.chmod(FILE, 0o600)

    print(f"Project     : {args.slug} ({entry['name']}, profile {entry['profile']})")
    print(f"Ingest key  : {entry['ingest_key']}")
    print(f"Dashboard   : {args.base_url.rstrip('/')}/p/{args.slug}/#token={entry['dashboard_token']}")
    print("\nRestart to apply:  sudo docker compose restart fastapi")


if __name__ == "__main__":
    main()
