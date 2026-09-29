#!/usr/bin/env python3
"""
Add a project (or give an existing one new secrets) in config/projects.json.

  python3 scripts/add_project.py byky-qa "BYKY QA" --profile byky \
      --base-url https://logs.softlandindia.net

Prints the project's ingest key (for the sending server's .env) and its
dashboard link (for the people reading the logs). Restart the fastapi service
afterwards:  sudo docker compose restart fastapi

  --rotate         new secrets for an existing project: the old link and key
                   stop working after the restart.
  --token VALUE    set the dashboard token to VALUE (your own, any length).
                   On an existing project only the token changes -- the ingest
                   key stays, so the sending server needs nothing new:
                     python3 scripts/add_project.py byky-qa --token mytoken
"""

import argparse
import json
import os
import re
import secrets
import sys
import urllib.parse
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
    parser.add_argument("--token", help="your own dashboard token (only the token changes on an existing project)")
    args = parser.parse_args()

    if not re.match(r"^[a-z0-9][a-z0-9-]{0,39}$", args.slug):
        sys.exit("slug: lowercase letters, digits and dashes (max 40)")
    if args.token is not None and not args.token:
        sys.exit("--token: must not be empty -- the dashboard would be open to anyone")
    projects = json.loads(FILE.read_text()) if FILE.exists() else {}
    exists = args.slug in projects
    if exists and not (args.rotate or args.token):
        sys.exit(f"'{args.slug}' exists already -- add --rotate for new secrets, or --token to set the token")
    if args.token and any(args.token in (p.get("ingest_key"), p.get("dashboard_token"))
                          for slug, p in projects.items() if slug != args.slug):
        sys.exit("--token: another project already uses that value")

    entry = projects.get(args.slug, {})
    entry["name"] = args.name or entry.get("name") or args.slug
    entry["profile"] = args.profile if (args.profile != "generic" or "profile" not in entry) else entry["profile"]
    if not exists or args.rotate:
        entry["ingest_key"] = secrets.token_hex(32)
    entry["dashboard_token"] = args.token or (secrets.token_hex(32) if (not exists or args.rotate)
                                              else entry["dashboard_token"])
    projects[args.slug] = entry

    FILE.parent.mkdir(exist_ok=True)
    FILE.write_text(json.dumps(projects, indent=2) + "\n")
    os.chmod(FILE, 0o600)

    print(f"Project     : {args.slug} ({entry['name']}, profile {entry['profile']})")
    print(f"Ingest key  : {entry['ingest_key']}")
    token = urllib.parse.quote(entry["dashboard_token"], safe="")
    print(f"Dashboard   : {args.base_url.rstrip('/')}/p/{args.slug}/#token={token}")
    print("\nRestart to apply:  sudo docker compose restart fastapi")


if __name__ == "__main__":
    main()
