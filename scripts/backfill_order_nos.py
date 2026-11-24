"""One-off: give BYKY logs stored before 24 Nov 2026 their log.order_nos, so the
dashboard's Order no filter finds them too.

New logs carry log.order_nos from byky_django itself (apps/monitoring/shipper.py).
This fills it in for older logs from what they already hold: every "order_no" in
the stored request (form_data) and reply (log.response) -- a reply cut short is
searched as text. A call that named its order only by id and was refused (so its
reply has no order) is not found: that needs BYKY's own database.

Only adds log.order_nos (a list, at most 50, [] when the log names no order) to
logs that have none. Nothing else changes. Safe to run again: it skips logs
already done. Dry run unless --apply.

Run inside the fastapi container (it has pymongo and MONGODB_URL):

    sudo docker compose exec -T fastapi python - < scripts/backfill_order_nos.py
    sudo docker compose exec -T fastapi python - --apply < scripts/backfill_order_nos.py

    --projects byky-qa,byky-live   which projects (default: both)
    --batch 1000                   documents per write
"""

import argparse
import os
import re
import sys
import time

import pymongo

MAX_ORDER_NOS = 50
_ORDER_NO_TEXT = re.compile(r'"order_no"\s*:\s*"([^"]{1,30})"')


def walk(node, found):
    """Every order_no in a stored body (dicts, lists, and text of a cut-short body)."""
    if isinstance(node, dict):
        for key, value in node.items():
            if key == "order_no" and isinstance(value, str) and value.strip():
                found.add(value.strip())
            else:
                walk(value, found)
    elif isinstance(node, list):
        for value in node:
            walk(value, found)
    elif isinstance(node, str) and '"order_no"' in node:
        found.update(_ORDER_NO_TEXT.findall(node))


def order_nos(doc):
    found = set()
    walk(doc.get("form_data"), found)
    walk((doc.get("log") or {}).get("response"), found)
    return sorted(found)[:MAX_ORDER_NOS]


def run(col, projects, apply=False, batch=1000, out=sys.stdout):
    query = {"project_id": {"$in": projects}, "log.order_nos": {"$exists": False}}
    total = col.count_documents(query)
    print(f"logs without order_nos in {', '.join(projects)}: {total}", file=out)
    seen = with_orders = written = 0
    ops = []
    started = time.monotonic()
    cursor = col.find(query, {"form_data": 1, "log.response": 1}, no_cursor_timeout=False).batch_size(batch)
    for doc in cursor:
        numbers = order_nos(doc)
        seen += 1
        with_orders += bool(numbers)
        if apply:
            ops.append(pymongo.UpdateOne({"_id": doc["_id"], "log.order_nos": {"$exists": False}},
                                         {"$set": {"log.order_nos": numbers}}))
            if len(ops) >= batch:
                written += col.bulk_write(ops, ordered=False).modified_count
                ops = []
        if seen % 20_000 == 0:
            print(f"  ... {seen}/{total} read, {with_orders} name an order", file=out)
    if ops:
        written += col.bulk_write(ops, ordered=False).modified_count
    took = time.monotonic() - started
    print(f"read {seen}, naming an order {with_orders}, "
          + (f"updated {written}" if apply else "DRY RUN -- nothing written (add --apply)")
          + f", {took:.0f}s", file=out)
    return {"seen": seen, "with_orders": with_orders, "written": written}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--projects", default="byky-qa,byky-live")
    parser.add_argument("--batch", type=int, default=1000)
    args = parser.parse_args(argv)
    client = pymongo.MongoClient(os.environ["MONGODB_URL"])
    col = client[os.environ.get("MONGO_DB", "logsdb")]["logs"]
    run(col, [p.strip() for p in args.projects.split(",") if p.strip()], args.apply, args.batch)


if __name__ == "__main__":
    main()
