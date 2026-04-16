#!/usr/bin/env python3
"""
Nightly backup script for the log ingestion platform.

Performs a compressed mongodump of the logsdb database, creates a tar.gz
archive, verifies the archive is not suspiciously small (catches silent
corruption), and purges backups older than RETAIN_DAYS.

Designed to run via cron:
  0 3 * * * cd /app && python3 scripts/backup.py >> /var/log/backup.log 2>&1 \
    || echo "BACKUP FAILED $(date)" | mail -s "Backup failure" admin@yourcompany.com

Exit codes:
  0 — backup completed and verified successfully
  1 — backup failed (mongodump error, empty archive, etc.)

The non-zero exit code on failure allows cron, systemd, or any wrapper script
to detect failures and trigger alerts (email, Slack webhook, PagerDuty, etc.).

Directory structure:
  /backups/
  └── 2026/
      └── 04/
          ├── backup_2026-04-14.tar.gz
          └── backup_2026-04-15.tar.gz

Environment variables:
  MONGODB_URL       — MongoDB connection URI (required)
  BACKUP_DIR        — root backup directory (default: /backups)
  BACKUP_RETAIN_DAYS — days to keep old backups (default: 30)
"""

import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [BACKUP] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

MONGO_URI   = os.getenv("MONGODB_URL")
BACKUP_DIR  = Path(os.getenv("BACKUP_DIR", "/backups"))
RETAIN_DAYS = int(os.getenv("BACKUP_RETAIN_DAYS", "30"))


def run_backup():
    """
    Execute the full backup pipeline:

    1. Build date-based output path (e.g., /backups/2026/04/)
    2. Skip if today's backup already exists (idempotent for re-runs)
    3. Run mongodump with --gzip compression
    4. Create tar.gz archive from the dump directory
    5. Verify archive is > 1 KB (catches empty/corrupt dumps)
    6. Clean up raw dump directory
    7. Purge old backups beyond RETAIN_DAYS

    Exits with code 1 on any failure so cron can detect and alert.
    """
    if not MONGO_URI:
        log.error("MONGODB_URL environment variable not set")
        sys.exit(1)

    today    = datetime.utcnow()
    date_str = today.strftime("%Y-%m-%d")
    out_dir  = BACKUP_DIR / today.strftime("%Y/%m")
    out_dir.mkdir(parents=True, exist_ok=True)

    dump_path    = out_dir / f"dump_{date_str}"
    archive_path = out_dir / f"backup_{date_str}.tar.gz"

    # ── Step 1: Skip if already done (idempotent) ─────────────────────────
    if archive_path.exists():
        log.info(f"Backup already exists for {date_str} — skipping")
        return

    # ── Step 2: Run mongodump ─────────────────────────────────────────────
    log.info(f"Starting mongodump for {date_str}")

    result = subprocess.run(
        [
            "mongodump",
            f"--uri={MONGO_URI}",
            "--db=logsdb",
            f"--out={dump_path}",
            "--gzip",
        ],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        log.error(f"mongodump FAILED: {result.stderr}")
        sys.exit(1)   # non-zero exit — cron detects failure

    # ── Step 3: Create compressed archive ─────────────────────────────────
    shutil.make_archive(str(dump_path), "gztar", str(out_dir), f"dump_{date_str}")
    shutil.rmtree(dump_path)

    # ── Step 4: Verify archive integrity ──────────────────────────────────
    # Catches silent corruption where mongodump "succeeds" but produces
    # an empty dump (e.g., auth failure silently skips collections)
    size_bytes = archive_path.stat().st_size
    if size_bytes < 1024:
        log.error(
            f"Archive suspiciously small ({size_bytes} bytes) — "
            "possible empty dump. Investigate immediately."
        )
        sys.exit(1)

    size_mb = size_bytes / 1024 / 1024
    log.info(f"Backup verified: {archive_path} ({size_mb:.1f} MB)")

    # ── Step 5: Purge old backups ─────────────────────────────────────────
    cutoff = today - timedelta(days=RETAIN_DAYS)
    for f in BACKUP_DIR.rglob("backup_*.tar.gz"):
        try:
            file_date = datetime.strptime(
                f.stem.replace("backup_", ""), "%Y-%m-%d"
            )
            if file_date < cutoff:
                f.unlink()
                log.info(f"Purged old backup: {f}")
        except ValueError:
            pass  # filename doesn't match expected pattern — skip

    log.info(f"Backup complete for {date_str}")


if __name__ == "__main__":
    run_backup()
