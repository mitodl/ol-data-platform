#!/usr/bin/env python3
"""Expire old Iceberg snapshots to reduce metadata bloat.

Snapshot accumulation causes metadata.json to grow very large (100s of KB),
which slows down Iceberg table operations (load_table, plan_files, etc.)
by requiring S3 round trips to read oversized metadata files.

Usage:
    uv run scripts/expire_iceberg_snapshots.py --help
    uv run scripts/expire_iceberg_snapshots.py --dry-run
    uv run scripts/expire_iceberg_snapshots.py --older-than-days 7
"""

from __future__ import annotations

import argparse
import os
from datetime import UTC, datetime, timedelta

from pyiceberg.catalog.glue import GlueCatalog


def main() -> None:
    parser = argparse.ArgumentParser(description="Expire old Iceberg snapshots")
    parser.add_argument(
        "--table",
        default="ol_warehouse_production.student_risk.reporting__student_risk_probability",
        help="Fully-qualified Iceberg table identifier",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=3,
        help="Expire snapshots older than this many days (default: 3)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be expired without actually expiring",
    )
    args = parser.parse_args()

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
    catalog = GlueCatalog(
        "glue",
        **{
            "glue.region": region,
            "s3.connect-timeout": "10",
            "s3.request-timeout": "120",
        },
    )

    table = catalog.load_table(args.table)
    snapshots = table.snapshots()
    print(f"Table: {args.table}")
    print(f"Current snapshot count: {len(snapshots)}")

    if snapshots:
        oldest = min(s.timestamp_ms for s in snapshots)
        newest = max(s.timestamp_ms for s in snapshots)
        print(f"Oldest snapshot: {datetime.fromtimestamp(oldest / 1000, tz=UTC)}")
        print(f"Newest snapshot: {datetime.fromtimestamp(newest / 1000, tz=UTC)}")

    cutoff = datetime.now(tz=UTC) - timedelta(days=args.older_than_days)
    cutoff_ms = int(cutoff.timestamp() * 1000)
    to_expire = [s for s in snapshots if s.timestamp_ms < cutoff_ms]
    print(
        f"\nSnapshots older than {args.older_than_days} days ({cutoff.isoformat()}): {len(to_expire)}"
    )

    if args.dry_run:
        print("DRY RUN — no changes made")
        return

    if not to_expire:
        print("Nothing to expire")
        return

    print(f"Expiring {len(to_expire)} snapshot(s)...")
    table.manage_snapshots().expire_snapshots(older_than_ms=cutoff_ms).commit()

    table = catalog.load_table(args.table)
    print(f"Done. Remaining snapshots: {len(table.snapshots())}")


if __name__ == "__main__":
    main()
