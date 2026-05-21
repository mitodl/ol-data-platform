"""Expire old Iceberg snapshots to reduce metadata bloat.

Tables that are written to frequently accumulate snapshot history indefinitely
unless expire_snapshots() is run. Each uncompacted snapshot adds ~1.6 KB to the
metadata.json file, which must be read from S3 on every table operation.

Usage:
    uv run python scripts/expire_iceberg_snapshots.py
    uv run python scripts/expire_iceberg_snapshots.py --dry-run
    uv run python scripts/expire_iceberg_snapshots.py --older-than-days 14
"""

import argparse
import datetime

import boto3
from pyiceberg.catalog.glue import GlueCatalog

TABLES_TO_EXPIRE = [
    # Non-dbt singleton tables
    ("ol_warehouse_production_reporting", "student_risk_probability"),
    # Raw layer worst offenders (by snapshot count — Phase 0 remediation)
    # These have never had maintenance run and have bloated metadata.json files
    # that are re-downloaded on every Airbyte sync and dbt source read.
    ("ol_warehouse_production_raw", "raw__edxorg__program_learner_report"),
    ("ol_warehouse_production_raw", "raw__irx__edxorg__bigquery__email_opt_in"),
    (
        "ol_warehouse_production_raw",
        "raw__thirdparty__mailgun__destination_v2__domains",
    ),
    ("ol_warehouse_production_raw", "raw__emeritus__bigquery__api_enrollments"),
    ("ol_warehouse_production_raw", "raw__mitxonline__openedx__api__course_blocks"),
]

DEFAULT_OLDER_THAN_DAYS = 7


def expire_snapshots(
    dry_run: bool = False, older_than_days: int = DEFAULT_OLDER_THAN_DAYS
) -> None:
    """Expire old Iceberg snapshots for tables in TABLES_TO_EXPIRE."""
    glue = GlueCatalog(
        "default",
        client=boto3.client("glue", region_name="us-east-1"),
        **{
            "s3.connect-timeout": "10",
            "s3.request-timeout": "120",
        },
    )

    cutoff_dt = datetime.datetime.now(tz=datetime.UTC) - datetime.timedelta(
        days=older_than_days
    )
    cutoff_ms = int(cutoff_dt.timestamp() * 1000)

    for database, table_name in TABLES_TO_EXPIRE:
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Processing {database}.{table_name}")
        table = glue.load_table(f"{database}.{table_name}")

        snapshots = table.metadata.snapshots
        current_id = table.metadata.current_snapshot_id
        old_snapshots = [
            s
            for s in snapshots
            if s.snapshot_id != current_id and s.timestamp_ms < cutoff_ms
        ]

        print(f"  Total snapshots:       {len(snapshots)}")
        print(f"  Snapshots to expire:   {len(old_snapshots)}")
        print(f"  Snapshots to keep:     {len(snapshots) - len(old_snapshots)}")

        import boto3 as _boto3  # noqa: PLC0415

        s3 = _boto3.client("s3", region_name="us-east-1")
        bucket = "ol-data-lake-staging-production"
        prefix = f"processed/{database}/{table_name}/metadata/"
        paginator = s3.get_paginator("list_objects_v2")
        meta_size = 0
        meta_count = 0
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                meta_size += obj["Size"]
                meta_count += 1
        print(
            f"  Metadata directory:    ~{meta_size // 1024} KB across {meta_count} files"
        )

        if dry_run:
            print("  [DRY RUN] Skipping expiration.")
            continue

        if not old_snapshots:
            print("  No eligible snapshots to expire.")
            continue

        # pyiceberg >= 0.10.0 API: table.maintenance.expire_snapshots().older_than(dt).commit()
        table.maintenance.expire_snapshots().older_than(cutoff_dt).commit()
        print(f"  Expired {len(old_snapshots)} snapshots.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be expired without modifying anything",
    )
    parser.add_argument(
        "--older-than-days",
        type=int,
        default=DEFAULT_OLDER_THAN_DAYS,
        help=f"Expire snapshots older than this many days (default: {DEFAULT_OLDER_THAN_DAYS})",
    )
    args = parser.parse_args()
    expire_snapshots(dry_run=args.dry_run, older_than_days=args.older_than_days)
