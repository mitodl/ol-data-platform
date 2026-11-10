#!/usr/bin/env python3
"""Restore a history-layer Iceberg table from its bucket's noncurrent S3 versions.

History tables (``ol_warehouse_<env>_history``) are snapshots of raw tables and
cannot be rebuilt from anything. The history buckets keep noncurrent object
versions for 90 days, so within that window a table whose Glue entry was
dropped, or whose files were deleted, comes back in two steps:

  undelete   remove the delete markers placed on the table directory's objects
             since a given time, which makes each object's previous version
             current again
  register   register the table in Glue from the newest metadata file in its
             directory

Both print what they would do and change nothing without ``--execute``. The
runbook is docs/HISTORY_LAYER_RESTORE.md.

Usage::

    uv run python bin/history-table-restore.py undelete \\
        s3://ol-data-lake-history-qa/snapshot_x-<uuid> \\
        --deleted-after 2026-11-10T12:00:00Z
    uv run python bin/history-table-restore.py register \\
        s3://ol-data-lake-history-qa/snapshot_x-<uuid> \\
        --table ol_warehouse_qa_history.snapshot_x
"""
# ruff: noqa: T201

import re
from datetime import datetime
from functools import cache
from typing import Any

import boto3
from cyclopts import App
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from ol_orchestrate.lib.lake_orphan_sweep import S3_DELETE_BATCH_SIZE, normalize

app = App(name="history-table-restore")

# Iceberg names a table's metadata files <version>-<uuid>.metadata.json, with a
# zero-padded version that grows by one per commit.
METADATA_FILE = re.compile(r"/metadata/(\d+)-[0-9a-f-]+\.metadata\.json$")


@cache
def s3() -> Any:
    """Return a cached S3 client."""
    return boto3.client("s3")


def _split(location: str) -> tuple[str, str]:
    bucket, _, prefix = normalize(location).partition("/")
    if not bucket or not prefix:
        msg = f"{location} is not an s3://bucket/<table directory> location"
        raise ValueError(msg)
    return bucket, prefix


@app.command
def undelete(location: str, *, deleted_after: datetime, execute: bool = False) -> None:
    """Make the objects deleted under a table directory current again.

    Parameters
    ----------
    location: The table directory, s3://<bucket>/<table>-<uuid>.
    deleted_after: Only delete markers placed at or after this time are removed,
        so an object deleted on purpose before the incident stays deleted.
    execute: Without it, print what would be restored and change nothing.
    """
    bucket, prefix = _split(location)
    markers: list[dict[str, str]] = []
    older = 0
    pages = (
        s3()
        .get_paginator("list_object_versions")
        .paginate(Bucket=bucket, Prefix=f"{prefix}/")
    )
    for page in pages:
        for marker in page.get("DeleteMarkers", []):
            if not marker["IsLatest"]:
                continue
            if marker["LastModified"] >= deleted_after:
                markers.append({"Key": marker["Key"], "VersionId": marker["VersionId"]})
            else:
                older += 1
    print(
        f"s3://{bucket}/{prefix}/: {len(markers)} objects deleted since "
        f"{deleted_after.isoformat()}, {older} deleted before it (left alone)"
    )
    if not execute:
        return
    errors: list[str] = []
    for start in range(0, len(markers), S3_DELETE_BATCH_SIZE):
        response = s3().delete_objects(
            Bucket=bucket,
            Delete={"Objects": markers[start : start + S3_DELETE_BATCH_SIZE]},
        )
        errors += [
            f"{error['Key']}: {error['Code']} {error['Message']}"
            for error in response.get("Errors", [])
        ]
    if errors:
        msg = f"{len(errors)} delete markers could not be removed: {errors[:5]}"
        raise RuntimeError(msg)
    print(f"Removed {len(markers)} delete markers.")


@app.command
def register(location: str, *, table: str, execute: bool = False) -> None:
    """Register a table in Glue from the newest metadata file in its directory.

    Parameters
    ----------
    location: The table directory, s3://<bucket>/<table>-<uuid>.
    table: The Glue identifier to register it as, <database>.<table>.
    execute: Without it, print the metadata file it would use and change nothing.
    """
    bucket, prefix = _split(location)
    pages = (
        s3()
        .get_paginator("list_objects_v2")
        .paginate(Bucket=bucket, Prefix=f"{prefix}/metadata/")
    )
    versions = [
        (int(match.group(1)), obj["Key"])
        for page in pages
        for obj in page.get("Contents", [])
        if (match := METADATA_FILE.search(f"/{obj['Key']}"))
    ]
    if not versions:
        msg = f"No current metadata file under s3://{bucket}/{prefix}/metadata/"
        raise RuntimeError(msg)
    metadata_location = f"s3://{bucket}/{max(versions)[1]}"
    print(f"{table}: newest of {len(versions)} metadata files is {metadata_location}")
    if not execute:
        return
    restored = get_glue_catalog().register_table(table, metadata_location)
    snapshot = restored.current_snapshot()
    records = snapshot.summary["total-records"] if snapshot and snapshot.summary else 0
    print(
        f"Registered {table}: {len(restored.snapshots())} snapshots, "
        f"current snapshot {snapshot.snapshot_id if snapshot else None}, "
        f"{records} records"
    )


if __name__ == "__main__":
    app()
