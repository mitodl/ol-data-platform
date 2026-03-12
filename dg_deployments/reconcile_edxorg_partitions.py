# ruff: noqa: INP001
"""Reconcile edxorg archive asset partitions and S3 objects with invalid course IDs.

Partitions created between commit 2c2b9c3a (2026-02-11) and the subsequent fix
have course_id values that incorrectly include a data-type suffix:

  Bad:       MITx-15.415x-3T2018-course|edge
  Canonical: MITx-15.415x-3T2018|edge

This arises because ``parse_archive_path`` was not consuming the ``-course`` /
``-course_structure`` token that appears between the course run and the source-
system marker in ``.json`` archive filenames, so the regex engine absorbed it
into the course_id instead.

Impact by asset type
--------------------
* **course_structure** (``.json``) - wrong course_id in the S3 path AND the
  partition key.  Object keys look like::

      edxorg/raw_data/course_structure/edge/MITx-15.415x-3T2018-course/<hash>.json

  These must be S3-copied to the canonical path *and* re-emitted with the
  correct partition key and path metadata.

* **course_xml** (``.xml.tar.gz``) - same S3 path problem (the ``-course``
  suffix in the filename was absorbed into course_id by the pre-existing regex
  before 2c2b9c3a *and* continued afterwards).

* **db_table** (``.sql``) - course_id was always parsed correctly (the table-
  name token was consumed by ``DATA_ATTRIBUTE_REGEX``), so **no S3 correction
  is needed**.

* **forum_mongo** (``.mongo``) - filenames never carry a suffix before the
  source-system marker, so course_id was always correct; **no S3 correction
  needed**.

What this script does
---------------------
1. Lists all dynamic partition keys for ``course_and_source``.
2. Identifies keys whose course_id component ends in ``-course`` or
   ``-course_structure``.
3. Adds the canonical (suffix-stripped) partition key if not already present.
4. For every asset materialisation event under the bad partition key:

   a. Inspects the ``object_key`` metadata field.
   b. If that key embeds the wrong course_id as a path segment, S3-copies the
      object to the corrected path (skipped if the destination already exists).
   c. Re-emits an ``AssetMaterialisation`` under the canonical partition key
      with updated ``object_key``, ``path``, and ``course_id`` metadata.

5. Deletes the invalid dynamic partition keys.

Usage::

    uv run python scripts/reconcile_edxorg_partitions.py \\
        [--dry-run] \\
        [--aws-profile PROFILE] \\
        [--s3-bucket BUCKET] \\
        [--max-workers N]

``--dry-run`` prints what would change without writing anything.
``--s3-bucket`` overrides the bucket resolved from the materialisation ``path``
metadata; useful when running against a non-production copy of the event log.
``--max-workers`` sets the thread-pool size for concurrent S3 and partition-
deletion operations (default: 16).
"""

import argparse
import logging
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from urllib.parse import urlparse

import boto3
import botocore
from dagster import (
    AssetKey,
    AssetMaterialization,
    DagsterInstance,
    MetadataValue,
    MultiPartitionKey,
)
from dagster._core.events import DagsterEventType
from dagster._core.storage.event_log.base import EventRecordsFilter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger(__name__)

PARTITIONS_DEF_NAME = "course_and_source"

# Suffixes that should never appear in the course_id dimension of a partition
# key.  These originate from the file-type label embedded in edxorg archive
# filenames (e.g. MITx-15.415x-3T2018-course-prod.json).
INVALID_COURSE_ID_SUFFIX_RE = re.compile(r"-(course|course_structure)$")

# Asset keys that may have been materialised with the wrong partition.
# db_table and forum_mongo always had correct course_ids so are omitted.
AFFECTED_ASSET_KEYS: list[AssetKey] = [
    AssetKey(("edxorg", "raw_data", "course_structure")),
    AssetKey(("edxorg", "raw_data", "course_xml")),
]

# Default thread-pool size for concurrent S3 and partition-deletion operations.
DEFAULT_MAX_WORKERS = 16


@dataclass
class _WorkItem:
    """All information needed to process a single bad-partition materialisation."""

    bad_key: str
    canon_key: MultiPartitionKey
    asset_key: AssetKey
    new_materialization: AssetMaterialization
    # S3 copy details; None when no copy is required for this record.
    bucket: str | None = None
    bad_s3_key: str | None = None
    good_s3_key: str | None = None
    # Populated during the S3 execution phase.
    s3_copy_failed: bool = field(default=False, init=False)


def _canonical_course_id(course_id: str) -> str | None:
    """Return the stripped course_id, or None if it is already canonical."""
    m = INVALID_COURSE_ID_SUFFIX_RE.search(course_id)
    return course_id[: m.start()] if m else None


def _parse_partition_key(raw_key: str) -> tuple[str, str]:
    """Split a ``course_id|source_system`` key string into its two components.

    Keys stored in the dynamic partition are serialised from
    ``MultiPartitionKey({"course_id": ..., "source_system": ...})``, which
    Dagster stores as ``{course_id}|{source_system}`` (no ``dim=`` prefix).
    """
    idx = raw_key.rfind("|")
    if idx == -1:
        msg = f"Unexpected partition key format (no '|'): {raw_key!r}"
        raise ValueError(msg)
    return raw_key[:idx], raw_key[idx + 1 :]


def _make_multi_partition_key(course_id: str, source_system: str) -> MultiPartitionKey:
    return MultiPartitionKey({"course_id": course_id, "source_system": source_system})


def _tag_safe(value: str) -> str:
    """Replace characters that are invalid in Dagster tag values.

    Dagster tags allow only ``[a-zA-Z0-9_\\-.]`` and a maximum of 63 chars.
    Partition keys contain ``|`` as a dimension separator; replace it with
    ``__`` so the value survives tag validation while remaining readable.
    """
    return value.replace("|", "__")[:63]


def _correct_object_key(object_key: str) -> str | None:
    """Strip the invalid course_id suffix from any matching path segment.

    Returns the corrected key, or None if no correction was needed.

    The course_id is always the last directory segment before the filename::

        edxorg/raw_data/course_structure/edge/<course_id>/<hash>.json

    So we inspect each segment and strip the suffix from whichever one matches.
    """
    parts = object_key.split("/")
    corrected = []
    changed = False
    for part in parts:
        m = INVALID_COURSE_ID_SUFFIX_RE.search(part)
        if m:
            corrected.append(part[: m.start()])
            changed = True
        else:
            corrected.append(part)
    return "/".join(corrected) if changed else None


def _s3_object_exists(s3_client, bucket: str, key: str) -> bool:
    """Return True if the S3 object exists."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True  # noqa: TRY300
    except s3_client.exceptions.ClientError as exc:
        if exc.response["Error"]["Code"] in {"404", "NoSuchKey"}:
            return False
        raise


def _parse_s3_url(url: str) -> tuple[str, str]:
    """Return (bucket, key) from an ``s3://bucket/key`` URL."""
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        msg = f"Expected s3:// URL, got: {url!r}"
        raise ValueError(msg)
    return parsed.netloc, parsed.path.lstrip("/")


def _meta_value(meta: dict[str, object | str], field_name: str) -> str | None:
    """Extract a raw string value from a metadata dict regardless of wrapper type."""
    val: object | str = meta.get(field_name)
    if val is None:
        return None
    retval: str = str(val.value) if hasattr(val, "value") else str(val)
    return retval


def _plan_work_items(
    instance: DagsterInstance,
    bad_keys: list[str],
    canonical_keys: dict[str, MultiPartitionKey],
    s3_bucket_override: str | None,
) -> list[_WorkItem]:
    """Fetch all event records in two bulk queries and build the work plan.

    Rather than issuing ``len(bad_keys) x len(AFFECTED_ASSET_KEYS)`` individual
    event-log queries, this function issues one query per asset key and passes
    *all* bad partition keys at once via ``EventRecordsFilter.asset_partitions``.
    """
    items: list[_WorkItem] = []

    for asset_key in AFFECTED_ASSET_KEYS:
        records = instance.get_event_records(
            EventRecordsFilter(
                asset_key=asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=bad_keys,
            )
        )

        # Group by partition so we can log a concise summary per bad key.
        records_by_partition: dict[str, int] = defaultdict(int)
        for record in records:
            original = record.asset_materialization
            if original is None:
                continue
            bad_key = original.partition
            if bad_key not in canonical_keys:
                continue
            records_by_partition[bad_key] += 1

            canon_key = canonical_keys[bad_key]
            original_meta = dict(original.metadata)
            updated_meta = dict(original_meta)

            raw_object_key = _meta_value(original_meta, "object_key")
            raw_path = _meta_value(original_meta, "path")

            correct_object_key = (
                _correct_object_key(raw_object_key) if raw_object_key else None
            )

            bucket: str | None = None
            bad_s3_key: str | None = None
            good_s3_key: str | None = None

            if correct_object_key and raw_path:
                bucket, _ = _parse_s3_url(str(raw_path))
                if s3_bucket_override:
                    bucket = s3_bucket_override

                prefix = str(raw_path).split(raw_object_key)[0].rstrip("/")
                prefix_no_scheme = prefix.removeprefix("s3://").split("/", 1)
                s3_prefix = prefix_no_scheme[1] if len(prefix_no_scheme) > 1 else ""

                bad_s3_key = (
                    f"{s3_prefix}/{raw_object_key}".lstrip("/")
                    if s3_prefix
                    else raw_object_key
                )
                good_s3_key = (
                    f"{s3_prefix}/{correct_object_key}".lstrip("/")
                    if s3_prefix
                    else correct_object_key
                )
                correct_path = f"s3://{bucket}/{good_s3_key}"
                updated_meta["object_key"] = correct_object_key
                updated_meta["path"] = MetadataValue.path(correct_path)

            raw_course_id = _meta_value(original_meta, "course_id")
            if raw_course_id:
                canon_course_id = _canonical_course_id(str(raw_course_id))
                if canon_course_id:
                    updated_meta["course_id"] = MetadataValue.text(canon_course_id)

            new_mat = AssetMaterialization(
                asset_key=original.asset_key,
                description=original.description,
                metadata=updated_meta,
                partition=canon_key,
                tags={
                    **original.tags,
                    # Audit trail: tag value must be tag-safe (no '|').
                    "dagster/reconciled_from_partition": _tag_safe(bad_key),
                },
            )
            items.append(
                _WorkItem(
                    bad_key=bad_key,
                    canon_key=canon_key,
                    asset_key=asset_key,
                    new_materialization=new_mat,
                    bucket=bucket,
                    bad_s3_key=bad_s3_key,
                    good_s3_key=good_s3_key,
                )
            )

        for bk, count in records_by_partition.items():
            log.info(
                "Planned %d materialization(s) for %s  partition %r -> %r",
                count,
                asset_key,
                bk,
                canonical_keys[bk],
            )

    return items


def _execute_s3_copy(item: _WorkItem, s3_client, dry_run: bool) -> None:  # noqa: FBT001
    """Check whether the destination S3 object exists and copy if needed.

    This function is designed to be called concurrently from a thread pool;
    it mutates ``item.s3_copy_failed`` on failure.
    """
    assert item.bucket  # noqa: S101
    assert item.bad_s3_key  # noqa: S101
    assert item.good_s3_key  # noqa: S101
    bucket = item.bucket
    bad_s3_key = item.bad_s3_key
    good_s3_key = item.good_s3_key
    correct_path = f"s3://{bucket}/{good_s3_key}"

    if _s3_object_exists(s3_client, bucket, good_s3_key):
        log.info("S3 destination already exists, skipping copy: %s", correct_path)
        return

    log.info(
        "S3 copy: s3://%s/%s -> s3://%s/%s",
        bucket,
        bad_s3_key,
        bucket,
        good_s3_key,
    )
    if dry_run:
        return

    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={"Bucket": bucket, "Key": bad_s3_key},
            Key=good_s3_key,
        )
    except botocore.errorfactory.NoSuchKey:
        log.info("Source key no longer exists: s3://%s/%s", bucket, bad_s3_key)
    except Exception:
        log.exception(
            "S3 copy failed: s3://%s/%s -> s3://%s/%s",
            bucket,
            bad_s3_key,
            bucket,
            good_s3_key,
        )
        item.s3_copy_failed = True


def reconcile(  # noqa: C901, PLR0912, PLR0915
    *,
    dry_run: bool = False,
    aws_profile: str | None = None,
    s3_bucket_override: str | None = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
) -> None:
    """Identify invalid partitions, fix S3 objects, and re-emit events.

    Parameters
    ----------
    dry_run:
        When True, logs every action that *would* be taken but makes no
        mutations to S3 or the Dagster event store.
    aws_profile:
        Optional AWS profile name for boto3.  Defaults to the environment /
        instance-profile credential chain.
    s3_bucket_override:
        Force all S3 operations to use this bucket instead of the one
        embedded in each materialisation's ``path`` metadata.
    max_workers:
        Thread-pool size for concurrent S3 operations and partition deletions.

    """
    session = boto3.Session(profile_name=aws_profile)
    s3_client = session.client("s3")
    instance = DagsterInstance.get()

    all_keys: list[str] = instance.get_dynamic_partitions(PARTITIONS_DEF_NAME)
    log.info(
        "Found %d total partition keys in '%s'.",
        len(all_keys),
        PARTITIONS_DEF_NAME,
    )

    bad_keys: list[str] = []
    # bad_key_str -> canonical MultiPartitionKey
    canonical_keys: dict[str, MultiPartitionKey] = {}

    for key in all_keys:
        try:
            course_id, source_system = _parse_partition_key(key)
        except ValueError:
            log.warning("Skipping unparseable partition key: %r", key)
            continue
        canonical = _canonical_course_id(course_id)
        if canonical is not None:
            bad_keys.append(key)
            canonical_keys[key] = _make_multi_partition_key(canonical, source_system)

    if not bad_keys:
        log.info("No invalid partition keys found - nothing to do.")
        return

    log.info(
        "Found %d invalid partition key(s) to reconcile%s.",
        len(bad_keys),
        " (dry-run)" if dry_run else "",
    )
    for bk in bad_keys:
        log.info("  %r  ->  %r", bk, canonical_keys[bk])

    existing_key_set: set[str] = set(all_keys)

    # 1. Add canonical partition keys that do not exist yet (single bulk call).
    keys_to_add = [
        mk for bad_k, mk in canonical_keys.items() if str(mk) not in existing_key_set
    ]
    if keys_to_add:
        log.info("Adding %d new canonical partition key(s).", len(keys_to_add))
        if not dry_run:
            instance.add_dynamic_partitions(
                PARTITIONS_DEF_NAME, partition_keys=keys_to_add
            )

    # 2. Plan all work: two bulk event-log queries (one per asset key) instead
    #    of len(bad_keys) x len(AFFECTED_ASSET_KEYS) individual queries.
    log.info(
        "Fetching event records for %d bad partition(s) across %d asset key(s)...",
        len(bad_keys),
        len(AFFECTED_ASSET_KEYS),
    )
    work_items = _plan_work_items(
        instance, bad_keys, canonical_keys, s3_bucket_override
    )
    log.info("Planned %d total materialization(s) to reconcile.", len(work_items))

    # 3. Execute S3 copy operations concurrently.
    s3_items = [item for item in work_items if item.bucket]
    if s3_items:
        log.info(
            "Executing %d S3 copy operation(s) with up to %d concurrent workers%s.",
            len(s3_items),
            max_workers,
            " (dry-run)" if dry_run else "",
        )
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_item = {
                pool.submit(_execute_s3_copy, item, s3_client, dry_run): item
                for item in s3_items
            }
            for future in as_completed(future_to_item):
                future.result()  # propagate unexpected exceptions

    # 4. Re-emit materialisation events sequentially (DB writes are not
    #    safe to parallelize against the Dagster event log).
    emit_count = 0
    for item in work_items:
        if item.s3_copy_failed:
            log.warning(
                "Skipping re-emit for %s / %r due to S3 copy failure.",
                item.asset_key,
                item.bad_key,
            )
            continue
        log.info(
            "Re-emitting materialisation for %s under %r",
            item.asset_key,
            item.canon_key,
        )
        if not dry_run:
            instance.report_runless_asset_event(item.new_materialization)
        emit_count += 1
    log.info("Re-emitted %d materialisation event(s).", emit_count)

    # 5. Delete the invalid dynamic partition keys concurrently.
    log.info(
        "Deleting %d invalid partition key(s) with up to %d concurrent workers.",
        len(bad_keys),
        max_workers,
    )

    def _delete_partition(bad_key: str) -> None:
        log.info("  Deleting %r", bad_key)
        if not dry_run:
            instance.delete_dynamic_partition(PARTITIONS_DEF_NAME, bad_key)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_delete_partition, bk) for bk in bad_keys]
        for future in as_completed(futures):
            future.result()

    suffix = " (dry-run - no changes persisted)" if dry_run else ""
    log.info("Reconciliation complete%s.", suffix)


def main() -> None:
    """Entry point for the reconciliation script."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would change without writing to S3 or the Dagster instance.",
    )
    parser.add_argument(
        "--aws-profile",
        default=None,
        metavar="PROFILE",
        help="AWS profile name for boto3 credentials (default: env/instance profile).",
    )
    parser.add_argument(
        "--s3-bucket",
        default=None,
        metavar="BUCKET",
        dest="s3_bucket",
        help=(
            "Override the S3 bucket for all copy operations (useful when "
            "running against a non-production event log)."
        ),
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        metavar="N",
        dest="max_workers",
        help=(
            f"Thread-pool size for concurrent S3 and partition-deletion "
            f"operations (default: {DEFAULT_MAX_WORKERS})."
        ),
    )
    args = parser.parse_args()
    reconcile(
        dry_run=args.dry_run,
        aws_profile=args.aws_profile,
        s3_bucket_override=args.s3_bucket,
        max_workers=args.max_workers,
    )


if __name__ == "__main__":
    main()
