#!/usr/bin/env python3
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
        [--s3-bucket BUCKET]

``--dry-run`` prints what would change without writing anything.
``--s3-bucket`` overrides the bucket resolved from the materialisation ``path``
metadata; useful when running against a non-production copy of the event log.
"""

import argparse
import logging
import re
import sys
from urllib.parse import urlparse

import boto3
from dagster import AssetKey, AssetMaterialization, DagsterInstance
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


def _canonical_course_id(course_id: str) -> str | None:
    """Return the stripped course_id, or None if it is already canonical."""
    m = INVALID_COURSE_ID_SUFFIX_RE.search(course_id)
    return course_id[: m.start()] if m else None


def _parse_partition_key(raw_key: str) -> tuple[str, str]:
    """Split a ``course_id|source_system`` string into its two components.

    MultiPartitionKey serialises as ``<course_id>|<source_system>``.
    """
    idx = raw_key.rfind("|")
    if idx == -1:
        msg = f"Unexpected partition key format (no '|'): {raw_key!r}"
        raise ValueError(msg)
    return raw_key[:idx], raw_key[idx + 1 :]


def _build_partition_key(course_id: str, source_system: str) -> str:
    return f"{course_id}|{source_system}"


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


def reconcile(  # noqa: C901, PLR0912, PLR0915
    *,
    dry_run: bool = False,
    aws_profile: str | None = None,
    s3_bucket_override: str | None = None,
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
    canonical_keys: dict[str, str] = {}  # bad_key -> canonical_key

    for key in all_keys:
        try:
            course_id, source_system = _parse_partition_key(key)
        except ValueError:
            log.warning("Skipping unparseable partition key: %r", key)
            continue
        canonical = _canonical_course_id(course_id)
        if canonical is not None:
            bad_keys.append(key)
            canonical_keys[key] = _build_partition_key(canonical, source_system)

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

    # 1. Add canonical partition keys that do not exist yet.
    keys_to_add = [ck for ck in canonical_keys.values() if ck not in existing_key_set]
    if keys_to_add:
        log.info("Adding %d new canonical partition key(s).", len(keys_to_add))
        if not dry_run:
            instance.add_dynamic_partitions(
                PARTITIONS_DEF_NAME, partition_keys=keys_to_add
            )

    # 2. For each bad partition, re-emit materialisations with corrected
    #    partition keys and (where applicable) corrected S3 paths.
    for bad_key in bad_keys:
        canon_key = canonical_keys[bad_key]
        for asset_key in AFFECTED_ASSET_KEYS:
            records = instance.get_event_records(
                EventRecordsFilter(
                    asset_key=asset_key,
                    event_type=DagsterEventType.ASSET_MATERIALIZATION,
                    asset_partitions=[bad_key],
                )
            )
            if not records:
                continue

            log.info(
                "Processing %d materialization(s) for %s  partition %r -> %r",
                len(records),
                asset_key,
                bad_key,
                canon_key,
            )

            for record in records:
                original = record.asset_materialization
                if original is None:
                    continue

                original_meta = dict(original.metadata)
                updated_meta = dict(original_meta)

                # ----------------------------------------------------------
                # Determine whether the S3 object lives at the wrong path.
                # The object_key embeds the course_id as a directory segment;
                # if that segment carries the invalid suffix the file must be
                # copied before we can re-emit the corrected event.
                # ----------------------------------------------------------
                raw_object_key = (
                    original_meta["object_key"].value
                    if hasattr(original_meta.get("object_key"), "value")
                    else original_meta.get("object_key")
                )
                raw_path = (
                    original_meta["path"].value
                    if hasattr(original_meta.get("path"), "value")
                    else original_meta.get("path")
                )

                correct_object_key = (
                    _correct_object_key(raw_object_key) if raw_object_key else None
                )

                if correct_object_key and raw_path:
                    # Derive bucket from the materialisation path; allow
                    # override for cross-environment use.
                    bucket, _bad_s3_key = _parse_s3_url(str(raw_path))
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

                    if _s3_object_exists(s3_client, bucket, good_s3_key):
                        log.info(
                            "    S3 destination already exists, skipping copy: %s",
                            correct_path,
                        )
                    else:
                        log.info(
                            "    S3 copy: s3://%s/%s -> s3://%s/%s",
                            bucket,
                            bad_s3_key,
                            bucket,
                            good_s3_key,
                        )
                        if not dry_run:
                            s3_client.copy_object(
                                Bucket=bucket,
                                CopySource={"Bucket": bucket, "Key": bad_s3_key},
                                Key=good_s3_key,
                            )

                    # Update metadata for the re-emitted event.
                    from dagster import MetadataValue  # noqa: PLC0415

                    updated_meta["object_key"] = correct_object_key
                    updated_meta["path"] = MetadataValue.path(correct_path)
                else:
                    log.debug("    No S3 path correction needed for %s", raw_object_key)

                # Correct the course_id metadata field if present.
                raw_course_id = (
                    original_meta["course_id"].value
                    if hasattr(original_meta.get("course_id"), "value")
                    else original_meta.get("course_id")
                )
                if raw_course_id:
                    canon_course_id = _canonical_course_id(str(raw_course_id))
                    if canon_course_id:
                        from dagster import MetadataValue  # noqa: PLC0415

                        updated_meta["course_id"] = MetadataValue.text(canon_course_id)

                new_mat = AssetMaterialization(
                    asset_key=original.asset_key,
                    description=original.description,
                    metadata=updated_meta,
                    partition=canon_key,
                    tags={
                        **original.tags,
                        # Audit trail: record which bad partition this migrated from.
                        "dagster/reconciled_from_partition": bad_key,
                    },
                )
                log.info(
                    "    Re-emitting materialisation for %s under %r",
                    asset_key,
                    canon_key,
                )
                if not dry_run:
                    instance.report_runless_asset_event(new_mat)

    # 3. Delete the invalid dynamic partition keys.
    log.info("Deleting %d invalid partition key(s).", len(bad_keys))
    for bad_key in bad_keys:
        log.info("  Deleting %r", bad_key)
        if not dry_run:
            instance.delete_dynamic_partition(PARTITIONS_DEF_NAME, bad_key)

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
    args = parser.parse_args()
    reconcile(
        dry_run=args.dry_run,
        aws_profile=args.aws_profile,
        s3_bucket_override=args.s3_bucket,
    )


if __name__ == "__main__":
    main()
