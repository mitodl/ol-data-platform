"""S3 backup of the durable user_pk key map.

``int__combined__user_key_map`` is the only model in the dbt project that is STATE
rather than a pure function of its sources. It records which person key each account
natural key was assigned and, critically, WHEN -- and survivorship depends on that
ordering. Assignment order is a fact about the history of builds, not about the source
data, so it cannot be recomputed. Re-minting recovers a key only for groups whose
winner has not changed since first assignment, and a group whose winner changed is
precisely the case the map exists to survive.

Losing it re-keys the warehouse: 27 dbt models join ``dim_user.user_pk``, and the
dimensional schema grants ``select`` to ``reverse_etl``, so external systems hold those
values too. See ``docs/design/adr_durable_user_surrogate_key.md``.

What already protects the table protects it against different things:
``full_refresh=false`` stops ``dbt build --full-refresh``, and 30-day Iceberg snapshot
retention gives time travel inside a 30-day window. Neither survives a dropped table,
a bad migration, a catalog-level mistake, or anything older than 30 days. This asset
is the backstop for those.
"""

import hashlib
import json
import os
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import boto3
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Failure,
    MetadataValue,
    Output,
    asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import warehouse_env_for

# Both the source database and the destination bucket are derived from the SAME
# warehouse env, so a QA run cannot read production's map or write into production's
# bucket. This mirrors the scoping argument in ol_orchestrate.lib.iceberg_maintenance:
# an IAM denial is a safety net, not a scoping mechanism.
WAREHOUSE_ENV = warehouse_env_for(DAGSTER_ENV)
SOURCE_DATABASE = f"ol_warehouse_{WAREHOUSE_ENV}_intermediate"
MODEL_NAME = "int__combined__user_key_map"

# Defaults to the layer bucket the table already lives in. That covers every failure
# mode this backup is actually for (dropped table, bad migration, catalog mistake, an
# out-of-band truncate) but NOT loss of the bucket itself. Override the env var to point
# at a dedicated backup bucket when one exists -- no code change needed.
BACKUP_BUCKET = os.environ.get(
    "USER_KEY_MAP_BACKUP_S3_BUCKET",
    f"ol-data-lake-intermediate-{WAREHOUSE_ENV}",
)
BACKUP_PREFIX = os.environ.get(
    "USER_KEY_MAP_BACKUP_S3_PREFIX", "_backups/user_key_map"
).rstrip("/")

# The pointer object. Holds the key of the newest GOOD backup plus the row count it was
# taken at, which is what makes the monotonicity check below possible across runs.
LATEST_KEY = f"{BACKUP_PREFIX}/latest.json"


def _read_latest_manifest(s3, bucket: str) -> dict[str, Any] | None:
    """Return the previous backup's manifest, or None on the first ever run."""
    try:
        body = s3.get_object(Bucket=bucket, Key=LATEST_KEY)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(body)


@asset(
    name="user_key_map_s3_backup",
    group_name="intermediate",
    deps=[AssetKey([MODEL_NAME])],
    description=(
        "Copies int__combined__user_key_map to S3 as Parquet after each build. The map "
        "is unreproducible state -- assignment ORDER cannot be recomputed from the "
        "sources -- and losing it re-keys every user_pk in the warehouse."
    ),
    compute_kind="python",
)
def user_key_map_s3_backup(context: AssetExecutionContext) -> Output[dict[str, Any]]:
    """Snapshot the key map to S3 and update the latest-good pointer.

    Refuses to record an empty or shrunken map, so the pointer can only ever name a
    backup that is at least as complete as the one before it.
    """
    s3 = boto3.client("s3")

    frame = get_dbt_model_as_dataframe(SOURCE_DATABASE, MODEL_NAME).collect()
    row_count = frame.height

    if row_count == 0:
        msg = (
            f"{SOURCE_DATABASE}.{MODEL_NAME} returned 0 rows. Refusing to record an "
            "empty key map as a good backup -- an empty map means the table was "
            "dropped or truncated, which is the disaster this asset exists to recover "
            "from, not a state to snapshot."
        )
        raise Failure(msg)

    # The map is append-only by construction: int__combined__user_key_map uses
    # incremental_strategy='append' and only ever inserts natural keys it has not seen.
    # So the row count can only grow. A shrink means something wrote to it that should
    # not have, and recording that as `latest` would point recovery at a truncated copy.
    previous = _read_latest_manifest(s3, BACKUP_BUCKET)
    if previous and row_count < previous["row_count"]:
        msg = (
            f"Key map row count went DOWN: {previous['row_count']:,} -> {row_count:,}. "
            "The map is append-only, so this cannot happen through normal operation. "
            f"Refusing to overwrite the latest pointer, which still names "
            f"{previous['key']}. Investigate before re-running."
        )
        raise Failure(msg)

    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")
    key = f"{BACKUP_PREFIX}/dt={stamp[:10]}/user_key_map-{stamp}.parquet"

    with tempfile.TemporaryDirectory() as tmp:
        local = Path(tmp) / "user_key_map.parquet"
        frame.write_parquet(local, compression="zstd")
        payload = local.read_bytes()
        checksum = hashlib.sha256(payload).hexdigest()
        size_bytes = len(payload)
        context.log.info(
            "Uploading %s rows (%.1f MiB) to s3://%s/%s",
            f"{row_count:,}",
            size_bytes / 1024 / 1024,
            BACKUP_BUCKET,
            key,
        )
        s3.put_object(Body=payload, Bucket=BACKUP_BUCKET, Key=key)

    # Read back rather than trusting the write. A backup nobody has verified is a
    # belief, not a backup.
    head = s3.head_object(Bucket=BACKUP_BUCKET, Key=key)
    if head["ContentLength"] != size_bytes:
        msg = (
            f"Verification failed for s3://{BACKUP_BUCKET}/{key}: uploaded "
            f"{size_bytes} bytes, S3 reports {head['ContentLength']}."
        )
        raise Failure(msg)

    manifest = {
        "key": key,
        "bucket": BACKUP_BUCKET,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "sha256": checksum,
        "taken_at": stamp,
        "source": f"{SOURCE_DATABASE}.{MODEL_NAME}",
        "warehouse_env": WAREHOUSE_ENV,
    }
    # Written only after the object above is verified, so `latest` can never name a key
    # that failed to upload.
    s3.put_object(
        Body=json.dumps(manifest, indent=2).encode(),
        Bucket=BACKUP_BUCKET,
        Key=LATEST_KEY,
        ContentType="application/json",
    )

    return Output(
        manifest,
        metadata={
            "rows_backed_up": MetadataValue.int(row_count),
            "rows_added_since_last_backup": MetadataValue.int(
                row_count - previous["row_count"] if previous else row_count
            ),
            "size_mib": MetadataValue.float(round(size_bytes / 1024 / 1024, 2)),
            "s3_uri": MetadataValue.text(f"s3://{BACKUP_BUCKET}/{key}"),
            "sha256": MetadataValue.text(checksum),
            "warehouse_env": MetadataValue.text(WAREHOUSE_ENV),
        },
    )
