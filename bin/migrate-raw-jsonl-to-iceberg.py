#!/usr/bin/env python3
"""Migrate legacy JSONL tables in the raw Glue layer to Apache Iceberg format.

Legacy tables in ol_warehouse_{env}_raw were written as newline-delimited JSON
by Airbyte before the platform standardized on Iceberg format.  This script
converts those tables so they are accessible via the StarRocks
ol_data_lake_iceberg external catalog.

Usage::

    # Dry run (default) — shows what would be migrated without making changes
    uv run python bin/migrate-raw-jsonl-to-iceberg.py --env qa

    # Migrate all legacy tables in QA
    uv run python bin/migrate-raw-jsonl-to-iceberg.py --env qa --no-dry-run

    # Migrate a single table
    uv run python bin/migrate-raw-jsonl-to-iceberg.py \\
        --env qa --table raw__edxorg__s3__mitx_courses --no-dry-run

Safety:

    The original Glue table definition is saved before deletion.  If Iceberg
    table creation fails, the original JSONL entry is automatically restored in
    Glue so the table remains accessible.  JSONL files in S3 are never deleted
    by this script.
"""

import logging
import sys
from typing import TYPE_CHECKING, Annotated, Any

import boto3
import cyclopts
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from pyiceberg.catalog.glue import GlueCatalog

if TYPE_CHECKING:
    import botocore.client

log = logging.getLogger(__name__)

app = cyclopts.App(help="Migrate legacy JSONL raw-layer Glue tables to Iceberg format.")

_RAW_DATABASE = "ol_warehouse_{env}_raw"
_AWS_REGION = "us-east-1"

# Fields accepted by Glue CreateTable's TableInput (excludes server-managed fields
# like CreateTime, UpdateTime, CreatedBy, IsRegisteredWithLakeFormation, etc.)
_TABLE_INPUT_FIELDS = frozenset(
    {
        "Name",
        "Description",
        "Owner",
        "LastAccessTime",
        "LastAnalyzedTime",
        "Retention",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "TableType",
        "Parameters",
        "TargetTable",
        "ViewDefinition",
    }
)


def _is_iceberg(table: dict[str, Any]) -> bool:
    return table.get("Parameters", {}).get("table_type", "").upper() == "ICEBERG"


def _is_legacy_jsonl(table: dict[str, Any]) -> bool:
    """Return True only for tables with the JSONL/text format written by old Airbyte.

    Checks for TextInputFormat + a JSON SerDe to avoid accidentally migrating
    tables in unexpected formats (Parquet, CSV, ORC, etc.).
    """
    sd = table.get("StorageDescriptor", {})
    input_format = sd.get("InputFormat", "")
    serde_lib = sd.get("SerdeInfo", {}).get("SerializationLibrary", "")
    return (
        not _is_iceberg(table)
        and "TextInputFormat" in input_format
        and ("json" in serde_lib.lower())
    )


def _to_table_input(glue_table: dict[str, Any]) -> dict[str, Any]:
    """Strip server-managed fields so the dict can be passed to Glue CreateTable."""
    return {k: v for k, v in glue_table.items() if k in _TABLE_INPUT_FIELDS}


def _list_json_files(
    s3_client: "botocore.client.S3",
    location: str,
) -> list[str]:
    """List JSON/JSONL files at location; returns '{bucket}/{key}' paths for pa.fs."""
    path = location.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")
    prefix = prefix.rstrip("/") + "/"

    files: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if (
                key.endswith((".jsonl", ".json", ".jsonl.gz"))
                # Ignore Iceberg subdirs left by a prior partial run
                and "/metadata/" not in key
                and "/data/" not in key
            ):
                files.append(f"{bucket}/{key}")
    return files


def _schema_from_glue_columns(columns: list[dict[str, Any]]) -> pa.Schema:
    """Build a minimal PyArrow schema from Glue column definitions.

    Used only for empty tables that have no data files to infer a schema from.
    Every field defaults to large_utf8 (string) since Glue type fidelity for
    legacy JSONL tables is often poor.
    """
    return pa.schema([pa.field(col["Name"], pa.large_utf8()) for col in columns])


def _restore_glue_table(
    glue: "botocore.client.Glue",
    database: str,
    table_def: dict[str, Any],
) -> None:
    """Re-register an original Glue table definition after a failed migration."""
    glue.create_table(DatabaseName=database, TableInput=_to_table_input(table_def))
    log.info("  restored original JSONL Glue entry")


def _migrate_one(  # noqa: PLR0913, C901, PLR0912
    *,
    glue: "botocore.client.Glue",
    s3: "botocore.client.S3",
    arrow_fs: pafs.S3FileSystem,
    catalog: GlueCatalog,
    database: str,
    table_name: str,
    dry_run: bool,
) -> bool:
    """Migrate a single JSONL Glue table to Iceberg.  Returns True on success."""
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    original_def = resp["Table"]
    location = original_def["StorageDescriptor"]["Location"].rstrip("/")

    files = _list_json_files(s3, location)
    log.info("%s  location=%s  files=%d", table_name, location, len(files))

    if dry_run:
        log.info("  [dry-run] would migrate %d file(s)", len(files))
        if files:
            sample = ds.dataset(files[:1], format="json", filesystem=arrow_fs)
            log.info("  [dry-run] inferred schema from sample: %s", sample.schema)
            log.info("  [dry-run] estimated rows: %d", sample.count_rows())
        return True

    # ── Read ──────────────────────────────────────────────────────────────────
    if not files:
        glue_cols = original_def["StorageDescriptor"].get("Columns", [])
        if not glue_cols:
            log.warning("  no data files and no column definitions — skipping")
            return False
        log.info(
            "  empty table; building schema from %d Glue column(s)", len(glue_cols)
        )
        arrow_schema = _schema_from_glue_columns(glue_cols)
        arrow_table = arrow_schema.empty_table()
    else:
        try:
            dataset = ds.dataset(files, format="json", filesystem=arrow_fs)
            arrow_schema = dataset.schema
            arrow_table = dataset.to_table()
        except Exception:
            log.exception("  failed to read JSONL data — skipping table")
            return False
        log.info("  read %d row(s), %d field(s)", len(arrow_table), len(arrow_schema))

    source_count = len(arrow_table)

    # ── Safe cutover: delete JSONL entry, create Iceberg, restore on failure ──
    glue.delete_table(DatabaseName=database, Name=table_name)
    log.info("  deleted JSONL Glue entry")

    try:
        iceberg = catalog.create_table(
            identifier=(database, table_name),
            schema=arrow_schema,
            location=location,
        )
        if source_count:
            iceberg.append(arrow_table)

        # Validate row count from Iceberg snapshot metadata
        snapshot = iceberg.current_snapshot()
        if snapshot and snapshot.summary:
            iceberg_count = int(snapshot.summary.get("total-records", "0"))
            if iceberg_count != source_count:
                log.warning(
                    "  row count mismatch: source=%d iceberg=%d",
                    source_count,
                    iceberg_count,
                )
            else:
                log.info("  validated: %d rows written", iceberg_count)
        else:
            log.info("  created Iceberg table (empty)")

    except Exception:
        log.exception(
            "  Iceberg creation failed — attempting to restore original entry"
        )
        try:
            _restore_glue_table(glue, database, original_def)
        except Exception:
            log.exception(
                "  CRITICAL: could not restore %s.%s — "
                "table definition: %s  data still at: %s",
                database,
                table_name,
                _to_table_input(original_def),
                location,
            )
        return False

    return True


@app.default
def main(
    env: Annotated[str, cyclopts.Parameter(help="Environment: qa or production")],
    *,
    dry_run: Annotated[
        bool,
        cyclopts.Parameter(help="Preview changes without modifying Glue or S3"),
    ] = True,
    table: Annotated[
        str | None,
        cyclopts.Parameter(help="Migrate only this specific Glue table name"),
    ] = None,
    aws_region: Annotated[
        str,
        cyclopts.Parameter(help="AWS region for Glue and S3"),
    ] = _AWS_REGION,
) -> None:
    """Migrate legacy JSONL raw-layer Glue tables to Apache Iceberg format."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    database = _RAW_DATABASE.format(env=env)
    log.info("database=%s  dry_run=%s  table=%s", database, dry_run, table or "(all)")

    glue = boto3.client("glue", region_name=aws_region)
    s3 = boto3.client("s3", region_name=aws_region)
    catalog = GlueCatalog(name=database, **{"region_name": aws_region})
    arrow_fs = pafs.S3FileSystem(region=aws_region)

    # ── Collect candidate tables ───────────────────────────────────────────
    if table:
        resp = glue.get_table(DatabaseName=database, Name=table)
        all_tables = [resp["Table"]]
    else:
        all_tables = []
        paginator = glue.get_paginator("get_tables")
        for page in paginator.paginate(DatabaseName=database):
            all_tables.extend(page["TableList"])

    jsonl_tables = [t for t in all_tables if _is_legacy_jsonl(t)]
    unknown_tables = [
        t for t in all_tables if not _is_iceberg(t) and not _is_legacy_jsonl(t)
    ]

    log.info(
        "tables total=%d  jsonl=%d  iceberg=%d  unknown=%d",
        len(all_tables),
        len(jsonl_tables),
        len(all_tables) - len(jsonl_tables) - len(unknown_tables),
        len(unknown_tables),
    )
    for t in unknown_tables:
        sd = t.get("StorageDescriptor", {})
        log.warning(
            "skipping %s — non-Iceberg, non-JSONL format (InputFormat=%s, SerdeLib=%s)",
            t["Name"],
            sd.get("InputFormat", ""),
            sd.get("SerdeInfo", {}).get("SerializationLibrary", ""),
        )

    # ── Migrate ────────────────────────────────────────────────────────────
    succeeded = failed = skipped = 0
    for t in jsonl_tables:
        name = t["Name"]
        log.info("── %s ──", name)
        try:
            ok = _migrate_one(
                glue=glue,
                s3=s3,
                arrow_fs=arrow_fs,
                catalog=catalog,
                database=database,
                table_name=name,
                dry_run=dry_run,
            )
            if ok:
                succeeded += 1
            else:
                skipped += 1
        except Exception:
            log.exception("unexpected error migrating %s", name)
            failed += 1

    log.info(
        "complete: succeeded=%d  skipped=%d  failed=%d  dry_run=%s",
        succeeded,
        skipped,
        failed,
        dry_run,
    )
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    app()
