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

    Each table is read fully into memory, so tables larger than
    ``--max-table-bytes`` are reported and left as JSONL instead of being
    attempted.  The check runs before the Glue entry is touched.
"""

import logging
import re
import sys
from typing import TYPE_CHECKING, Annotated, Any

import boto3
import cyclopts
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
import pyarrow.json as pj
from pyiceberg.catalog.glue import GlueCatalog

if TYPE_CHECKING:
    import botocore.client

log = logging.getLogger(__name__)

app = cyclopts.App(help="Migrate legacy JSONL raw-layer Glue tables to Iceberg format.")

_RAW_DATABASE = "ol_warehouse_{env}_raw"
_AWS_REGION = "us-east-1"

# Migration reads a table's whole JSONL body into an Arrow table in memory, so
# one oversized table can OOM a run that would otherwise convert thousands of
# small ones. Tables above this are reported and left as JSONL rather than
# attempted. 8 GiB of compressed JSON already expands well past that in Arrow;
# raise it deliberately, on a machine sized for it, for a specific table.
#
# This is not hypothetical: in ol_warehouse_qa_raw (2026-09-08),
# raw__irx__edxorg__s3__course_studentmodules is 652 GB across 3,114 files --
# 97% of the 675 GB behind that database's whole legacy prefix, and read by no
# dbt source. Without a guard it is the first table big enough to kill the run.
_DEFAULT_MAX_TABLE_BYTES = 8 * 1024**3

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
) -> tuple[list[str], int]:
    """List JSON/JSONL files at location.

    Returns '{bucket}/{key}' paths for pa.fs, plus their total size in bytes so
    the caller can refuse a table too large to read into memory.
    """
    path = location.removeprefix("s3://")
    bucket, _, prefix = path.partition("/")
    prefix = prefix.rstrip("/") + "/"

    files: list[str] = []
    total_bytes = 0
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
                total_bytes += obj["Size"]
    return files, total_bytes


# Every scalar Glue type found on the legacy JSONL tables in qa and production
# raw (2026-09-10). array and struct columns are left to inference because
# their Glue types are not trustworthy: production's
# raw__thirdparty__github__workflow_runs declares pull_requests as
# array<string> and holds objects.
_GLUE_SCALAR_TYPES = {
    "string": pa.large_utf8(),
    "boolean": pa.bool_(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    # Glue could only type the column as null, so it holds no values; Iceberg
    # v2 has no null type, and string is the least committal concrete one.
    "null": pa.large_utf8(),
}
# An omitted scale means 0; two QA columns are declared as decimal(38).
_GLUE_DECIMAL = re.compile(r"decimal\((\d+)(?:,\s*(\d+))?\)")


def _arrow_type(glue_type: str) -> pa.DataType | None:
    if match := _GLUE_DECIMAL.fullmatch(glue_type):
        return pa.decimal128(int(match[1]), int(match[2] or 0))
    return _GLUE_SCALAR_TYPES.get(glue_type)


def _empty_schema(columns: list[dict[str, Any]]) -> pa.Schema:
    """Build the schema for a table with no data files to infer one from."""
    return pa.schema(
        [
            pa.field(c["Name"], _arrow_type(c["Type"]) or pa.large_utf8())
            for c in columns
        ]
    )


def _parse_options(columns: list[dict[str, Any]]) -> pj.ParseOptions:
    """Build parse options that take scalar column types from Glue, not inference.

    pyarrow infers a column's type from the first block it reads. A decimal
    column whose early rows are whole numbers becomes int64 and fails on the
    first fractional value ("couldn't parse: 987.65"), and a column that is null
    in every row becomes pa.null(), which Iceberg v2 rejects. Glue already
    carries each column's type, so the scalar ones are declared up front.

    Declaring every column as string would be simpler and does not work: the
    reader refuses a JSON number or boolean in a string column.
    """
    declared = pa.schema(
        [pa.field(c["Name"], t) for c in columns if (t := _arrow_type(c["Type"]))]
    )
    return pj.ParseOptions(explicit_schema=declared, unexpected_field_behavior="infer")


def _without_null(arrow_type: pa.DataType) -> pa.DataType:
    """Replace pa.null() with string, including inside structs and lists."""
    if pa.types.is_null(arrow_type):
        return pa.large_utf8()
    if pa.types.is_struct(arrow_type):
        return pa.struct(
            [f.with_type(_without_null(f.type)) for f in arrow_type.fields]
        )
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(
            arrow_type.value_field.with_type(_without_null(arrow_type.value_type))
        )
    if pa.types.is_list(arrow_type):
        return pa.list_(
            arrow_type.value_field.with_type(_without_null(arrow_type.value_type))
        )
    return arrow_type


def _read_typed(
    files: list[str],
    columns: list[dict[str, Any]],
    arrow_fs: pafs.FileSystem,
) -> pa.Table:
    # File by file: pyarrow.dataset's JsonFileFormat ignores explicit_schema
    # (pyarrow 25), so a dataset scan would infer every column regardless. A
    # key Glue never declared can infer differently per file; permissive
    # concat widens it.
    options = _parse_options(columns)
    tables = []
    for path in files:
        with arrow_fs.open_input_stream(path) as stream:
            tables.append(pj.read_json(stream, parse_options=options))
    return pa.concat_tables(tables, promote_options="permissive")


def _has_case_collision(schema: pa.Schema) -> bool:
    return len({name.lower() for name in schema.names}) < len(schema.names)


def _read_jsonl(
    files: list[str],
    columns: list[dict[str, Any]],
    arrow_fs: pafs.FileSystem,
) -> pa.Table:
    """Read a table with Glue's column types, falling back to inference.

    Glue's types beat inference on most tables but are sometimes wrong, and a
    declared type the data contradicts fails the read outright. Falling back to
    plain inference, which is all this script did before, keeps every table
    that converted before converting. Salesforce tables take the fallback too:
    their JSON keys (Id) differ from Glue's lowercased names (id) only by case,
    so a typed read yields both columns.
    """
    table = None
    try:
        table = _read_typed(files, columns, arrow_fs)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
        log.info("  Glue-typed read failed, inferring types instead: %s", exc)
    if table is not None and _has_case_collision(table.schema):
        log.info("  JSON keys match Glue columns only by case; inferring types")
        table = None
    if table is None:
        table = ds.dataset(files, format="json", filesystem=arrow_fs).to_table()
    # A key Glue never declared, or a field nested in an array or struct, can
    # still be null in every row and infer as pa.null(); give it the same
    # string type as a Glue null column.
    return table.cast(
        pa.schema([f.with_type(_without_null(f.type)) for f in table.schema])
    )


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
    max_table_bytes: int,
) -> bool:
    """Migrate a single JSONL Glue table to Iceberg.  Returns True on success."""
    resp = glue.get_table(DatabaseName=database, Name=table_name)
    original_def = resp["Table"]
    location = original_def["StorageDescriptor"]["Location"].rstrip("/")

    files, total_bytes = _list_json_files(s3, location)
    log.info(
        "%s  location=%s  files=%d  bytes=%d",
        table_name,
        location,
        len(files),
        total_bytes,
    )

    # Checked before the Glue entry is deleted, so refusing costs nothing.
    if total_bytes > max_table_bytes:
        log.warning(
            "  %.2f GB exceeds the %.2f GB limit — leaving as JSONL. "
            "Re-run with --table %s --max-table-bytes N on a host sized for it.",
            total_bytes / 1024**3,
            max_table_bytes / 1024**3,
            table_name,
        )
        return False

    glue_cols = original_def["StorageDescriptor"].get("Columns", [])

    if dry_run:
        log.info("  [dry-run] would migrate %d file(s)", len(files))
        if files:
            sample = _read_jsonl(files[:1], glue_cols, arrow_fs)
            log.info("  [dry-run] schema from first file: %s", sample.schema)
            log.info("  [dry-run] rows in first file: %d", len(sample))
        return True

    # ── Read ──────────────────────────────────────────────────────────────────
    if not files:
        if not glue_cols:
            log.warning("  no data files and no column definitions — skipping")
            return False
        log.info(
            "  empty table; building schema from %d Glue column(s)", len(glue_cols)
        )
        arrow_schema = _empty_schema(glue_cols)
        arrow_table = arrow_schema.empty_table()
    else:
        try:
            arrow_table = _read_jsonl(files, glue_cols, arrow_fs)
            arrow_schema = arrow_table.schema
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
    max_table_bytes: Annotated[
        int,
        cyclopts.Parameter(
            help="Skip tables whose JSONL body exceeds this many bytes "
            "(migration reads each table fully into memory)"
        ),
    ] = _DEFAULT_MAX_TABLE_BYTES,
) -> None:
    """Migrate legacy JSONL raw-layer Glue tables to Apache Iceberg format."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    database = _RAW_DATABASE.format(env=env)
    log.info(
        "database=%s  dry_run=%s  table=%s  max_table_bytes=%d",
        database,
        dry_run,
        table or "(all)",
        max_table_bytes,
    )

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
                max_table_bytes=max_table_bytes,
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
