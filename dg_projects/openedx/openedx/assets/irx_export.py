"""The nightly IRx drop, cut from the irx__ models instead of queried live.

legacy_openedx queries each deployment's edxapp MySQL every night and uploads
six CSVs for MIT Institutional Research. This writes the same six files, with
the same headers and value formatting, from tables the warehouse already
maintains. The column-level contract is in
src/ol_dbt/models/external/IRX_SIMEON_MAPPING.md.
"""

import hashlib
import io
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    DailyPartitionsDefinition,
    DataVersion,
    Failure,
    MaterializeResult,
    MetadataValue,
    get_dagster_logger,
    multi_asset,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import load_dbt_model_table, scan_dbt_model_table
from upath import UPath

IRX_EXPORT_GROUP = "irx_export"
IRX_GLUE_DATABASE = "ol_warehouse_production_external"

# end_offset=1 makes today the latest partition, so the scheduled run on
# 2026-09-12 writes {deployment}/20260912/, which is the date legacy_openedx
# names the same night's drop by.
IRX_EXPORT_PARTITIONS = DailyPartitionsDefinition(start_date="2026-09-11", end_offset=1)

IRX_EXPORT_ROOTS = {
    "production": "s3://ol-irx-partners-storage-production",
    "qa": "s3://ol-irx-partners-storage-qa",
}
IRX_EXPORT_SANDBOX_ROOT = "s3://ol-devops-sandbox/pipeline-storage/irx-export"

LEGACY_DATETIME = "%Y-%m-%d %H:%M:%S"


@dataclass(frozen=True)
class IrxExportFile:
    """One legacy CSV and the irx__ model it is cut from."""

    name: str
    model: str
    columns: tuple[str, ...]
    renames: Mapping[str, str] = field(default_factory=dict)
    required: tuple[str, ...] = ()


IRX_EXPORT_FILES = (
    IrxExportFile(
        name="users_query",
        model="auth_user",
        columns=(
            "id",
            "username",
            "first_name",
            "last_name",
            "email",
            "is_staff",
            "is_active",
            "is_superuser",
            "last_login",
            "date_joined",
            "course_id",
        ),
    ),
    IrxExportFile(
        name="enrollment_query",
        model="student_courseenrollment",
        columns=("id", "user_id", "course_id", "created", "is_active", "mode"),
    ),
    IrxExportFile(
        name="role_query",
        model="student_courseaccessrole",
        columns=("id", "user_id", "org", "course_id", "role"),
    ),
    IrxExportFile(
        name="role_users",
        model="django_comment_client_role_users",
        columns=("id", "user_id", "org", "course_id", "role"),
        renames={"name": "role"},
        # Legacy reached org through an inner join on
        # organizations_organizationcourse, so a course run with no organization
        # link delivered no forum-role rows at all. The model keeps those rows.
        required=("org",),
    ),
    IrxExportFile(
        name="studentmodule_query",
        model="courseware_studentmodule",
        columns=(
            "id",
            "module_type",
            "module_id",
            "student_id",
            "state",
            "grade",
            "created",
            "modified",
            "max_grade",
            "done",
            "course_id",
        ),
    ),
)


def irx_model_name(deployment: str, model: str) -> str:
    return f"irx__{deployment}__openedx__mysql__{model}"


def legacy_csv_columns(schema: pl.Schema, columns: Sequence[str]) -> list[pl.Expr]:
    """Project columns so polars writes them the way legacy's csv.DictWriter did.

    Legacy wrote MySQL rows through Python's csv module: booleans arrive as
    tinyint 1/0, datetimes as str(datetime), which drops the fraction when the
    microseconds are zero, and an empty string is written the same as NULL.
    Left alone, polars writes true/false, a fixed-width fraction, and quotes
    empty strings as "".
    """
    exprs = []
    for name in columns:
        column, dtype = pl.col(name), schema[name]
        if dtype == pl.Boolean:
            column = column.cast(pl.Int8)
        elif isinstance(dtype, pl.Datetime):
            column = (
                pl.when(column.dt.microsecond() == 0)
                .then(column.dt.strftime(LEGACY_DATETIME))
                .otherwise(column.dt.strftime(f"{LEGACY_DATETIME}%.6f"))
            )
        elif dtype == pl.String:
            column = pl.when(column == "").then(None).otherwise(column)
        exprs.append(column.alias(name))
    return exprs


class _DigestingWriter(io.RawIOBase):
    """Hash and count bytes on their way to the object store."""

    def __init__(self, sink: Any):
        self._sink = sink
        self.digest = hashlib.sha256()
        self.size = 0

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        self.digest.update(data)
        self.size += len(data)
        return self._sink.write(data)


def write_legacy_csv(frame: pl.LazyFrame, destination: UPath) -> tuple[str, int]:
    """Stream a frame to the drop as CSV and return its sha256 and size in bytes.

    Streamed, never collected: studentmodule_query.csv runs to 59 GB for mitx,
    and legacy_openedx needed a 32Gi memory limit for loading it whole.
    """
    with destination.open("wb") as sink:
        writer = _DigestingWriter(sink)
        frame.sink_csv(writer, line_terminator="\r\n")
    return writer.digest.hexdigest(), writer.size


def build_irx_export_asset(deployment: str) -> AssetsDefinition:
    """Build the nightly IRx drop for one deployment as a single multi-asset.

    One op rather than one asset per file, so all six files in a drop are cut
    against the same course list, the way legacy's single job was.
    """
    course_ids_key = AssetKey([deployment, IRX_EXPORT_GROUP, "course_ids"])
    specs = [
        AssetSpec(
            key=course_ids_key,
            description="course_ids.csv: the course runs the Open edX API lists.",
            code_version="irx_export_v1",
        ),
        *(
            AssetSpec(
                key=AssetKey([deployment, IRX_EXPORT_GROUP, export.name]),
                deps=[AssetKey(["external", irx_model_name(deployment, export.model)])],
                description=f"{export.name}.csv in the nightly IRx drop.",
                code_version="irx_export_v1",
            )
            for export in IRX_EXPORT_FILES
        ),
    ]

    @multi_asset(
        name=f"{deployment}_irx_export",
        specs=specs,
        group_name=IRX_EXPORT_GROUP,
        partitions_def=IRX_EXPORT_PARTITIONS,
        required_resource_keys={"openedx"},
    )
    def irx_export(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
        root = UPath(IRX_EXPORT_ROOTS.get(DAGSTER_ENV, IRX_EXPORT_SANDBOX_ROOT))
        drop_date = context.partition_time_window.start.strftime("%Y%m%d")
        drop = root / deployment / drop_date

        # The live API list, not the course-run partition set: the sensor that
        # maintains those partitions only ever adds, so they accumulate runs
        # the LMS no longer lists.
        course_ids = [
            course["id"]
            for page in context.resources.openedx.client.get_edx_course_ids()
            for course in page
        ]
        if not course_ids:
            raise Failure(
                description=(
                    f"The {deployment} course API listed no course runs, so every "
                    "file in the drop would be empty."
                )
            )
        yield _export(
            course_ids_key,
            pl.LazyFrame({"course_id": course_ids}, schema={"course_id": pl.String}),
            drop / "course_ids.csv",
            {"row_count": len(course_ids)},
        )

        for export in IRX_EXPORT_FILES:
            table_name = irx_model_name(deployment, export.model)
            table = load_dbt_model_table(IRX_GLUE_DATABASE, table_name)
            # Pinned so the row count and the file are read from the same
            # snapshot even if dbt rebuilds the model mid-export.
            snapshot_id = table.current_snapshot().snapshot_id
            frame = (
                scan_dbt_model_table(table, snapshot_id)
                .rename(dict(export.renames))
                .filter(pl.col("course_id").is_in(course_ids))
                .drop_nulls(list(export.required))
            )
            yield _export(
                AssetKey([deployment, IRX_EXPORT_GROUP, export.name]),
                frame.select(
                    legacy_csv_columns(frame.collect_schema(), export.columns)
                ),
                drop / f"{export.name}.csv",
                {
                    "row_count": frame.select(pl.len()).collect().item(),
                    "source_table": f"{IRX_GLUE_DATABASE}.{table_name}",
                    "source_snapshot_id": str(snapshot_id),
                },
            )

    return irx_export


def _export(
    key: AssetKey,
    frame: pl.LazyFrame,
    destination: UPath,
    metadata: Mapping[str, Any],
) -> MaterializeResult:
    sha256, size = write_legacy_csv(frame, destination)
    get_dagster_logger().info(
        "Wrote %s rows (%d bytes) to %s", metadata["row_count"], size, destination
    )
    return MaterializeResult(
        asset_key=key,
        data_version=DataVersion(sha256),
        metadata={
            **metadata,
            "path": MetadataValue.path(str(destination)),
            "size_bytes": size,
            "sha256": sha256,
        },
    )
