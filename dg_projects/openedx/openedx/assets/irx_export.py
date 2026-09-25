"""The nightly IRx drop, cut from the irx__ models instead of queried live.

legacy_openedx queries each deployment's edxapp MySQL every night and uploads
six CSVs for MIT Institutional Research, plus a mongodump of the forum database.
This writes the same six CSVs, with the same headers and value formatting, from
tables the warehouse already maintains. The forum is delivered as a flat
Parquet export of the irx__ model instead of a reconstructed Mongo dump: Mongo
has not backed the forum since the forum-v2 cutover, so there is no dump shape
left to match, and IRx adapts their tooling to the columns we actually have.
The column-level contract is in src/ol_dbt/models/external/IRX_SIMEON_MAPPING.md.
"""

import hashlib
import io
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

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

MANIFEST_NAME = "_MANIFEST.json"
MANIFEST_FILE_FIELDS = ("row_count", "size_bytes", "sha256")

FORUM_CONTENTS_MODEL = "forum_contents"


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
    """Hash and count bytes and CSV records on their way to the object store.

    A record ends at a CRLF outside quotes. polars quotes any field holding a
    CR, LF or quote, and doubles the quotes inside it, so splitting on quotes
    alternates between unquoted and quoted text. Both that state and a CR that
    ends one chunk carry over to the next write.
    """

    def __init__(self, sink: Any):
        self._sink = sink
        self._in_quotes = False
        self._pending_cr = False
        self.digest = hashlib.sha256()
        self.size = 0
        self.records = 0

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        self.digest.update(data)
        self.size += len(data)
        for index, segment in enumerate(bytes(data).split(b'"')):
            if index:
                self._in_quotes = not self._in_quotes
                self._pending_cr = False
            if self._in_quotes:
                continue
            if self._pending_cr and segment[:1] == b"\n":
                self.records += 1
            self.records += segment.count(b"\r\n")
            if segment:
                self._pending_cr = segment.endswith(b"\r")
        return self._sink.write(data)


def write_legacy_csv(frame: pl.LazyFrame, destination: UPath) -> tuple[str, int, int]:
    """Stream a frame to the drop as CSV; return its sha256, size, and row count.

    Streamed, never collected: studentmodule_query.csv runs to 59 GB for mitx,
    and legacy_openedx needed a 32Gi memory limit for loading it whole. Row
    count is counted off the bytes as they're written (minus the header line)
    rather than from a second full scan of the frame.
    """
    with destination.open("wb") as sink:
        writer = _DigestingWriter(sink)
        frame.sink_csv(writer, line_terminator="\r\n")
    return writer.digest.hexdigest(), writer.size, writer.records - 1


class _HashingWriter(io.RawIOBase):
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


def write_parquet(frame: pl.LazyFrame, destination: UPath) -> tuple[str, int, int]:
    """Stream a frame to the drop as Parquet; return its sha256, size, and row count.

    Streamed, never collected, same as write_legacy_csv. Row count comes from a
    separate `len()` aggregate rather than the byte-level counting
    _DigestingWriter does for CSV: Parquet's row groups do not delimit records
    the way CRLFs do, but a count is a running total the engine streams
    through in the same bounded memory as the write itself.
    """
    row_count = frame.select(pl.len()).collect().item()
    with destination.open("wb") as sink:
        writer = _HashingWriter(sink)
        frame.sink_parquet(writer)
    return writer.digest.hexdigest(), writer.size, row_count


def build_irx_export_asset(deployment: str) -> AssetsDefinition:
    """Build the nightly IRx drop for one deployment as a single multi-asset.

    One op rather than one asset per file, so all six files in a drop are cut
    against the same course list, the way legacy's single job was.
    """
    course_ids_key = AssetKey([deployment, IRX_EXPORT_GROUP, "course_ids"])
    forum_key = AssetKey([deployment, IRX_EXPORT_GROUP, FORUM_CONTENTS_MODEL])
    manifest_key = AssetKey([deployment, IRX_EXPORT_GROUP, "manifest"])
    file_specs = [
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
        AssetSpec(
            key=forum_key,
            deps=[
                AssetKey(["external", irx_model_name(deployment, FORUM_CONTENTS_MODEL)])
            ],
            description=(
                "forum_contents.parquet: every forum post in the irx__ model's "
                "own columns, not a reconstructed Mongo document."
            ),
            code_version="irx_export_v1",
        ),
    ]
    specs = [
        *file_specs,
        AssetSpec(
            key=manifest_key,
            deps=[spec.key for spec in file_specs],
            description=(
                f"{MANIFEST_NAME}: every file in the drop with its row count, size "
                "and sha256. Written last, so its presence means the drop is "
                "complete."
            ),
            code_version="irx_export_v1",
        ),
    ]

    @multi_asset(
        name=f"{deployment}_irx_export",
        specs=specs,
        group_name=IRX_EXPORT_GROUP,
        partitions_def=IRX_EXPORT_PARTITIONS,
        required_resource_keys={"openedx"},
        # Two overlapping runs of one drop (a manual re-materialize during the
        # nightly run) would each write files over the other's, and the first to
        # finish would write a manifest vouching for hashes the other replaced.
        # As with openedx_course_export, naming the pool only makes the limit
        # settable: `irx_export_<deployment>` needs a slot limit of 1 on the
        # instance (Deployment -> Concurrency) before the runs are serialized.
        pool=f"irx_export_{deployment}",
    )
    def irx_export(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
        root = UPath(IRX_EXPORT_ROOTS.get(DAGSTER_ENV, IRX_EXPORT_SANDBOX_ROOT))
        drop_date = context.partition_time_window.start.strftime("%Y%m%d")
        drop = root / deployment / drop_date
        # A re-run rewrites the files in place. Take the old manifest down first,
        # so a re-run that fails partway leaves no manifest vouching for a mix of
        # old and new files.
        manifest_path = drop / MANIFEST_NAME
        manifest_path.unlink(missing_ok=True)
        # s3fs deletes through DeleteObjects and drops per-key errors, so a
        # denied delete returns as if it had worked.
        if manifest_path.exists():
            raise Failure(
                description=(
                    f"Could not delete {manifest_path}; it would vouch for a drop "
                    "this run is about to rewrite."
                )
            )
        delivered: dict[str, MaterializeResult] = {}

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
        delivered["course_ids.csv"] = _export(
            course_ids_key,
            write_legacy_csv,
            pl.LazyFrame({"course_id": course_ids}, schema={"course_id": pl.String}),
            drop / "course_ids.csv",
            {},
        )
        yield delivered["course_ids.csv"]

        for export in IRX_EXPORT_FILES:
            frame, metadata = _scan_pinned(irx_model_name(deployment, export.model))
            frame = (
                frame.rename(dict(export.renames))
                .filter(pl.col("course_id").is_in(course_ids))
                .drop_nulls(list(export.required))
            )
            file_name = f"{export.name}.csv"
            delivered[file_name] = _export(
                AssetKey([deployment, IRX_EXPORT_GROUP, export.name]),
                write_legacy_csv,
                frame.select(
                    legacy_csv_columns(frame.collect_schema(), export.columns)
                ),
                drop / file_name,
                metadata,
            )
            yield delivered[file_name]

        # Not cut to the course list: legacy dumped the whole forum database,
        # posts in runs the LMS no longer lists included.
        frame, metadata = _scan_pinned(irx_model_name(deployment, FORUM_CONTENTS_MODEL))
        delivered["forum_contents.parquet"] = _export(
            forum_key,
            write_parquet,
            frame,
            drop / "forum_contents.parquet",
            metadata,
        )
        yield delivered["forum_contents.parquet"]

        manifest = build_manifest(deployment, drop_date, context.run.run_id, delivered)
        yield _write_manifest(manifest_key, manifest, manifest_path)

    return irx_export


def _scan_pinned(table_name: str) -> tuple[pl.LazyFrame, dict[str, str]]:
    """Scan an irx table at its current snapshot.

    Pinned so the row count and the file are read from the same snapshot even
    if dbt rebuilds the model mid-export.
    """
    table = load_dbt_model_table(IRX_GLUE_DATABASE, table_name)
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise Failure(
            description=(
                f"{IRX_GLUE_DATABASE}.{table_name} has no snapshot, so there is "
                "nothing to export."
            )
        )
    return scan_dbt_model_table(table, snapshot.snapshot_id), {
        "source_table": f"{IRX_GLUE_DATABASE}.{table_name}",
        "source_snapshot_id": str(snapshot.snapshot_id),
    }


def build_manifest(
    deployment: str,
    drop_date: str,
    run_id: str,
    delivered: Mapping[str, MaterializeResult],
) -> dict[str, Any]:
    """Describe a finished drop from what its files' materializations recorded.

    Built from the metadata each file's write already computed, not by reading
    the files back.
    """
    return {
        "deployment": deployment,
        "drop_date": drop_date,
        "run_id": run_id,
        "files": [
            {
                "name": name,
                **{
                    field_name: cast("Mapping[str, Any]", result.metadata)[field_name]
                    for field_name in MANIFEST_FILE_FIELDS
                },
            }
            for name, result in delivered.items()
        ],
    }


def _write_manifest(
    key: AssetKey, manifest: Mapping[str, Any], destination: UPath
) -> MaterializeResult:
    data = json.dumps(manifest, indent=2).encode()
    destination.write_bytes(data)
    sha256 = hashlib.sha256(data).hexdigest()
    return MaterializeResult(
        asset_key=key,
        data_version=DataVersion(sha256),
        metadata={
            "file_count": len(manifest["files"]),
            "path": MetadataValue.path(str(destination)),
            "sha256": sha256,
        },
    )


def _export(
    key: AssetKey,
    write: Callable[[pl.LazyFrame, UPath], tuple[str, int, int]],
    frame: pl.LazyFrame,
    destination: UPath,
    metadata: Mapping[str, Any],
) -> MaterializeResult:
    sha256, size, row_count = write(frame, destination)
    get_dagster_logger().info(
        "Wrote %s rows (%d bytes) to %s", row_count, size, destination
    )
    return MaterializeResult(
        asset_key=key,
        data_version=DataVersion(sha256),
        metadata={
            **metadata,
            "row_count": row_count,
            "path": MetadataValue.path(str(destination)),
            "size_bytes": size,
            "sha256": sha256,
        },
    )
