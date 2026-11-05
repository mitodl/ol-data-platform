"""The nightly IRx drop, cut from the irx__ models instead of queried live.

legacy_openedx queries each deployment's edxapp MySQL every night and uploads
six CSVs for MIT Institutional Research, plus a mongodump of the forum database.
This writes the same six CSVs, with the same headers and value formatting, from
tables the warehouse already maintains. The forum is delivered as a flat
Parquet export of the irx__ model instead of a reconstructed Mongo dump: Mongo
has not backed the forum since the forum-v2 cutover, so there is no dump shape
left to match, and IRx adapts their tooling to the columns we actually have.
Course tarballs are not re-copied into each night's courses/ the way legacy did
it: each exported version is copied once to courses/{deployment}/ at the top of
the bucket, which the 90-day drop expiry does not cover. The manifest names the
copy of each listed run's newest export, and the runs left out because they
have no export or their copy failed.
The column-level contract is in src/ol_dbt/models/external/IRX_SIMEON_MAPPING.md.
"""

import hashlib
import io
import itertools
import json
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import partial
from typing import Any, cast

import polars as pl
import pyarrow.parquet as pq
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
from ol_orchestrate.lib.glue_helper import load_dbt_model_table
from ol_orchestrate.lib.utils import s3_uploads_bucket
from pyiceberg.io.pyarrow import schema_to_pyarrow
from pyiceberg.table import Table
from upath import UPath

from openedx.assets.openedx import course_xml

IRX_EXPORT_GROUP = "irx_export"
IRX_EXPORT_CODE_VERSION = "irx_export_v4"
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

# Outside {deployment}/, so the bucket's 90-day expiry of nightly drops leaves
# the copies alone. ol-infrastructure scopes that rule to the deployment prefixes.
COURSE_EXPORTS_PREFIX = "courses"
COURSE_XML_SUFFIX = ".xml.tar.gz"
# botocore's default connection pool size, which s3fs does not raise.
COPY_WORKERS = 10

LEGACY_DATETIME = "%Y-%m-%d %H:%M:%S"

MANIFEST_NAME = "_MANIFEST.json"
MANIFEST_FILE_FIELDS = ("row_count", "size_bytes", "sha256")

FORUM_CONTENTS_MODEL = "forum_contents"

# Rows decoded per pull from a data file. studentmodule's state column averages
# 2 KB a row, so a batch is about 35 MB of text.
READ_BATCH_ROWS = 16_384

# Reads of one irx table before a data file that keeps disappearing fails the
# run. A mitx drop takes about 40 minutes, most of it studentmodule, and on
# 2026-10-12 dbt rebuilt that table three times in about 70.
MAX_TABLE_READS = 3


class SourceTableReplacedError(Exception):
    """A data file of the snapshot being read no longer exists.

    The irx models are dbt tables, so a build writes a new table and drops the
    old one with its data files, whatever snapshot a reader has pinned.
    """


@dataclass(frozen=True)
class IrxExportFile:
    """One legacy CSV and the irx__ model it is cut from."""

    name: str
    model: str
    columns: tuple[str, ...]
    renames: Mapping[str, str] = field(default_factory=dict)
    required: tuple[str, ...] = ()

    @property
    def source_columns(self) -> tuple[str, ...]:
        """The model columns the CSV is cut from, under their model names."""
        model_names = {new: old for old, new in self.renames.items()}
        return tuple(model_names.get(column, column) for column in self.columns)


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


def legacy_batch(
    batch: pl.DataFrame, export: IrxExportFile, course_ids: Sequence[str]
) -> pl.DataFrame:
    """Cut one batch of an irx model down to its legacy CSV's rows and columns."""
    batch = (
        batch.rename(dict(export.renames))
        .filter(pl.col("course_id").is_in(course_ids))
        .drop_nulls(list(export.required))
    )
    return batch.select(legacy_csv_columns(batch.schema, export.columns))


def legacy_batches(
    batches: Iterable[pl.DataFrame], export: IrxExportFile, course_ids: Sequence[str]
) -> Iterator[pl.DataFrame]:
    """Cut each batch of an irx model down to its legacy CSV's rows and columns."""
    return (legacy_batch(batch, export, course_ids) for batch in batches)


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


def write_legacy_csv(
    batches: Iterable[pl.DataFrame], destination: UPath
) -> tuple[str, int, int]:
    """Write batches to the drop as CSV; return its sha256, size, and row count.

    One batch at a time, each written before the next is pulled:
    studentmodule_query.csv runs to 59 GB for mitx. The header comes from the
    first batch, so callers pass at least one, empty if need be. Row count is
    counted off the bytes as they're written (minus the header line) rather
    than from a second pass over the data.
    """
    with destination.open("wb") as sink:
        writer = _DigestingWriter(sink)
        for index, batch in enumerate(batches):
            batch.write_csv(writer, include_header=index == 0, line_terminator="\r\n")
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


def write_parquet(
    batches: Iterable[pl.DataFrame], destination: UPath
) -> tuple[str, int, int]:
    """Write batches to the drop as Parquet; return its sha256, size, and row count.

    One row group per batch, each written before the next is pulled, same as
    write_legacy_csv. The schema comes from the first batch.
    """
    batches = iter(batches)
    first = next(batches)
    row_count = 0
    with destination.open("wb") as sink:
        hashing = _HashingWriter(sink)
        with pq.ParquetWriter(
            hashing, first.to_arrow().schema, compression="zstd"
        ) as writer:
            for batch in itertools.chain([first], batches):
                if batch.height:
                    writer.write_table(batch.to_arrow())
                    row_count += batch.height
    return hashing.digest.hexdigest(), hashing.size, row_count


def read_data_files(
    table: Table, snapshot_id: int, columns: Sequence[str] = ("*",)
) -> Iterator[pl.DataFrame]:
    """Read an Iceberg snapshot READ_BATCH_ROWS rows at a time, after an empty batch.

    Not pl.scan_iceberg: with the pyiceberg reader glue_helper forces, it reads
    through pyiceberg's to_arrow_batch_reader, which hands every data file to a
    thread pool at once and keeps each decoded file until the consumer reaches
    it. An S3 upload drains far slower than the pool reads, so the run ends up
    holding most of the table (mitxonline's studentmodule peaked at 29 GB, and
    mitx's is twice the size). Polars' streaming sinks also read ahead of a slow
    Python sink, so the writers take plain batches instead of a LazyFrame.

    Not pyiceberg's ArrowScan either, even one data file at a time: its Arrow
    dataset scanner decodes several row groups ahead of the consumer. That is
    harmless on the 30 MB files a dbt build writes, but the nightly OPTIMIZE
    rewrites studentmodule into 580 MB files of 1.9 GB row groups, and one of
    those passes the worker's 8Gi limit within 30 seconds. ParquetFile decodes
    only the batch being pulled and holds one compressed column chunk beside
    it, so memory follows a row group's size on disk, not its decoded size.

    The leading empty batch carries the schema, so a table with no data files
    still gets a CSV header or a Parquet schema.
    """
    scan = table.scan(snapshot_id=snapshot_id, selected_fields=tuple(columns))
    schema = schema_to_pyarrow(scan.projection())
    yield cast("pl.DataFrame", pl.from_arrow(schema.empty_table()))
    try:
        # Planning reads the snapshot's manifest list and manifests, which a
        # dbt rebuild drops along with the data files.
        tasks = list(scan.plan_files())
    except FileNotFoundError as error:
        raise SourceTableReplacedError(str(error)) from error
    for task in tasks:
        # Reading the Parquet file directly skips what ArrowScan would apply.
        # The irx tables are rebuilt whole by dbt, so they never carry deletes.
        if task.delete_files:
            raise Failure(
                description=(
                    f"{task.file.file_path} has delete files, which this reader "
                    "does not apply; the export would deliver deleted rows."
                )
            )
        data_file = table.io.new_input(task.file.file_path)
        try:
            with data_file.open() as source:
                for batch in pq.ParquetFile(source).iter_batches(
                    batch_size=READ_BATCH_ROWS, columns=schema.names
                ):
                    yield cast(
                        "pl.DataFrame",
                        pl.from_arrow(batch.select(schema.names).cast(schema)),
                    )
        except OSError as error:
            # A file dropped before it is opened raises FileNotFoundError; one
            # dropped between two ranged reads raises a bare OSError naming the
            # object store's own error, so the file is looked for again.
            if isinstance(error, FileNotFoundError) or not data_file.exists():
                raise SourceTableReplacedError(task.file.file_path) from error
            raise


def course_xml_root(deployment: str) -> UPath:
    """Where course_xml writes a deployment's {course_id}/{sha256}.xml.tar.gz."""
    landing = s3_uploads_bucket(DAGSTER_ENV)
    return UPath(f"s3://{landing['bucket']}").joinpath(
        *filter(None, [landing["prefix"]]), deployment, *course_xml.key.path
    )


@dataclass(frozen=True)
class CourseExport:
    """The newest course_xml export of one course run."""

    course_id: str
    source: str
    size_bytes: int
    sha256: str
    exported_at: datetime

    @property
    def file_name(self) -> str:
        return f"{self.sha256}{COURSE_XML_SUFFIX}"


def _last_modified(info: Mapping[str, Any]) -> datetime:
    # s3fs reports LastModified; the local filesystem, in tests, reports mtime.
    if "LastModified" in info:
        return info["LastModified"]
    return datetime.fromtimestamp(info["mtime"], tz=UTC)


def latest_course_exports(
    root: UPath, course_ids: Iterable[str]
) -> tuple[list[CourseExport], list[str]]:
    """Find the newest course_xml export of each listed run, and the runs with none.

    course_xml writes each version of a course to {course_id}/{sha256}.xml.tar.gz
    and never deletes the older ones, so the newest object is the version last
    exported. That is what legacy delivered too, since it exported every course
    fresh each night. One recursive listing rather than one per course: mitxonline
    lists close to 4,900 runs.
    """
    fs = root.fs
    prefix = f"{root.path.rstrip('/')}/"
    newest: dict[str, CourseExport] = {}
    for path, info in fs.find(root.path, detail=True).items():
        course_id, _, name = path.removeprefix(prefix).rpartition("/")
        if not course_id or not name.endswith(COURSE_XML_SUFFIX):
            continue
        export = CourseExport(
            course_id=course_id,
            source=path,
            size_bytes=info["size"],
            sha256=name.removesuffix(COURSE_XML_SUFFIX),
            exported_at=_last_modified(info),
        )
        current = newest.get(course_id)
        # The path breaks a tie in LastModified, so a rerun picks the same one.
        if current is None or (export.exported_at, export.source) > (
            current.exported_at,
            current.source,
        ):
            newest[course_id] = export
    listed = list(dict.fromkeys(course_ids))
    return (
        [newest[course_id] for course_id in listed if course_id in newest],
        [course_id for course_id in listed if course_id not in newest],
    )


@dataclass
class CourseExportDelivery:
    """What one night's copy of course exports delivered, and what it did not."""

    delivered: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, str]] = field(default_factory=list)
    copied: int = 0


def _copy_error(fs: Any, source: str, target: str) -> str | None:
    # s3fs raises an S3 error response as an OSError subclass, chained to the
    # botocore error that carries S3's code.
    try:
        fs.cp_file(source, target)
    except OSError as error:
        get_dagster_logger().warning(
            "Could not copy %s to %s: %s", source, target, error
        )
        # The class and S3's code only: S3's message can name the role that was
        # denied, and the manifest is read outside MIT OL.
        response = getattr(error.__cause__, "response", None)
        if response:
            return f"{type(error).__name__} ({response['Error']['Code']})"
        return type(error).__name__
    return None


def copy_course_exports(
    exports: Sequence[CourseExport], destination: UPath
) -> CourseExportDelivery:
    """Copy each export not already under destination.

    A copy is named by the tarball's sha256, so one already present is that
    same version and is left alone: a night copies only what changed since the
    last one. S3 copies object to object without the bytes passing through
    this process, and an object only appears once its copy completes. Both
    roots are on one filesystem (S3, or local disk in tests).

    A copy S3 refuses leaves that run out of the delivered list and names it
    with its error, so one bad object doesn't cost IRx the night's drop. Legacy
    did the same with a course whose export failed. The next night retries it.
    A transport error that outlasts s3fs's retries is not an S3 refusal; it
    fails the run, as an outage should.
    """
    fs = destination.fs
    base = destination.path.rstrip("/")
    present = set(fs.find(base))
    targets = {
        export.course_id: f"{base}/{export.course_id}/{export.file_name}"
        for export in exports
    }
    pending = [export for export in exports if targets[export.course_id] not in present]
    with ThreadPoolExecutor(max_workers=COPY_WORKERS) as pool:
        errors = dict(
            zip(
                (export.course_id for export in pending),
                pool.map(
                    lambda export: _copy_error(
                        fs, export.source, targets[export.course_id]
                    ),
                    pending,
                ),
                strict=True,
            )
        )
    delivery = CourseExportDelivery(
        copied=sum(error is None for error in errors.values())
    )
    for export in exports:
        if error := errors.get(export.course_id):
            delivery.failed.append({"course_id": export.course_id, "error": error})
            continue
        delivery.delivered.append(
            {
                "course_id": export.course_id,
                "path": fs.unstrip_protocol(targets[export.course_id]),
                "size_bytes": export.size_bytes,
                "sha256": export.sha256,
                "exported_at": export.exported_at.isoformat(),
            }
        )
    return delivery


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
            code_version=IRX_EXPORT_CODE_VERSION,
        ),
        *(
            AssetSpec(
                key=AssetKey([deployment, IRX_EXPORT_GROUP, export.name]),
                deps=[AssetKey(["external", irx_model_name(deployment, export.model)])],
                description=f"{export.name}.csv in the nightly IRx drop.",
                code_version=IRX_EXPORT_CODE_VERSION,
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
            code_version=IRX_EXPORT_CODE_VERSION,
        ),
    ]
    specs = [
        *file_specs,
        AssetSpec(
            key=manifest_key,
            deps=[spec.key for spec in file_specs],
            description=(
                f"{MANIFEST_NAME}: every file in the drop with its row count, size "
                "and sha256; the copy of each listed course run's newest course "
                "export under courses/ (course_exports); and the listed runs "
                "with no export (course_exports_missing) or whose copy failed "
                "(course_exports_failed). Written last, so its presence means "
                "the drop is complete."
            ),
            code_version=IRX_EXPORT_CODE_VERSION,
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
            [pl.DataFrame({"course_id": course_ids}, schema={"course_id": pl.String})],
            drop / "course_ids.csv",
            {},
        )
        yield delivered["course_ids.csv"]

        for export in IRX_EXPORT_FILES:
            file_name = f"{export.name}.csv"
            delivered[file_name] = _export_table(
                AssetKey([deployment, IRX_EXPORT_GROUP, export.name]),
                write_legacy_csv,
                irx_model_name(deployment, export.model),
                drop / file_name,
                export.source_columns,
                partial(legacy_batches, export=export, course_ids=course_ids),
            )
            yield delivered[file_name]

        # Not cut to the course list: legacy dumped the whole forum database,
        # posts in runs the LMS no longer lists included.
        delivered["forum_contents.parquet"] = _export_table(
            forum_key,
            write_parquet,
            irx_model_name(deployment, FORUM_CONTENTS_MODEL),
            drop / "forum_contents.parquet",
        )
        yield delivered["forum_contents.parquet"]

        # A run with no export yet is left out and named, the way legacy left
        # out a course whose export failed that night.
        exports, missing = latest_course_exports(
            course_xml_root(deployment), course_ids
        )
        if missing:
            context.log.warning(
                "%d listed course runs have no course export: %s",
                len(missing),
                missing,
            )
        courses = copy_course_exports(
            exports, root / COURSE_EXPORTS_PREFIX / deployment
        )
        context.log.info(
            "Copied %d of %d course exports; the rest were already delivered",
            courses.copied,
            len(exports),
        )
        if courses.failed:
            context.log.error(
                "%d course exports could not be copied: %s",
                len(courses.failed),
                courses.failed,
            )
            # One bad object is left out; every copy failing is a broken grant
            # or bucket, and a manifest with no courses would hide it.
            if not courses.copied:
                raise Failure(
                    description=(
                        f"All {len(courses.failed)} course export copies to "
                        f"{root / COURSE_EXPORTS_PREFIX / deployment} failed."
                    )
                )

        manifest = build_manifest(
            deployment,
            drop_date,
            context.run.run_id,
            delivered,
            {
                "course_exports": courses.delivered,
                "course_exports_missing": missing,
                "course_exports_failed": courses.failed,
            },
        )
        yield _write_manifest(manifest_key, manifest, manifest_path, courses.copied)

    return irx_export


def _read_pinned(
    table_name: str, columns: Sequence[str] = ("*",)
) -> tuple[Iterator[pl.DataFrame], dict[str, str]]:
    """Read an irx table at its current snapshot.

    Pinned so the file's metadata names the snapshot its rows came from. The
    pin does not keep the snapshot readable: a dbt rebuild replaces the table
    and deletes the old one's data files (SourceTableReplacedError).
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
    return read_data_files(table, snapshot.snapshot_id, columns), {
        "source_table": f"{IRX_GLUE_DATABASE}.{table_name}",
        "source_snapshot_id": str(snapshot.snapshot_id),
    }


def build_manifest(
    deployment: str,
    drop_date: str,
    run_id: str,
    delivered: Mapping[str, MaterializeResult],
    courses: Mapping[str, Sequence[Any]],
) -> dict[str, Any]:
    """Describe a finished drop from what its files' materializations recorded.

    Built from the metadata each file's write already computed, not by reading
    the files back. Course exports are listed where their copies live, outside
    the drop's own prefix.
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
        **{section: list(entries) for section, entries in courses.items()},
    }


def _write_manifest(
    key: AssetKey,
    manifest: Mapping[str, Any],
    destination: UPath,
    course_exports_copied: int,
) -> MaterializeResult:
    data = json.dumps(manifest, indent=2).encode()
    destination.write_bytes(data)
    sha256 = hashlib.sha256(data).hexdigest()
    return MaterializeResult(
        asset_key=key,
        data_version=DataVersion(sha256),
        metadata={
            "file_count": len(manifest["files"]),
            "course_export_count": len(manifest["course_exports"]),
            "course_exports_copied": course_exports_copied,
            "course_exports_missing": len(manifest["course_exports_missing"]),
            "course_exports_failed": len(manifest["course_exports_failed"]),
            "path": MetadataValue.path(str(destination)),
            "sha256": sha256,
        },
    )


def _export_table(  # noqa: PLR0913
    key: AssetKey,
    write: Callable[[Iterable[pl.DataFrame], UPath], tuple[str, int, int]],
    table_name: str,
    destination: UPath,
    columns: Sequence[str] = ("*",),
    cut: Callable[[Iterator[pl.DataFrame]], Iterable[pl.DataFrame]] = iter,
) -> MaterializeResult:
    """Export one irx table, starting over if dbt replaces it under the read.

    The file is rewritten whole from the new table's snapshot: the destination
    is overwritten, and the digest, row count and snapshot id recorded are the
    last read's.
    """

    def export_once() -> MaterializeResult:
        batches, metadata = _read_pinned(table_name, columns)
        return _export(key, write, cut(batches), destination, metadata)

    for _ in range(MAX_TABLE_READS - 1):
        try:
            return export_once()
        except SourceTableReplacedError as error:
            get_dagster_logger().warning(
                "%s.%s was replaced while it was being read (%s is gone); "
                "reading it again from its current snapshot",
                IRX_GLUE_DATABASE,
                table_name,
                error,
            )
    return export_once()


def _export(
    key: AssetKey,
    write: Callable[[Iterable[pl.DataFrame], UPath], tuple[str, int, int]],
    batches: Iterable[pl.DataFrame],
    destination: UPath,
    metadata: Mapping[str, Any],
) -> MaterializeResult:
    sha256, size, row_count = write(batches, destination)
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
