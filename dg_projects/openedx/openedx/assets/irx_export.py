"""The nightly IRx drop, cut from the irx__ models instead of queried live.

legacy_openedx queries each deployment's edxapp MySQL every night and uploads
six CSVs for MIT Institutional Research, plus a mongodump of the forum database.
This writes the same six files, with the same headers and value formatting, and
the forum's contents collection in the same BSON format, from tables the
warehouse already maintains. The column-level contract is in
src/ol_dbt/models/external/IRX_SIMEON_MAPPING.md.
"""

import hashlib
import io
import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import bson
import polars as pl
from bson import ObjectId
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
# Which fields the retired cs_comments_service stored on each kind of post, read
# off the last mongodump legacy_openedx shipped.
FORUM_SHARED_FIELDS = (
    "course_id",
    "author_username",
    "body",
    "group_id",
    "visible",
    "anonymous",
    "anonymous_to_peers",
    "created_at",
    "updated_at",
)
FORUM_THREAD_FIELDS = (
    "title",
    "thread_type",
    "context",
    "commentable_id",
    "closed",
    "pinned",
    "comment_count",
    "last_activity_at",
)
FORUM_COMMENT_FIELDS = ("endorsed", "depth", "child_count")


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
    """Hash and count bytes and rows on their way to the object store."""

    def __init__(self, sink: Any, line_terminator: bytes):
        self._sink = sink
        self._line_terminator = line_terminator
        self.digest = hashlib.sha256()
        self.size = 0
        self.lines = 0

    def writable(self) -> bool:
        return True

    def write(self, data: Any) -> int:
        self.digest.update(data)
        self.size += len(data)
        self.lines += data.count(self._line_terminator)
        return self._sink.write(data)


def write_legacy_csv(frame: pl.LazyFrame, destination: UPath) -> tuple[str, int, int]:
    """Stream a frame to the drop as CSV; return its sha256, size, and row count.

    Streamed, never collected: studentmodule_query.csv runs to 59 GB for mitx,
    and legacy_openedx needed a 32Gi memory limit for loading it whole. Row
    count is counted off the bytes as they're written (minus the header line)
    rather than from a second full scan of the frame.
    """
    line_terminator = "\r\n"
    with destination.open("wb") as sink:
        writer = _DigestingWriter(sink, line_terminator.encode())
        frame.sink_csv(writer, line_terminator=line_terminator)
    return writer.digest.hexdigest(), writer.size, writer.lines - 1


def mint_objectid(content_type: str, content_id: int) -> ObjectId:
    """Stand in for the ObjectId of a post created after the Mongo cutover.

    An ObjectId opens with its creation time in seconds, and forum ObjectIds
    date from 2012 on, so a zero timestamp cannot collide with a real one. The
    byte after it keeps threads and comments apart, because their ids overlap.
    """
    kind = 0 if content_type == "CommentThread" else 1
    return ObjectId(f"{0:08x}{kind:02x}{content_id:014x}")


def _forum_objectid(
    mongoid: str | None, content_type: str, content_id: int | None
) -> ObjectId | None:
    if content_id is None:
        return None
    return ObjectId(mongoid) if mongoid else mint_objectid(content_type, content_id)


def forum_document(row: Mapping[str, Any]) -> dict[str, Any]:
    """Rebuild one post as the Mongo document legacy's mongodump carried.

    Simeon's forum loader reads comment_thread_id and parent_id as the
    ObjectId of the post they point at, and joins them to _id. Emitting the
    MySQL foreign keys instead would make those joins match nothing, and its
    forum_posts query drops the rows rather than failing.

    User ids are strings, as Mongo stored them, and a field with no value is
    left out rather than written as null.
    """
    content_type = row["_type"]
    document_id = _forum_objectid(row["mongoid"], content_type, row["id"])
    up, down = row["votes_up"] or [], row["votes_down"] or []
    document: dict[str, Any] = {
        "_id": document_id,
        "_type": content_type,
        "author_id": str(row["author_id"]),
        "votes": {
            "up": up,
            "down": down,
            "up_count": len(up),
            "down_count": len(down),
            "count": len(up) + len(down),
            "point": len(up) - len(down),
        },
        "abuse_flaggers": row["abuse_flaggers"] or [],
        "historical_abuse_flaggers": row["historical_abuse_flaggers"] or [],
        # Only ever empty in the last dump, and forum-v2 has no column for it.
        "at_position_list": [],
    }
    fields: tuple[str, ...]
    if content_type == "CommentThread":
        fields = FORUM_THREAD_FIELDS
    else:
        fields = FORUM_COMMENT_FIELDS
        parent_id = _forum_objectid(row["parent_mongoid"], "Comment", row["parent_id"])
        document["comment_thread_id"] = _forum_objectid(
            row["comment_thread_mongoid"], "CommentThread", row["comment_thread_id"]
        )
        document["parent_id"] = parent_id
        # Open edX nests comments one level under a response, so a comment's
        # only ancestor below the thread is its parent. Mongo held the whole
        # chain, which differs only for 187 mitx posts from 2012.
        document["parent_ids"] = [parent_id] if parent_id else []
        document["sk"] = f"{parent_id}-{document_id}" if parent_id else str(document_id)
        if endorsement := json.loads(row["endorsement"] or "{}"):
            document["endorsement"] = {
                "user_id": endorsement["user_id"],
                "time": datetime.fromisoformat(endorsement["time"]),
            }
    document.update((name, row[name]) for name in (*FORUM_SHARED_FIELDS, *fields))
    return {name: value for name, value in document.items() if value is not None}


def write_forum_bson(frame: pl.LazyFrame, destination: UPath) -> tuple[str, int, int]:
    """Write posts as a mongodump collection file; return sha256, size, count.

    A mongodump .bson file is the documents' BSON encodings back to back.
    Sorted so an unchanged forum writes an unchanged file.
    """
    digest, size, count = hashlib.sha256(), 0, 0
    with destination.open("wb") as sink:
        for batch in frame.sort("_type", "id").collect_batches():
            for row in batch.iter_rows(named=True):
                data = bson.encode(forum_document(row))
                digest.update(data)
                size += len(data)
                count += 1
                sink.write(data)
    return digest.hexdigest(), size, count


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
                "forum/contents.bson: every forum post, as the Mongo contents "
                "collection legacy dumped."
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
    )
    def irx_export(context: AssetExecutionContext) -> Iterator[MaterializeResult]:
        root = UPath(IRX_EXPORT_ROOTS.get(DAGSTER_ENV, IRX_EXPORT_SANDBOX_ROOT))
        drop_date = context.partition_time_window.start.strftime("%Y%m%d")
        drop = root / deployment / drop_date
        # A re-run rewrites the files in place. Take the old manifest down first,
        # so a re-run that fails partway leaves no manifest vouching for a mix of
        # old and new files.
        (drop / MANIFEST_NAME).unlink(missing_ok=True)
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
        delivered["forum/contents.bson"] = _export(
            forum_key,
            write_forum_bson,
            frame,
            drop / "forum" / "contents.bson",
            metadata,
        )
        yield delivered["forum/contents.bson"]

        manifest = build_manifest(deployment, drop_date, context.run.run_id, delivered)
        yield _write_manifest(manifest_key, manifest, drop / MANIFEST_NAME)

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
                    field_name: (result.metadata or {})[field_name]
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
