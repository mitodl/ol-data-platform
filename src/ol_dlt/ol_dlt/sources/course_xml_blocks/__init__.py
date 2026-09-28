"""Course XML block ingestion via dlt.

The course archive assets parse every block out of a course's XML export and
land one JSON Lines file per course version in the production landing zone:

    edxorg (dg_projects/edxorg/.../openedx_course_archives.py)
        edxorg-raw-data/edxorg/processed_data/course_xml_blocks/
            {prod,edge}/<course>/<version hash>.json
    openedx (dg_projects/openedx/.../openedx.py), one prefix per deployment
        {mitx,mitxonline,xpro}/openedx/processed_data/course_xml_blocks/
            <deployment>/<course>/<version hash>.json

Nothing loaded those files into the warehouse, so the two raw tables below did
not exist and their staging models could not build. This source appends every
file's rows, stamped with the file they came from, and staging keeps each
block's newest copy by ``_file_modified_at``.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.course_xml_blocks
"""

import io
import json
import logging
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import dlt
import pyarrow as pa
import s3fs
from dlt.sources.filesystem import FileItemDict

from ol_dlt import config
from ol_dlt.file_metadata import FILE_METADATA_COLUMNS, add_file_metadata
from ol_dlt.sources.edxorg_s3 import edxorg_files

logger = logging.getLogger(__name__)

# Always the production landing zone, whatever the profile: it is where the
# files are read from, not where rows are written to.
LANDING_BUCKET = "s3://ol-data-lake-landing-zone-production/"

# The largest file is 33 MB, so the budget is what sizes a load. dlt's Iceberg
# writer holds a whole load in memory (see ol_dlt.sources.edxorg_s3), measured
# at about 0.9 GB + 1.6x the batch on edxorg TSVs, so 512 MiB should peak near
# 1.7 GB, well inside the data_loading run pod's 8Gi default limit. That ratio
# was not measured on this JSON. The ~63 GB backlog is ~126 batches, inside one
# run's 200-batch cap.
BUDGET_BYTES = 512 * 1024**2

BATCH_ROWS = 10_000

# Every field the archive assets write, in their order. All text: the scalar
# metadata fields are strings or null in every file sampled, and staging casts
# what it needs. xml_attributes is an object of arbitrary XML attribute names,
# kept as a JSON string because dlt would otherwise flatten each attribute
# name it meets into its own column.
FIELDS = (
    "course_id",
    "source_system",
    "block_id",
    "block_type",
    "block_display_name",
    "xml_attributes",
    "xml_path",
    "raw_xml",
    "retrieved_at",
    "edx_video_id",
    "duration",
    "max_attempts",
    "weight",
    "markdown",
)
SCHEMA = pa.schema([pa.field(name, pa.string()) for name in FIELDS])


@dataclass(frozen=True)
class XmlBlocksTable:
    """One raw table and the landing-zone globs that feed it.

    :param raw_table: Raw warehouse table name.
    :param pipeline_prefix: Destination bucket prefix, the table's deployment.
    :param file_globs: Globs relative to ``LANDING_BUCKET``.
    """

    raw_table: str
    pipeline_prefix: str
    file_globs: tuple[str, ...]


EDXORG = XmlBlocksTable(
    raw_table="raw__edxorg__s3__course_xml_blocks",
    pipeline_prefix="edxorg",
    file_globs=("edxorg-raw-data/edxorg/processed_data/course_xml_blocks/**/*.json",),
)
OPENEDX = XmlBlocksTable(
    raw_table="raw__openedx__s3__course_xml_blocks",
    pipeline_prefix="openedx",
    file_globs=tuple(
        f"{deployment}/openedx/processed_data/course_xml_blocks/**/*.json"
        for deployment in ("mitx", "mitxonline", "xpro")
    ),
)
TABLES = {table.raw_table: table for table in (EDXORG, OPENEDX)}


def _text(value: Any) -> str | None:  # noqa: ANN401
    """Return ``value`` as text, JSON-encoding anything that is not a string."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def _row(line: str) -> dict[str, str | None]:
    """Parse one JSON line into a row, failing on a line missing any field."""
    record = json.loads(line)
    return {name: _text(record[name]) for name in FIELDS}


@dlt.transformer(standalone=True)
def read_xml_blocks(
    items: Iterable[FileItemDict], batch_rows: int = BATCH_ROWS
) -> Iterator[pa.Table]:
    """Stream each JSON Lines file as Arrow tables stamped with its provenance."""
    for item in items:
        rows: list[dict[str, str | None]] = []
        with item.open() as raw, io.TextIOWrapper(raw, encoding="utf-8") as text:
            for line in text:
                if not line.strip():
                    continue
                rows.append(_row(line))
                if len(rows) == batch_rows:
                    yield _stamp(rows, item)
                    rows = []
        if rows:
            yield _stamp(rows, item)


def _stamp(rows: list[dict[str, str | None]], item: FileItemDict) -> pa.Table:
    return add_file_metadata(
        pa.Table.from_pylist(rows, schema=SCHEMA),
        source_file=item["file_url"],
        modified_at=item.get("modification_date"),
    )


@dlt.source(name="course_xml_blocks")
def course_xml_blocks_source(
    raw_table: str,
    bucket_url: str = LANDING_BUCKET,
    table_format: config.TableFormat | None = None,
    budget_bytes: int = BUDGET_BYTES,
) -> Generator[Any]:
    """Load one course XML block table from the landing zone.

    One table per source so each gets its own pipeline, cursor and Dagster asset.
    The caller re-runs the source until a batch reads nothing, as for edxorg_s3.

    Args:
        raw_table: A key of ``TABLES``.
        bucket_url: Bucket the globs are relative to.
        table_format: ``native`` or ``iceberg``; defaults to the active profile's.
        budget_bytes: How much source JSON one load may cover.
    """
    table = TABLES[raw_table]
    # No explicit credentials, so s3fs refreshes IRSA credentials itself (see
    # ol_dlt.sources.edxorg_s3 for why dlt's own credentials expire mid-run).
    files = edxorg_files(
        bucket_url=bucket_url,
        file_globs=table.file_globs,
        credentials=s3fs.S3FileSystem(),
        budget_bytes=budget_bytes,
    )
    yield (
        (files | read_xml_blocks())
        .with_name(table.raw_table)
        .apply_hints(
            table_name=table.raw_table,
            # Append for the reason edxorg_s3 appends: each course version is a
            # whole new file of mostly unchanged blocks, and an Iceberg merge of
            # that size does not finish. deduplicate_raw_table orders by
            # _file_modified_at (the inventory's raw_metadata_column), so a
            # block removed from a course keeps its last copy in staging.
            write_disposition="append",
            table_format=table_format or config.active_table_format(),
            columns={**config.DLT_LOAD_ID_COLUMN, **FILE_METADATA_COLUMNS},
        )
    )


def course_xml_blocks_pipeline_for(raw_table: str) -> dlt.Pipeline:
    """Return the pipeline for one table.

    The pipeline name keys the modification_date cursor, so it must stay
    stable: renaming it re-reads the landing zone and, under append, inserts
    every row again.
    """
    table = TABLES[raw_table]
    return config.pipeline_for(
        table.pipeline_prefix,
        pipeline_name=f"course_xml_blocks__{table.pipeline_prefix}",
    )
