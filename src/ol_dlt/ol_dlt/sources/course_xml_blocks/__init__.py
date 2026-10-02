"""Course XML block ingestion via dlt.

The course archive assets parse every block out of a course's XML export and
land one JSON Lines file per course version in the production landing zone:

    edxorg (dg_projects/edxorg/.../openedx_course_archives.py)
        edxorg-raw-data/edxorg/processed_data/course_xml_blocks/
            {prod,edge}/<course>/<version hash>.json
    openedx (dg_projects/openedx/.../openedx.py), one prefix per deployment
        {mitx,mitxonline,xpro}/openedx/processed_data/course_xml_blocks/
            <deployment>/<course>/<version hash>.json

The openedx code location also lands the text it extracts from each course's
static files, for MIT Learn's ContentFiles (Cohort 4), in the same layout:

        {mitx,mitxonline,xpro}/openedx/processed_data/course_document_text/
        {mitx,mitxonline,xpro}/openedx/processed_data/course_transcript_text/
            <deployment>/<course>/<version hash>.jsonl

and, for the same ContentFiles, every file in each course export flagged with
whether MIT Learn excludes it (staff-only, manifests, unreferenced static files):

        {mitx,mitxonline,xpro}/openedx/processed_data/course_file_exclusions/
            <deployment>/<course>/<version hash>.jsonl

Nothing loaded those files into the warehouse, so the raw tables below did not
exist and their staging models could not build. This source appends every
file's rows, stamped with the file they came from, and staging keeps the rows
of each course's newest file.

The edxorg code location also un-nests each course's structure document into
one JSON Lines file of blocks per structure version
(dg_projects/edxorg/.../edxorg_archive.py):

        edxorg-raw-data/edxorg/processed_data/course_blocks/
            <course>|{prod,edge}/<structure hash>.json

An Airbyte source-s3 connection loads those into raw__edxorg__s3__course_blocks,
and on 2026-10-05 that table held 6,466 of the 9,860 landed files. This source
loads them into raw__edxorg__s3__course_structure_blocks, which is to replace
it. A re-materialized structure overwrites its file with a new retrieved_at, so
the landing zone holds only the latest copy of each: the Airbyte table has
earlier copies of 1,918 files that this source cannot read.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.course_xml_blocks
"""

import io
import json
import logging
from collections.abc import Generator, Iterable, Iterator
from dataclasses import dataclass
from typing import Any
from urllib.parse import unquote

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
XML_BLOCK_FIELDS = (
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
# The row ol_orchestrate.lib.openedx.un_nest_course_structure writes, in the
# order Airbyte landed the columns. block_details is the block's whole structure
# entry, kept as a JSON string for staging to extract from. course_id is a
# string in some files and a one-element list in others (138,244 of 428,478
# rows across 431 files sampled 2026-10-05), and the list becomes its JSON text,
# which is what Airbyte landed and what staging strips the brackets from.
STRUCTURE_BLOCK_FIELDS = (
    "block_id",
    "block_due",
    "course_id",
    "block_type",
    "block_index",
    "block_start",
    "block_title",
    "block_parent",
    "course_start",
    "course_title",
    "retrieved_at",
    "block_details",
    "block_content_hash",
    "course_content_hash",
)
# The row the document and transcript text assets write
# (dg_projects/openedx/openedx/assets/content_files.py and transcripts.py).
# size_bytes stays text like everything else here; staging casts it.
# extracted_at is absent from files written before the assets stamped it.
CONTENT_TEXT_FIELDS = (
    "course_id",
    "source_system",
    "file_path",
    "file_extension",
    "content_type",
    "size_bytes",
    "content",
    "extraction_status",
    "extracted_at",
)

# The row the file exclusions asset writes
# (dg_projects/openedx/openedx/assets/content_file_exclusions.py).
FILE_EXCLUSION_FIELDS = (
    "course_id",
    "source_system",
    "file_path",
    "excluded",
    "exclusion_reason",
)


@dataclass(frozen=True)
class XmlBlocksTable:
    """One raw table and the landing-zone globs that feed it.

    :param raw_table: Raw warehouse table name.
    :param pipeline_prefix: Destination bucket prefix, the table's deployment.
    :param file_globs: Globs relative to ``LANDING_BUCKET``.
    :param fields: Every field a line carries, in order.
    :param optional_fields: Fields a line may leave out. A failed extraction
        row has no file_extension.
    :param integer_fields: Fields loaded as bigint instead of text.
    :param pipeline_name: dlt pipeline name, which keys the cursor.
    :param marks_empty_files: Load a marker row for a file with no lines. The
        text assets write an empty file when a course has nothing left to
        extract, and without a row that file never becomes the course's newest
        in staging, so its last non-empty version would stay current forever.
    """

    raw_table: str
    pipeline_prefix: str
    file_globs: tuple[str, ...]
    fields: tuple[str, ...] = XML_BLOCK_FIELDS
    optional_fields: frozenset[str] = frozenset()
    integer_fields: frozenset[str] = frozenset()
    pipeline_name: str = ""
    marks_empty_files: bool = False

    def __post_init__(self) -> None:
        # The two block tables predate the other tables and keep their names:
        # renaming a pipeline re-reads the landing zone and, under append,
        # inserts every row again.
        if not self.pipeline_name:
            object.__setattr__(
                self, "pipeline_name", f"course_xml_blocks__{self.pipeline_prefix}"
            )

    @property
    def schema(self) -> pa.Schema:
        return pa.schema(
            [
                pa.field(
                    name, pa.int64() if name in self.integer_fields else pa.string()
                )
                for name in self.fields
            ]
        )


EDXORG = XmlBlocksTable(
    raw_table="raw__edxorg__s3__course_xml_blocks",
    pipeline_prefix="edxorg",
    file_globs=("edxorg-raw-data/edxorg/processed_data/course_xml_blocks/**/*.json",),
)
EDXORG_STRUCTURE_BLOCKS = XmlBlocksTable(
    raw_table="raw__edxorg__s3__course_structure_blocks",
    pipeline_prefix="edxorg",
    file_globs=("edxorg-raw-data/edxorg/processed_data/course_blocks/**/*.json",),
    fields=STRUCTURE_BLOCK_FIELDS,
    # bigint in the Airbyte table, and staging passes it through uncast.
    integer_fields=frozenset({"block_index"}),
    pipeline_name="course_structure_blocks__edxorg",
)
OPENEDX = XmlBlocksTable(
    raw_table="raw__openedx__s3__course_xml_blocks",
    pipeline_prefix="openedx",
    file_globs=tuple(
        f"{deployment}/openedx/processed_data/course_xml_blocks/**/*.json"
        for deployment in ("mitx", "mitxonline", "xpro")
    ),
)
_OPENEDX_DEPLOYMENTS = ("mitx", "mitxonline", "xpro")
OPENEDX_DOCUMENT_TEXT = XmlBlocksTable(
    raw_table="raw__openedx__s3__course_document_text",
    pipeline_prefix="openedx",
    file_globs=tuple(
        f"{deployment}/openedx/processed_data/course_document_text/**/*.jsonl"
        for deployment in _OPENEDX_DEPLOYMENTS
    ),
    fields=CONTENT_TEXT_FIELDS,
    optional_fields=frozenset({"file_extension", "extracted_at"}),
    pipeline_name="course_document_text__openedx",
    marks_empty_files=True,
)
OPENEDX_TRANSCRIPT_TEXT = XmlBlocksTable(
    raw_table="raw__openedx__s3__course_transcript_text",
    pipeline_prefix="openedx",
    file_globs=tuple(
        f"{deployment}/openedx/processed_data/course_transcript_text/**/*.jsonl"
        for deployment in _OPENEDX_DEPLOYMENTS
    ),
    fields=CONTENT_TEXT_FIELDS,
    optional_fields=frozenset({"file_extension", "extracted_at"}),
    pipeline_name="course_transcript_text__openedx",
    marks_empty_files=True,
)
OPENEDX_FILE_EXCLUSIONS = XmlBlocksTable(
    raw_table="raw__openedx__s3__course_file_exclusions",
    pipeline_prefix="openedx",
    file_globs=tuple(
        f"{deployment}/openedx/processed_data/course_file_exclusions/**/*.jsonl"
        for deployment in _OPENEDX_DEPLOYMENTS
    ),
    fields=FILE_EXCLUSION_FIELDS,
    pipeline_name="course_file_exclusions__openedx",
)
TABLES = {
    table.raw_table: table
    for table in (
        EDXORG,
        EDXORG_STRUCTURE_BLOCKS,
        OPENEDX,
        OPENEDX_DOCUMENT_TEXT,
        OPENEDX_TRANSCRIPT_TEXT,
        OPENEDX_FILE_EXCLUSIONS,
    )
}


def _text(value: Any) -> str | None:  # noqa: ANN401
    """Return ``value`` as text, JSON-encoding anything that is not a string."""
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value)


def _row(line: str, table: XmlBlocksTable = OPENEDX) -> dict[str, str | int | None]:
    """Parse one JSON line into a row, failing on a line missing a required field."""
    record = json.loads(line)
    row: dict[str, str | int | None] = {}
    for name in table.fields:
        value = record.get(name) if name in table.optional_fields else record[name]
        row[name] = value if name in table.integer_fields else _text(value)
    return row


@dlt.transformer(standalone=True)
def read_xml_blocks(
    items: Iterable[FileItemDict],
    table: XmlBlocksTable = OPENEDX,
    batch_rows: int = BATCH_ROWS,
) -> Iterator[pa.Table]:
    """Stream each JSON Lines file as Arrow tables stamped with its provenance."""
    for item in items:
        rows: list[dict[str, str | int | None]] = []
        empty = True
        with item.open() as raw, io.TextIOWrapper(raw, encoding="utf-8") as text:
            for line in text:
                if not line.strip():
                    continue
                empty = False
                rows.append(_row(line, table))
                if len(rows) == batch_rows:
                    yield _stamp(rows, item, table)
                    rows = []
        if empty and table.marks_empty_files:
            rows.append(_empty_file_marker(item["file_url"], table))
        if rows:
            yield _stamp(rows, item, table)


def _empty_file_marker(
    file_url: str, table: XmlBlocksTable
) -> dict[str, str | int | None]:
    """Build the row standing for an empty file: its course from the path, nothing else.

    The path is .../<deployment>/<course>/<version>.jsonl, percent-encoded as
    dlt gives it. Staging picks each course's newest file and then drops the
    marker, whose file_path is null.
    """
    *_, source_system, course_id, _version = file_url.split("/")
    return {
        **dict.fromkeys(table.fields),
        "course_id": unquote(course_id),
        "source_system": unquote(source_system),
    }


def _stamp(
    rows: list[dict[str, str | int | None]], item: FileItemDict, table: XmlBlocksTable
) -> pa.Table:
    return add_file_metadata(
        pa.Table.from_pylist(rows, schema=table.schema),
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
        (files | read_xml_blocks(table=table))
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
    return config.pipeline_for(table.pipeline_prefix, pipeline_name=table.pipeline_name)
