"""edX.org program learner report ingestion via dlt.

edX writes one learner report per MITx program every day into its
edx-program-reports bucket. The edxorg code location's
sync_edxorg_program_reports job (started by edxorg_program_reports_sensor)
copies them into the production landing zone:

    edxorg-program-credentials/reports_v2/MITx/<program uuid>/
        learner_report__<YYYY-MM-DD>.csv
        program_cohort_report__<YYYY-MM-DD>.csv

Each learner report is a full snapshot of its program: one row per learner and
course run, so a day of reports repeats almost every row of the day before.
The Airbyte source-s3 connection "S3 edX.org Program Credentials → S3 Data
Lake" appends every report into raw__edxorg__program_learner_report. This
source appends them into raw__edxorg__s3__program_learner_report, stamped with
the file they came from, and staging keeps each key's row from its newest file.
The newest file is not always the newest mtime: the backlog was copied into the
landing zone in bulk (1,580 files on 2024-02-19), so older reports' mtimes do
not follow the date in their names. Each program's latest report does carry
its newest mtime.
The cohort reports are a different row that nothing reads, and are not loaded.

On 2026-10-23 the prefix held 23,205 learner reports (4.93 TB) for 24
programs, dated from 2023-07-05 on. A day's reports were 5.77 GB, the largest
file 1.02 GB, and every header sampled across the backlog was
``REPORT_COLUMNS``. The whole backlog is loaded, not only the newest reports:
staging keeps a row for a learner who has since left a program's report, and
277,815 of the 14,970,432 keys in the Airbyte table are only in older files.

Run standalone:
    DLT_PROFILE=dev python -m ol_dlt.sources.edxorg_program_reports
"""

import logging
from collections.abc import Generator, Iterable, Iterator
from typing import Any

import dlt
import pyarrow as pa
import s3fs
from dlt.sources.filesystem import FileItemDict
from pyarrow import csv

from ol_dlt import config
from ol_dlt.file_metadata import FILE_METADATA_COLUMNS, add_file_metadata
from ol_dlt.sources.edxorg_s3 import edxorg_files

logger = logging.getLogger(__name__)

# Always the production landing zone, whatever the profile: it is where the
# files are read from, not where rows are written to.
LANDING_BUCKET = "s3://ol-data-lake-landing-zone-production/"
FILE_GLOB = "edxorg-program-credentials/reports_v2/MITx/**/learner_report__*.csv"

RAW_TABLE = "raw__edxorg__s3__program_learner_report"
PIPELINE_NAME = "edxorg_program_learner_report"

# dlt's Iceberg writer holds a whole load in memory: about 0.9 GB + 1.6x the
# batch on edxorg TSVs (see ol_dlt.sources.edxorg_s3), and 2.58 GB for the
# largest report (1.02 GB) loaded alone into a local Iceberg table. That
# predicted 4.4 GB for a 2 GiB batch, but the first production run (2026-10-27)
# peaked at 8.06 GB, so the schedule raises its run pod's memory limit (see
# edxorg_program_reports_ingest_schedule). A day of reports is three batches,
# and the 4.93 TB backlog about 2,300, which at the 200-batch cap per run is
# about 12 daily runs.
BUDGET_BYTES = 2 * 1024**3

# Bytes pyarrow parses per batch. A report row is ~300 bytes, so ~50k rows.
BLOCK_BYTES = 16 * 1024**2

# The header of every report, in order. The raw columns are these lowercased
# with underscores for spaces, which is what dlt's naming convention would make
# of them anyway; naming them here keeps the table's columns fixed rather than
# following whatever a file's header says.
REPORT_COLUMNS = (
    "AUTHORING INSTITUTION",
    "PROGRAM TITLE",
    "PROGRAM UUID",
    "PROGRAM TYPE",
    "USER ID",
    "NAME",
    "USERNAME",
    "USER KEY",
    "COURSE TITLE",
    "COURSE RUN KEY",
    "COURSE RUN START DATE",
    "EXTERNAL COURSE KEY",
    "USER ROLES",
    "TRACK",
    "PURCHASED AS BUNDLE",
    "GRADE",
    "LETTER GRADE",
    "DATE FIRST ENROLLED",
    "DATE LAST UNENROLLED",
    "CURRENTLY ENROLLED",
    "DATE FIRST UPGRADED TO VERIFIED",
    "COMPLETED PROGRAM",
    "DATE PROGRAM CERTIFICATE AWARDED",
    "COMPLETED",
    "DATE COMPLETED",
    "LAST ACTIVITY DATE",
)
RAW_COLUMNS = tuple(name.lower().replace(" ", "_") for name in REPORT_COLUMNS)

# Every value stays the text the report holds, as Airbyte landed it. The
# reports write a missing value as the string "null" and an empty one as "",
# and staging tests for 'null', so neither may become a SQL NULL here.
_CONVERT_OPTIONS = csv.ConvertOptions(
    column_types=dict.fromkeys(REPORT_COLUMNS, pa.string()),
    null_values=[],
    strings_can_be_null=False,
    quoted_strings_can_be_null=False,
)
# Names and course titles are free text, so a quoted value may hold a newline.
_PARSE_OPTIONS = csv.ParseOptions(newlines_in_values=True)


class LearnerReportHeaderError(Exception):
    """A learner report's header is not ``REPORT_COLUMNS``."""


def read_report(source: Any, file_url: str) -> Iterator[pa.RecordBatch]:  # noqa: ANN401
    """Stream one learner report as all-text Arrow batches with the raw column names.

    :param source: Binary file object holding the CSV.
    :param file_url: The file's URL, named in the error for a changed header.
    :returns: The report's rows, ``BLOCK_BYTES`` of CSV at a time.
    :rtype: Iterator[pa.RecordBatch]
    :raises LearnerReportHeaderError: The header is not ``REPORT_COLUMNS``. A
        renamed or dropped column would otherwise load as nulls under a name
        staging no longer reads, or as a new column nothing reads.
    """
    reader = csv.open_csv(
        source,
        read_options=csv.ReadOptions(block_size=BLOCK_BYTES),
        parse_options=_PARSE_OPTIONS,
        convert_options=_CONVERT_OPTIONS,
    )
    if tuple(reader.schema.names) != REPORT_COLUMNS:
        msg = (
            f"{file_url} has the header {reader.schema.names}, "
            f"not the learner report's {list(REPORT_COLUMNS)}"
        )
        raise LearnerReportHeaderError(msg)
    for batch in reader:
        yield pa.RecordBatch.from_arrays(batch.columns, names=list(RAW_COLUMNS))


@dlt.transformer(standalone=True)
def read_learner_reports(items: Iterable[FileItemDict]) -> Iterator[pa.RecordBatch]:
    """Read each learner report, stamped with the file it came from."""
    for item in items:
        if not item.get("size_in_bytes"):
            logger.warning("Skipping empty learner report %s.", item["file_url"])
            continue
        with item.open() as source:
            for batch in read_report(source, item["file_url"]):
                yield add_file_metadata(
                    batch,
                    source_file=item["file_url"],
                    modified_at=item.get("modification_date"),
                )


@dlt.source(name="edxorg_program_reports")
def edxorg_program_reports_source(
    bucket_url: str = LANDING_BUCKET,
    table_format: config.TableFormat | None = None,
    budget_bytes: int = BUDGET_BYTES,
) -> Generator[Any]:
    """Load the learner reports from the landing zone, one budget at a time.

    The caller re-runs the source until a batch reads nothing, as for edxorg_s3.

    :param bucket_url: Bucket ``FILE_GLOB`` is relative to.
    :param table_format: ``native`` or ``iceberg``; defaults to the active
        profile's.
    :param budget_bytes: How much report CSV one load may cover.
    """
    # No explicit credentials, so s3fs refreshes IRSA credentials itself (see
    # ol_dlt.sources.edxorg_s3 for why dlt's own credentials expire mid-run).
    files = edxorg_files(
        bucket_url=bucket_url,
        file_globs=[FILE_GLOB],
        credentials=s3fs.S3FileSystem(),
        budget_bytes=budget_bytes,
    )
    yield (
        (files | read_learner_reports())
        .with_name(RAW_TABLE)
        .apply_hints(
            table_name=RAW_TABLE,
            # Append: every report is a new file of mostly unchanged rows, and
            # an Iceberg merge of ~15M rows a day does not finish (see
            # edxorg_s3). deduplicate_raw_table orders by _file_modified_at,
            # the unit's raw_metadata_column, as it did by Airbyte's columns.
            write_disposition="append",
            table_format=table_format or config.active_table_format(),
            columns={**config.DLT_LOAD_ID_COLUMN, **FILE_METADATA_COLUMNS},
        )
    )


def edxorg_program_reports_pipeline() -> dlt.Pipeline:
    """Return the pipeline for the learner reports.

    The pipeline name keys the modification_date cursor, so it must stay
    stable: renaming it re-reads the landing zone and, under append, inserts
    every row again. The "edxorg" bucket prefix is the one edxorg_s3 and the
    edxorg course blocks write under.
    """
    return config.pipeline_for("edxorg", pipeline_name=PIPELINE_NAME)
