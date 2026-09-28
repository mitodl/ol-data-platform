"""Provenance columns every file-based (S3) source stamps onto its rows.

Two rows read out of two different exports of the same course are byte-identical
apart from these columns, so without them nothing downstream can tell a
re-exported duplicate from a real one, order versions of a record, or scope a
compaction pass to the copies it means to drop.

``_file_modified_at`` is the file's S3 modification time, not load time: a
backfill walks the landing zone in whatever order it likes, and
``_dlt_load_id`` records that order rather than the order the data was exported
in. It is the column ``raw_metadata_column`` should name in the ingestion
inventory once a unit has reloaded, so ``deduplicate_raw_table`` orders staging
rows by when the source file was written.
"""

from datetime import datetime
from typing import Any

import pyarrow as pa

SOURCE_FILE_COLUMN = "_source_file"
FILE_MODIFIED_AT_COLUMN = "_file_modified_at"

# Microseconds, matching Iceberg's timestamptz, and UTC because S3 reports
# modification times in UTC.
FILE_MODIFIED_AT_TYPE = pa.timestamp("us", tz="UTC")

# Nullable for the same reason as config.DLT_LOAD_ID_COLUMN: pyiceberg refuses
# to add a REQUIRED column to a table that already holds rows, so declaring
# these required would fail the first load against any existing table rather
# than backfilling nulls into the rows that predate them.
FILE_METADATA_COLUMNS: dict[str, dict[str, Any]] = {
    SOURCE_FILE_COLUMN: {"data_type": "text", "nullable": True},
    FILE_MODIFIED_AT_COLUMN: {
        "data_type": "timestamp",
        "nullable": True,
        "precision": 6,
    },
}


def add_file_metadata(
    batch: pa.RecordBatch | pa.Table,
    *,
    source_file: str,
    modified_at: datetime | None,
) -> pa.RecordBatch | pa.Table:
    """Stamp ``batch`` with the file it was read from and that file's mtime.

    Args:
        batch: Arrow batch or table of rows read out of one file.
        source_file: Full URL of that file, e.g. its ``s3://`` URL.
        modified_at: The file's modification time, or None if the filesystem
            did not report one.

    Returns:
        The batch with both columns appended.
    """
    rows = batch.num_rows
    return batch.append_column(
        SOURCE_FILE_COLUMN,
        pa.repeat(pa.scalar(source_file, type=pa.string()), rows),
    ).append_column(
        FILE_MODIFIED_AT_COLUMN,
        pa.repeat(pa.scalar(modified_at, type=FILE_MODIFIED_AT_TYPE), rows),
    )
