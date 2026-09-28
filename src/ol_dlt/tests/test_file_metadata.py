"""Unit tests for the shared file-provenance columns."""

from datetime import UTC, datetime

import pyarrow as pa

from ol_dlt import file_metadata

_MODIFIED_AT = datetime(2026, 3, 7, 10, 25, tzinfo=UTC)


def _batch() -> pa.RecordBatch:
    return pa.RecordBatch.from_pylist([{"id": "1"}, {"id": "2"}])


def test_stamps_both_columns_on_a_record_batch() -> None:
    stamped = file_metadata.add_file_metadata(
        _batch(), source_file="s3://bucket/a.tsv", modified_at=_MODIFIED_AT
    )

    assert stamped.to_pylist() == [
        {
            "id": "1",
            file_metadata.SOURCE_FILE_COLUMN: "s3://bucket/a.tsv",
            file_metadata.FILE_MODIFIED_AT_COLUMN: _MODIFIED_AT,
        },
        {
            "id": "2",
            file_metadata.SOURCE_FILE_COLUMN: "s3://bucket/a.tsv",
            file_metadata.FILE_MODIFIED_AT_COLUMN: _MODIFIED_AT,
        },
    ]


def test_stamps_a_table_as_well_as_a_batch() -> None:
    """Both turn up in practice: the readers yield either."""
    stamped = file_metadata.add_file_metadata(
        pa.Table.from_batches([_batch()]),
        source_file="s3://bucket/a.tsv",
        modified_at=_MODIFIED_AT,
    )

    assert isinstance(stamped, pa.Table)
    assert stamped.column(file_metadata.SOURCE_FILE_COLUMN).to_pylist() == [
        "s3://bucket/a.tsv",
        "s3://bucket/a.tsv",
    ]


def test_timestamp_column_is_microsecond_utc() -> None:
    """Iceberg stores timestamptz at microsecond precision.

    Nanoseconds would force a lossy downcast at write time, and a naive
    timestamp would land as a local-time value with no zone at all.
    """
    stamped = file_metadata.add_file_metadata(
        _batch(), source_file="s3://bucket/a.tsv", modified_at=_MODIFIED_AT
    )

    field = stamped.schema.field(file_metadata.FILE_MODIFIED_AT_COLUMN)
    assert field.type == pa.timestamp("us", tz="UTC")


def test_accepts_a_file_with_no_reported_modification_time() -> None:
    """Not every filesystem reports one; a null beats failing the load."""
    stamped = file_metadata.add_file_metadata(
        _batch(), source_file="s3://bucket/a.tsv", modified_at=None
    )

    assert stamped.column(file_metadata.FILE_MODIFIED_AT_COLUMN).to_pylist() == [
        None,
        None,
    ]


def test_columns_are_declared_nullable() -> None:
    """pyiceberg refuses to add a REQUIRED column to a table holding rows.

    Declaring these required would fail the first load against every table
    that predates them instead of backfilling nulls.
    """
    assert all(
        column["nullable"] for column in file_metadata.FILE_METADATA_COLUMNS.values()
    )
