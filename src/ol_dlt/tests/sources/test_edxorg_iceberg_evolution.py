"""Adding the provenance columns to an Iceberg table that already holds rows.

The production edxorg raw tables predate `_source_file` and
`_file_modified_at` (certificates_generatedcertificate holds 925 MB written
under the old merge disposition), so the first load after this change is a
schema evolution against a populated table, not a fresh create. Two things
can fail there and neither shows up in a test that only inspects
`compute_table_schema()`: pyiceberg refuses to add a REQUIRED column to a
table with rows, and a timestamp that is not microsecond-precision UTC is
rejected or silently downcast on write.

These run a real dlt pipeline against a local Iceberg table backed by a
SQLite catalog, so they exercise the same writer path production uses.
"""

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import dlt
import pytest

from ol_dlt import file_metadata

_MODIFIED_AT = datetime(2026, 3, 7, 10, 25, tzinfo=UTC)
_TABLE = "raw__edxorg__s3__tables__auth_user"


@pytest.fixture
def pipeline(sqlite_iceberg_lake: Path, tmp_path: Path) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="edxorg_iceberg_evolution_test",
        destination=dlt.destinations.filesystem(
            bucket_url=sqlite_iceberg_lake.as_uri()
        ),
        dataset_name="raw",
        pipelines_dir=str(tmp_path / "pipelines"),
    )


def _load(
    pipeline: dlt.Pipeline,
    rows: list[dict[str, Any]],
    *,
    with_provenance: bool = True,
) -> None:
    """Load ``rows``, declaring the provenance columns only when asked.

    The first load of a test leaves them out on purpose: that is the shape of
    the tables already in production, and declaring them would create the
    columns up front and test nothing.
    """

    @dlt.resource(
        name=_TABLE,
        write_disposition="append",
        table_format="iceberg",
        columns=dict(file_metadata.FILE_METADATA_COLUMNS) if with_provenance else {},
    )
    def rows_resource() -> Any:
        yield rows

    pipeline.run(rows_resource(), loader_file_format="parquet")


def _stamped(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            file_metadata.SOURCE_FILE_COLUMN: "s3://bucket/new.tsv",
            file_metadata.FILE_MODIFIED_AT_COLUMN: _MODIFIED_AT,
        }
        for row in rows
    ]


def _assert_columns_absent(pipeline: dlt.Pipeline) -> None:
    """Fail loudly if the first load already created the columns.

    Without this the evolution test passes vacuously: a table created with
    the columns in place is never evolved.
    """
    columns = pipeline.default_schema.get_table_columns(_TABLE)
    assert file_metadata.SOURCE_FILE_COLUMN not in columns
    assert file_metadata.FILE_MODIFIED_AT_COLUMN not in columns


def test_provenance_columns_land_on_a_table_that_already_has_rows(
    pipeline: dlt.Pipeline,
) -> None:
    """The first load after this change evolves a populated production table.

    Declaring the columns REQUIRED would fail here rather than backfilling
    nulls, which is why file_metadata declares them nullable.
    """
    _load(pipeline, [{"id": "1", "row_hash": "h1"}], with_provenance=False)
    _assert_columns_absent(pipeline)

    _load(pipeline, _stamped([{"id": "2", "row_hash": "h2"}]))

    with pipeline.sql_client() as client:
        rows = sorted(
            client.execute_sql(
                f"select id, {file_metadata.SOURCE_FILE_COLUMN}, "  # noqa: S608
                f"{file_metadata.FILE_MODIFIED_AT_COLUMN} from {_TABLE}"
            )
        )

    assert rows == [
        ("1", None, None),
        ("2", "s3://bucket/new.tsv", _MODIFIED_AT),
    ], "pre-existing rows keep nulls; the new load carries its provenance"


def test_the_timestamp_survives_the_iceberg_round_trip(
    pipeline: dlt.Pipeline,
) -> None:
    """Iceberg stores timestamptz at microsecond precision.

    A nanosecond value would be rejected or downcast on write, and a naive
    one would come back without its zone, which is what staging orders by.
    """
    _load(pipeline, _stamped([{"id": "1", "row_hash": "h1"}]))

    with pipeline.sql_client() as client:
        (value,), *_ = client.execute_sql(
            f"select {file_metadata.FILE_MODIFIED_AT_COLUMN} from {_TABLE}"  # noqa: S608
        )

    assert value == _MODIFIED_AT
    assert value.tzinfo is not None
