"""The pyiceberg upsert guard: see ol_dlt.iceberg_upsert_guard for the bug."""

from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import Table as IcebergTable

from ol_dlt import iceberg_upsert_guard

_DLT_TABLE_SCHEMA: Any = {
    "name": "t",
    "x-merge-strategy": "upsert",
    "columns": {
        "row_hash": {
            "name": "row_hash",
            "data_type": "text",
            "nullable": False,
            "primary_key": True,
        },
        "extracted_course_key": {
            "name": "extracted_course_key",
            "data_type": "text",
            "nullable": False,
            "primary_key": True,
        },
        "grade": {"name": "grade", "data_type": "text", "nullable": True},
        "_dlt_load_id": {"name": "_dlt_load_id", "data_type": "text", "nullable": True},
    },
}
_EXISTING = pa.table(
    {
        "row_hash": ["h1", "h2"],
        "extracted_course_key": ["c1", "c1"],
        "grade": ["0.5", "0.7"],
    }
)
# Matches h2 (an update) and adds h3 (an insert), carrying the new column.
_BATCH = pa.table(
    {
        "row_hash": ["h2", "h3"],
        "extracted_course_key": ["c1", "c1"],
        "grade": ["0.9", "0.1"],
        "_dlt_load_id": ["L1", "L1"],
    }
)


@pytest.fixture
def table(tmp_path: Path) -> IcebergTable:
    """A table left the way production was: new column, no snapshot under it."""
    catalog = SqlCatalog(
        "test", uri=f"sqlite:///{tmp_path}/catalog.db", warehouse=f"file://{tmp_path}"
    )
    catalog.create_namespace("ns")
    table = catalog.create_table("ns.t", schema=_EXISTING.schema)
    table.append(_EXISTING)
    with table.update_schema() as update:
        update.union_by_name(_BATCH.schema)
    return table


def test_dlt_merges_go_through_the_guard() -> None:
    assert dlt_pyiceberg.merge_iceberg_table is iceberg_upsert_guard.merge_iceberg_table


def test_unguarded_upsert_still_fails(table: IcebergTable) -> None:
    """If this stops raising, pyiceberg fixed #3105 and the guard can be removed."""
    with pytest.raises(ValueError, match="field names are not matching"):
        iceberg_upsert_guard._dlt_merge_iceberg_table(  # noqa: SLF001
            table=table, data=_BATCH, schema=_DLT_TABLE_SCHEMA, load_table_name="t"
        )


def test_guarded_upsert_updates_matches_and_inserts_new_rows(
    table: IcebergTable,
) -> None:
    dlt_pyiceberg.merge_iceberg_table(
        table=table, data=_BATCH, schema=_DLT_TABLE_SCHEMA, load_table_name="t"
    )

    rows = sorted(table.scan().to_arrow().to_pylist(), key=lambda r: r["row_hash"])
    assert [(r["row_hash"], r["grade"], r["_dlt_load_id"]) for r in rows] == [
        ("h1", "0.5", None),
        ("h2", "0.9", "L1"),
        ("h3", "0.1", "L1"),
    ]
