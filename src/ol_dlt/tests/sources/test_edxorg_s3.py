"""Unit tests for the edxorg_s3 source.

Materialization is not tested here: the source reads TSVs from the production S3
landing zone and cannot run hermetically. Coverage focuses on the pure per-run
deduplication logic, which is where the subtle correctness bug lived.
"""

from typing import Any

import pyarrow as pa

from ol_dlt.sources import edxorg_s3


def _table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows)


def test_deduplicator_drops_repeat_keys_within_run() -> None:
    dedup = edxorg_s3._make_deduplicator()
    first = _table(
        [
            {"row_hash": "h1", "extracted_course_key": "c1", "v": "a"},
            {"row_hash": "h1", "extracted_course_key": "c1", "v": "b"},  # dup key
            {"row_hash": "h2", "extracted_course_key": "c1", "v": "c"},
        ]
    )
    out = dedup(first)
    assert out.num_rows == 2  # noqa: PLR2004
    # The same key appearing in a later batch is also dropped (stateful).
    second = _table([{"row_hash": "h1", "extracted_course_key": "c1", "v": "d"}])
    assert dedup(second).num_rows == 0


def test_deduplicator_keeps_same_hash_different_course() -> None:
    dedup = edxorg_s3._make_deduplicator()
    tbl = _table(
        [
            {"row_hash": "h1", "extracted_course_key": "c1"},
            {"row_hash": "h1", "extracted_course_key": "c2"},
        ]
    )
    assert dedup(tbl).num_rows == 2  # noqa: PLR2004


def test_deduplicator_passes_through_without_key_columns() -> None:
    dedup = edxorg_s3._make_deduplicator()
    tbl = _table([{"some_col": "x"}, {"some_col": "x"}])
    # Missing primary-key columns: do not drop anything.
    assert dedup(tbl).num_rows == 2  # noqa: PLR2004


def test_source_yields_one_resource_per_table() -> None:
    source = edxorg_s3.edxorg_s3_source(
        tables=["auth_user", "student_courseenrollment"]
    )
    names = set(source.resources.keys())
    assert names == {
        "raw__edxorg__s3__tables__auth_user",
        "raw__edxorg__s3__tables__student_courseenrollment",
    }


def test_source_with_no_tables_yields_nothing() -> None:
    source = edxorg_s3.edxorg_s3_source(tables=[])
    assert dict(source.resources) == {}
