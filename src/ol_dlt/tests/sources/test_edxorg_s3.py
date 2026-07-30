"""Unit tests for the edxorg_s3 source.

Materialization is not tested here: the source reads TSVs from the production S3
landing zone and cannot run hermetically. Coverage focuses on the pure per-run
deduplication logic and on the DuckDB CSV reader options, which are where the
subtle correctness bugs lived.
"""

import io
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from ol_dlt.sources import edxorg_s3


def _table(rows: list[dict[str, Any]]) -> pa.Table:
    return pa.Table.from_pylist(rows)


# An auth_userprofile-shaped dump whose free-text `bio` carries a bare CR while
# the file is otherwise LF-terminated -- exactly what a user pasting into a
# textarea produces. Mixed newlines are a strict-mode dialect violation, and the
# sniffer runs before ignore_errors, so under strict mode this one row takes the
# whole resource down with "It was not possible to automatically detect the CSV
# parsing dialect".
_MIXED_NEWLINE_TSV = (
    b"id\tuser_id\tbio\tgoals\n"
    b"1\t10\tI want to learn\rand grow\tlearn\n"
    b"2\t20\tplain bio\tgrow\n"
)

# The same table with consistent LF endings: must parse the same either way.
_CLEAN_TSV = b"id\tuser_id\tbio\tgoals\n1\t10\tbio one\tlearn\n2\t20\tbio two\tgrow\n"


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


def test_deduplicator_keeps_null_distinct_from_empty_string() -> None:
    dedup = edxorg_s3._make_deduplicator()
    tbl = _table(
        [
            {"row_hash": None, "extracted_course_key": "c1"},
            {"row_hash": "", "extracted_course_key": "c1"},
        ]
    )
    assert dedup(tbl).num_rows == 2  # noqa: PLR2004


def test_deduplicator_passes_through_without_key_columns() -> None:
    dedup = edxorg_s3._make_deduplicator()
    tbl = _table([{"some_col": "x"}, {"some_col": "x"}])
    # Missing primary-key columns: do not drop anything.
    assert dedup(tbl).num_rows == 2  # noqa: PLR2004


def test_reader_options_parse_tsv_with_mixed_newlines() -> None:
    """A stray CR in a free-text column must not abort the resource."""
    relation = duckdb.from_csv_auto(
        io.BytesIO(_MIXED_NEWLINE_TSV), **edxorg_s3._CSV_READER_OPTIONS
    )
    assert relation.columns == ["id", "user_id", "bio", "goals"]
    assert relation.fetchall()  # the well-formed rows still land


def test_reader_options_disable_strict_mode() -> None:
    """Guard the specific option, and prove it is what saves the parse."""
    assert edxorg_s3._CSV_READER_OPTIONS["strict_mode"] is False

    strict = {**edxorg_s3._CSV_READER_OPTIONS, "strict_mode": True}
    with pytest.raises(duckdb.InvalidInputException, match="sniffing"):
        duckdb.from_csv_auto(io.BytesIO(_MIXED_NEWLINE_TSV), **strict)


def test_reader_options_leave_well_formed_tsv_unchanged() -> None:
    """Relaxing strict mode must not alter parsing of clean dumps."""
    relaxed = duckdb.from_csv_auto(
        io.BytesIO(_CLEAN_TSV), **edxorg_s3._CSV_READER_OPTIONS
    )
    strict = duckdb.from_csv_auto(
        io.BytesIO(_CLEAN_TSV), **{**edxorg_s3._CSV_READER_OPTIONS, "strict_mode": True}
    )
    assert relaxed.columns == strict.columns
    assert relaxed.fetchall() == strict.fetchall()


def test_reader_options_do_not_pad_ragged_rows() -> None:
    """null_padding would invent per-file `columnN` columns and churn schemas."""
    assert "null_padding" not in edxorg_s3._CSV_READER_OPTIONS
    ragged = b"id\tbio\n1\tbio\n2\tbio\twith\textra\ttabs\n"
    relation = duckdb.from_csv_auto(io.BytesIO(ragged), **edxorg_s3._CSV_READER_OPTIONS)
    assert relation.columns == ["id", "bio"]


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


def test_pipeline_for_gives_each_table_a_distinct_stable_name() -> None:
    """Each table's pipeline needs its own local working-directory identity.

    Concurrent table loads sharing one pipeline_name race on dlt's local
    extract/normalize staging files (reproduced independently: concurrent runs
    sharing a pipeline_name fail with NormalizeJobFailed/FileNotFoundError).
    """
    a = edxorg_s3.edxorg_s3_pipeline_for("auth_user")
    b = edxorg_s3.edxorg_s3_pipeline_for("student_courseenrollment")
    assert a.pipeline_name == "edxorg_s3__auth_user"
    assert b.pipeline_name == "edxorg_s3__student_courseenrollment"
    # Same name every call -- required so the modification_date incremental
    # cursor is found again on the next run instead of resetting.
    assert edxorg_s3.edxorg_s3_pipeline_for("auth_user").pipeline_name == (
        a.pipeline_name
    )


def test_pipeline_for_shares_destination_with_singleton_pipeline() -> None:
    """Per-table pipelines still land in the same edxorg destination bucket."""
    per_table = edxorg_s3.edxorg_s3_pipeline_for("auth_user")
    singleton = edxorg_s3.edxorg_s3_pipeline
    # destination_name is just the destination TYPE (e.g. "filesystem") and
    # would match even if the bucket/prefix diverged -- assert the actual
    # bucket_url so this test guards the intended behavior.
    assert (
        per_table.destination.config_params["bucket_url"]
        == singleton.destination.config_params["bucket_url"]
    )
    assert per_table.dataset_name == singleton.dataset_name
