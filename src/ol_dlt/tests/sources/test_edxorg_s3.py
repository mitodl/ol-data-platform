"""Unit tests for the edxorg_s3 source.

Materialization is not tested here: the source reads TSVs from the production S3
landing zone and cannot run hermetically. Coverage focuses on the pure per-run
deduplication logic and on the DuckDB CSV reader options, which are where the
subtle correctness bugs lived.
"""

import io
import json
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
    """A stray CR in a free-text column must not abort the resource.

    The bare CR splits row 1 into two ragged fragments, so ``ignore_errors``
    drops it -- losing that row is the accepted cost. What matters is that the
    loss stays confined to the offending row: row 2 must survive intact rather
    than the sniffer taking the whole table down with it.
    """
    relation = duckdb.from_csv_auto(
        io.BytesIO(_MIXED_NEWLINE_TSV), **edxorg_s3._CSV_READER_OPTIONS
    )
    assert relation.columns == ["id", "user_id", "bio", "goals"]
    # Exact contents, not just "non-empty": the well-formed row lands whole and
    # the malformed one is the only casualty.
    assert relation.fetchall() == [("2", "20", "plain bio", "grow")]


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


def test_reader_options_undouble_rfc4180_quotes() -> None:
    """An embedded `"` must survive, not come back doubled.

    The upstream edxorg_archive asset writes with polars'
    ``quote_style="necessary"``, which escapes a `"` inside a quoted field by
    doubling it. Left to the sniffer, DuckDB picks a quote/escape pair that
    does not undouble it -- so pinning ``escapechar`` is what keeps the value
    intact rather than silently gaining a character.
    """
    quoted = b'id\tbio\n1\t"he said ""hi"" loudly"\n'
    relation = duckdb.from_csv_auto(io.BytesIO(quoted), **edxorg_s3._CSV_READER_OPTIONS)
    assert relation.fetchall() == [("1", 'he said "hi" loudly')]

    unpinned = {
        k: v
        for k, v in edxorg_s3._CSV_READER_OPTIONS.items()
        if k not in {"quotechar", "escapechar"}
    }
    assert duckdb.from_csv_auto(io.BytesIO(quoted), **unpinned).fetchall() == [
        ("1", 'he said ""hi"" loudly')
    ]


def test_reader_options_still_read_legacy_unquoted_files() -> None:
    """Bare values in the older files must parse exactly as they always did."""
    legacy = b"id\tbio\tgoals\n1\tplain bio\tlearn\n2\tC:\\Users\\bob\tgrow\n"
    expected = [("1", "plain bio", "learn"), ("2", "C:\\Users\\bob", "grow")]

    assert (
        duckdb.from_csv_auto(
            io.BytesIO(legacy), **edxorg_s3._CSV_READER_OPTIONS
        ).fetchall()
        == expected
    )


def test_reader_options_repair_json_in_existing_files() -> None:
    """Pinning escapechar also fixes files already sitting in the landing zone.

    ``auth_userprofile.meta`` holds JSON, and edX's original dump quotes it.
    The archive asset reads with ``quote_char=None``, so that quoting survives
    into the landing-zone file as literal text. Without a pinned escapechar the
    reader strips the outer quotes but leaves the inner doubling, yielding
    invalid JSON -- measured at 4,646 unparseable ``meta`` values across a
    77-file production sample, versus 182 with the pins in place.
    """
    meta = b'id\tmeta\n1\t"{""skills_builder"": """", ""rv"": """"}"\n'
    (row,) = duckdb.from_csv_auto(
        io.BytesIO(meta), **edxorg_s3._CSV_READER_OPTIONS
    ).fetchall()
    assert json.loads(row[1]) == {"skills_builder": "", "rv": ""}


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


# ── read_edxorg_tsv ───────────────────────────────────────────────────────────


class _FakeFileItem(dict[str, Any]):
    """The three FileItemDict members the reader touches."""

    def __init__(self, url: str, content: bytes) -> None:
        super().__init__(
            file_url=url, file_name=url.rsplit("/", 1)[-1], size_in_bytes=len(content)
        )
        self._content = content

    def open(self):  # noqa: ANN201
        return io.BytesIO(self._content)


def _read(items: list[_FakeFileItem]) -> list[pa.Table]:
    """Drive the reader's generator directly, past dlt's transformer wrapper."""
    return list(
        edxorg_s3.read_edxorg_tsv._pipe.gen(  # noqa: SLF001
            items, **edxorg_s3._CSV_READER_OPTIONS
        )
    )


def test_reader_returns_rows_for_a_well_formed_file() -> None:
    batches = _read([_FakeFileItem("s3://bucket/clean.tsv", _CLEAN_TSV)])

    rows = [row for batch in batches for row in batch.to_pylist()]
    assert [r["id"] for r in rows] == ["1", "2"]


def test_reader_skips_an_empty_file_instead_of_failing_the_table() -> None:
    """from_csv_auto cannot infer a dialect from zero bytes, and reports it in
    the same words as a genuinely malformed file.

    An empty export is not an error -- there is nothing in it -- so it must not
    take the whole table's load down with it.
    """
    batches = _read(
        [
            _FakeFileItem("s3://bucket/empty.tsv", b""),
            _FakeFileItem("s3://bucket/clean.tsv", _CLEAN_TSV),
        ]
    )

    rows = [row for batch in batches for row in batch.to_pylist()]
    assert [r["id"] for r in rows] == ["1", "2"], "the readable file still loads"


def test_reader_names_the_s3_object_it_could_not_read() -> None:
    """DAGSTER-1C..1V reported a sniffing failure against
    ``DUCKDB_INTERNAL_OBJECTSTORE://e3d60147029d6cb5`` -- DuckDB's handle for
    the open file object, which maps to nothing anyone can go and look at.

    Whatever the underlying cause turns out to be, the error has to say which
    object it was or nobody can reproduce it.
    """
    # Two header fields, a data row with far more -- unreadable under the pinned
    # dialect with strict mode off and null padding deliberately unset.
    unreadable = b'id\tname\n1\t"unterminated quote\n'

    with pytest.raises(edxorg_s3.EdxorgTSVUnreadableError) as raised:
        _read(
            [_FakeFileItem("s3://bucket/db_table/auth_user/prod/x/bad.tsv", unreadable)]
        )

    assert "s3://bucket/db_table/auth_user/prod/x/bad.tsv" in str(raised.value)
