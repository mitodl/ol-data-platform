"""Unit tests for the edxorg_s3 source.

Materialization is not tested here: the source reads TSVs from the production S3
landing zone and cannot run hermetically. Coverage focuses on the reader -- the
DuckDB CSV options, how each file is handed to DuckDB, and the provenance
columns every row carries -- which is where the subtle correctness bugs lived.
"""

import contextlib
import io
import json
import tempfile
from collections.abc import Iterable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pytest

from ol_dlt import file_metadata
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


_MODIFIED_AT = datetime(2026, 3, 7, 10, 25, tzinfo=UTC)


class _FakeFileItem(dict[str, Any]):
    """The FileItemDict members the reader touches."""

    def __init__(
        self, url: str, content: bytes, modification_date: datetime = _MODIFIED_AT
    ) -> None:
        super().__init__(
            file_url=url,
            file_name=url.rsplit("/", 1)[-1],
            size_in_bytes=len(content),
            modification_date=modification_date,
        )
        self._content = content

    def open(self):  # noqa: ANN201
        return io.BytesIO(self._content)


def _read(
    items: Iterable[_FakeFileItem], budget_bytes: int | None = None
) -> list[pa.Table]:
    """Drive the reader's generator directly, past dlt's transformer wrapper."""
    kwargs: dict[str, Any] = dict(edxorg_s3._CSV_READER_OPTIONS)  # noqa: SLF001
    if budget_bytes is not None:
        kwargs["budget_bytes"] = budget_bytes
    return list(
        edxorg_s3.read_edxorg_tsv._pipe.gen(items, **kwargs)  # noqa: SLF001
    )


def _rows(batches: list[pa.Table]) -> list[dict[str, Any]]:
    return [row for batch in batches for row in batch.to_pylist()]


def test_reader_stamps_every_row_with_its_source_file() -> None:
    """Rows must carry the file they came from and when it was written.

    Two exports of the same course produce byte-identical rows apart from
    these columns, so without them the post-load dedupe cannot tell a
    re-exported copy from a distinct row, and staging has nothing to order
    versions of a record by.
    """
    earlier = datetime(2026, 2, 21, 7, 50, tzinfo=UTC)
    rows = _rows(
        _read(
            [
                _FakeFileItem("s3://bucket/old.tsv", _CLEAN_TSV, earlier),
                _FakeFileItem("s3://bucket/new.tsv", _CLEAN_TSV),
            ]
        )
    )

    assert [r[file_metadata.SOURCE_FILE_COLUMN] for r in rows] == [
        "s3://bucket/old.tsv",
        "s3://bucket/old.tsv",
        "s3://bucket/new.tsv",
        "s3://bucket/new.tsv",
    ]
    assert [r[file_metadata.FILE_MODIFIED_AT_COLUMN] for r in rows] == [
        earlier,
        earlier,
        _MODIFIED_AT,
        _MODIFIED_AT,
    ]


def test_reader_stamps_rows_recovered_by_the_unquoted_fallback() -> None:
    """The fallback path yields its own batches and must stamp them too."""
    rows = _rows(
        _read([_FakeFileItem("s3://bucket/legacy.tsv", _LEGACY_STRAY_QUOTE_TSV)])
    )

    assert {r[file_metadata.SOURCE_FILE_COLUMN] for r in rows} == {
        "s3://bucket/legacy.tsv"
    }


def test_resources_append_rather_than_merge() -> None:
    """Merge on (row_hash, extracted_course_key) can never update anything.

    row_hash covers every original CSV column, so a matched row is one whose
    content did not change between two exports of a course; the
    merge only bought one Iceberg commit per 1,000 rows and a whole load held
    in memory. Duplicates are removed after the load instead.
    """
    source = edxorg_s3.edxorg_s3_source(tables=["auth_user"])
    table = source.resources[
        "raw__edxorg__s3__tables__auth_user"
    ].compute_table_schema()

    assert table["write_disposition"] == "append"
    for column in (
        file_metadata.SOURCE_FILE_COLUMN,
        file_metadata.FILE_MODIFIED_AT_COLUMN,
    ):
        assert table["columns"][column]["nullable"] is True


def test_reader_hands_duckdb_a_path_not_a_file_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard for the OOMKills a file object caused.

    Given a file object, DuckDB ``read()``s the whole thing into its in-memory
    object store before parsing, which put a 14.5 GB courseware_studentmodule
    export entirely in RAM.
    """
    sources: list[object] = []
    real_from_csv_auto = duckdb.from_csv_auto

    def spy(source: object, **kwargs: Any) -> duckdb.DuckDBPyRelation:
        sources.append(source)
        return real_from_csv_auto(source, **kwargs)

    monkeypatch.setattr(duckdb, "from_csv_auto", spy)

    rows = _rows(_read([_FakeFileItem("s3://bucket/clean.tsv", _CLEAN_TSV)]))

    assert [r["id"] for r in rows] == ["1", "2"]
    assert len(sources) == 1
    assert isinstance(sources[0], str)


@pytest.mark.parametrize(
    "content",
    [
        pytest.param(_CLEAN_TSV, id="read"),
        pytest.param(b'id\tname\tbio\n1\t"open\tb\n2\n', id="unreadable"),
    ],
)
def test_reader_removes_its_local_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, content: bytes
) -> None:
    """Each download is deleted once its file is done, even when it fails.

    Left behind, a table's worth of downloads would fill the node's disk.
    """
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))

    with contextlib.suppress(edxorg_s3.EdxorgTSVUnreadableError):
        _read([_FakeFileItem("s3://bucket/file.tsv", content)])

    assert list(tmp_path.iterdir()) == []


def test_read_tsv_streams_instead_of_buffering_the_whole_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Regression guard for the OOMKills this streaming rewrite fixed.

    ``isgeneratorfunction`` alone would not catch a regression to
    ``yield from list(fetch_arrow(...))``, which still contains a ``yield``
    but buffers the whole file before producing anything -- exactly the
    OOMKill this rewrite fixed on courseware_studentmodule and
    auth_user/auth_userprofile once #2663 stopped those files' dialect
    failures from short-circuiting the read. Driving a fake ``fetch_arrow``
    through ``_read_tsv`` and checking what has been pulled after each
    ``next()`` proves the second batch is not produced until asked for.
    """
    pulled: list[int] = []

    def fake_fetch_arrow(_relation: object, _chunk_size: int) -> Iterator[int]:
        for i in range(2):
            pulled.append(i)
            yield i

    monkeypatch.setattr(edxorg_s3, "fetch_arrow", fake_fetch_arrow)

    path = tmp_path / "clean.tsv"
    path.write_bytes(_CLEAN_TSV)
    reader = edxorg_s3._read_tsv(  # noqa: SLF001
        path,
        5000,
        edxorg_s3._CSV_READER_OPTIONS,  # noqa: SLF001
    )

    assert next(reader) == 0
    assert pulled == [0], "the second batch must not be pulled until requested"

    assert next(reader) == 1
    assert pulled == [0, 1]


def test_reader_does_not_retry_unquoted_after_streaming_has_started(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A duckdb.Error after the first batch must fail loudly, not retry.

    Retrying unquoted here would re-read the whole file under a different
    dialect and splice it onto rows already yielded downstream from the
    pinned-dialect attempt -- duplicated, inconsistently parsed data reaching
    the destination instead of the loud failure the module otherwise insists
    on (see _read_unquoted_tsv's docstring).
    """

    def fake_fetch_arrow(
        _relation: object, _chunk_size: int
    ) -> Iterator[pa.RecordBatch]:
        yield _table([{"id": "1"}]).to_batches()[0]
        msg = "simulated mid-scan failure"
        raise duckdb.Error(msg)

    monkeypatch.setattr(edxorg_s3, "fetch_arrow", fake_fetch_arrow)

    with pytest.raises(edxorg_s3.EdxorgTSVUnreadableError, match="partway"):
        _read([_FakeFileItem("s3://bucket/clean.tsv", _CLEAN_TSV)])


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


# A legacy unquoted dump, shaped like the auth_userprofile file behind
# DAGSTER-30: every line is one record and the JSON quotes are literal text, but
# one bio happens to start with `"`, which under the pinned quote character
# opens a field that never closes.
_LEGACY_STRAY_QUOTE_TSV = (
    b"id\tbio\tmeta\n"
    + b"".join(f'{i}\tbio {i}\t{{""k"": ""v{i}""}}\n'.encode() for i in range(1, 40))
    + b'40\t"I love MIT\t{}\n'
    + b"".join(f"{i}\tbio {i}\t{{}}\n".encode() for i in range(41, 80))
)


def test_reader_recovers_a_legacy_file_with_a_stray_quote() -> None:
    with pytest.raises(duckdb.InvalidInputException, match="sniffing"):
        duckdb.from_csv_auto(
            io.BytesIO(_LEGACY_STRAY_QUOTE_TSV), **edxorg_s3._CSV_READER_OPTIONS
        )

    rows = _rows(
        _read([_FakeFileItem("s3://bucket/legacy.tsv", _LEGACY_STRAY_QUOTE_TSV)])
    )

    assert [r["id"] for r in rows] == [str(i) for i in range(1, 80)]
    assert rows[39]["bio"] == '"I love MIT'


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(_LEGACY_STRAY_QUOTE_TSV.rstrip(b"\n"), id="no_trailing_newline"),
        pytest.param(_LEGACY_STRAY_QUOTE_TSV + b"\n", id="trailing_blank_line"),
        pytest.param(_LEGACY_STRAY_QUOTE_TSV + b"\r\n", id="trailing_crlf_blank_line"),
        pytest.param(
            _LEGACY_STRAY_QUOTE_TSV.replace(b"50\tbio 50\t{}\n", b"50\tbio 50\t{}\n\n"),
            id="mid_file_blank_line",
        ),
    ],
)
def test_unquoted_fallback_line_count_matches_what_duckdb_reads(data: bytes) -> None:
    """DuckDB skips blank lines, so the row-count guard must not count them."""
    assert len(_rows(_read([_FakeFileItem("s3://bucket/legacy.tsv", data)]))) == 79  # noqa: PLR2004


def test_reader_names_the_s3_object_it_could_not_read() -> None:
    """A file neither read can take whole fails loudly, naming the object.

    The pinned read cannot sniff this file, and the unquoted read silently drops
    the short row, so it is refused rather than partially loaded. DAGSTER-1C..1V
    reported DuckDB's ``DUCKDB_INTERNAL_OBJECTSTORE://...`` handle instead of the
    S3 URL, which nobody can open.
    """
    unreadable = b'id\tname\tbio\n1\t"open\tb\n2\n'
    url = "s3://bucket/db_table/auth_userprofile/prod/x/bad.tsv"

    with pytest.raises(edxorg_s3.EdxorgTSVUnreadableError) as raised:
        _read([_FakeFileItem(url, unreadable)])

    assert url in str(raised.value)
