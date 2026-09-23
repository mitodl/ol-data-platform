"""Round-trip coverage for the TSV rewrite in ``process_edxorg_archive_bundle``.

The asset itself needs a Dagster context and a real tar archive, so it is not
exercised here. What *is* exercised is the part that silently lost data: the
``scan_csv`` -> ``sink_csv`` pairing, and whether the file it emits can still be
read back by the downstream ``ol_dlt`` edxorg_s3 source.

These tables carry user-entered free text (``auth_userprofile.bio``, ``.goals``,
``.mailing_address``), so a value occasionally holds a raw CR. Written unquoted,
that row becomes unrecoverable -- and a bare CR also makes the file
mixed-newline, which aborts DuckDB's dialect sniffer for the *whole* file.
"""

import io

import duckdb
import polars as pl
import pytest

# Mirrors the scan_csv call in edxorg.assets.edxorg_archive.
_SCAN_OPTIONS = {
    "has_header": True,
    "separator": "\t",
    "quote_char": None,
    "infer_schema": False,
    "truncate_ragged_lines": True,
    "ignore_errors": True,
}

# Mirrors _CSV_READER_OPTIONS in ol_dlt.sources.edxorg_s3, which is what
# ultimately consumes the file this asset writes.
_DOWNSTREAM_READER_OPTIONS = {
    "delimiter": "\t",
    "ignore_errors": True,
    "all_varchar": True,
    "strict_mode": False,
    "quotechar": '"',
    "escapechar": '"',
}

# A raw edX dump whose `bio` holds a bare CR, in an otherwise LF-terminated file.
_SOURCE_TSV = (
    b"id\tuser_id\tbio\tgoals\n"
    b"1\t10\tI want to learn\rand grow\tlearn\n"
    b"2\t20\tplain bio\tgrow\n"
)

_EXPECTED = [
    ("1", "10", "I want to learn\rand grow", "learn"),
    ("2", "20", "plain bio", "grow"),
]


def _rewrite_frame(frame: pl.DataFrame, quote_style: str) -> bytes:
    """Serialize a frame the way the archive asset's sink_csv call does."""
    sink = io.BytesIO()
    frame.write_csv(sink, separator="\t", include_header=True, quote_style=quote_style)
    return sink.getvalue()


def _rewrite(quote_style: str) -> bytes:
    """Read a raw dump and re-emit it the way the archive asset does."""
    frame = pl.scan_csv(io.BytesIO(_SOURCE_TSV), **_SCAN_OPTIONS).collect()
    return _rewrite_frame(frame, quote_style)


def test_polars_reads_the_carriage_return_row_intact() -> None:
    """The CR survives parsing -- nothing is lost on the way in."""
    frame = pl.scan_csv(io.BytesIO(_SOURCE_TSV), **_SCAN_OPTIONS).collect()
    assert frame.rows() == _EXPECTED


def test_rewritten_tsv_round_trips_through_the_downstream_reader() -> None:
    """The emitted file must give the dlt reader back every row, CR included."""
    relation = duckdb.from_csv_auto(
        io.BytesIO(_rewrite("necessary")), **_DOWNSTREAM_READER_OPTIONS
    )
    assert relation.columns == ["id", "user_id", "bio", "goals"]
    assert relation.fetchall() == _EXPECTED


def test_quote_style_never_would_drop_the_row() -> None:
    """Guard the fix: prove the previous quote_style is what lost the data.

    Without this, ``test_rewritten_tsv_round_trips...`` would still pass if
    someone reverted the writer *and* the fixture stopped containing a CR.
    """
    rows = duckdb.from_csv_auto(
        io.BytesIO(_rewrite("never")), **_DOWNSTREAM_READER_OPTIONS
    ).fetchall()
    assert rows == [("2", "20", "plain bio", "grow")]


def test_quoting_survives_duckdb_strict_mode() -> None:
    """A properly quoted file needs no strict-mode relief to parse.

    ``strict_mode=False`` in the reader is the safety net; correct quoting here
    is the actual fix.
    """
    strict = {**_DOWNSTREAM_READER_OPTIONS, "strict_mode": True}
    assert (
        duckdb.from_csv_auto(io.BytesIO(_rewrite("necessary")), **strict).fetchall()
        == _EXPECTED
    )

    with pytest.raises(duckdb.InvalidInputException, match="sniffing"):
        duckdb.from_csv_auto(io.BytesIO(_rewrite("never")), **strict)


@pytest.mark.parametrize(
    ("column", "value"),
    [
        ("bio", "tab\there"),
        ("bio", "newline\nhere"),
        ("bio", "carriage\rreturn"),
        # RFC-4180 doubling: polars writes this as `"quote "" here"`, and the
        # reader only undoubles it because escapechar is pinned to the quote
        # character. Without that pin it reads back as `quote "" here`.
        ("goals", 'quote " here'),
        ("goals", 'both "quotes" and\ttab'),
        ("bio", r"back\slash"),
    ],
)
def test_other_separator_characters_also_round_trip(column: str, value: str) -> None:
    """A CR is not the only character that needs quoting to survive."""
    frame = pl.DataFrame({"id": ["1"], "bio": ["plain"], "goals": ["plain"]})
    frame = frame.with_columns(pl.lit(value).alias(column))
    sink = io.BytesIO()
    frame.write_csv(sink, separator="\t", include_header=True, quote_style="necessary")

    relation = duckdb.from_csv_auto(
        io.BytesIO(sink.getvalue()), **_DOWNSTREAM_READER_OPTIONS
    )
    assert relation.fetchall() == [tuple(frame.rows()[0])]


def test_unquoted_tab_corrupts_field_alignment() -> None:
    """quote_style="never" did worse than drop rows -- it shifted fields.

    A raw tab in `bio` was indistinguishable from a column separator, so the
    row parsed with `goals` holding what came after the tab and the real
    `goals` value falling off the end. Silent corruption, not a skipped row.
    """
    frame = pl.DataFrame({"id": ["1"], "bio": ["tab\there"], "goals": ["real goal"]})
    rows = duckdb.from_csv_auto(
        io.BytesIO(_rewrite_frame(frame, "never")), **_DOWNSTREAM_READER_OPTIONS
    ).fetchall()
    assert rows == [("1", "tab", "here")]  # "real goal" is gone

    assert duckdb.from_csv_auto(
        io.BytesIO(_rewrite_frame(frame, "necessary")), **_DOWNSTREAM_READER_OPTIONS
    ).fetchall() == [("1", "tab\there", "real goal")]


def test_quoting_does_not_alter_clean_values() -> None:
    """Rows needing no quoting are emitted byte-identically either way."""
    clean = b"id\tbio\n1\tplain bio\n2\tanother\n"
    frame = pl.scan_csv(io.BytesIO(clean), **_SCAN_OPTIONS).collect()

    outputs = {}
    for style in ("never", "necessary"):
        sink = io.BytesIO()
        frame.write_csv(sink, separator="\t", include_header=True, quote_style=style)
        outputs[style] = sink.getvalue()

    assert outputs["never"] == outputs["necessary"] == clean
