"""Tests for the edX.org program learner report source."""

import csv
import io
from pathlib import Path

import fsspec
import pytest

from ol_dlt import config
from ol_dlt.sources import edxorg_program_reports

_ROW = {
    "AUTHORING INSTITUTION": "MITx",
    "PROGRAM TITLE": "Statistics and Data Science",
    "PROGRAM UUID": "program-a",
    "PROGRAM TYPE": "MicroMasters",
    "USER ID": "70464101",
    "NAME": "",
    "USERNAME": "learner",
    "USER KEY": "null",
    "COURSE TITLE": "Probability",
    "COURSE RUN KEY": "course-v1:MITx+6.431x+3T2026",
    "COURSE RUN START DATE": "2026-09-02 15:00:00 Z",
    "EXTERNAL COURSE KEY": "null",
    "USER ROLES": "null",
    "TRACK": "verified",
    "PURCHASED AS BUNDLE": "false",
    "GRADE": "0.04",
    "LETTER GRADE": "",
    "DATE FIRST ENROLLED": "2026-08-16 00:05:29 Z",
    "DATE LAST UNENROLLED": "null",
    "CURRENTLY ENROLLED": "true",
    "DATE FIRST UPGRADED TO VERIFIED": "null",
    "COMPLETED PROGRAM": "False",
    "DATE PROGRAM CERTIFICATE AWARDED": "null",
    "COMPLETED": "false",
    "DATE COMPLETED": "null",
    "LAST ACTIVITY DATE": "2026-10-01",
}


def _report(rows: list[dict[str, str]], header: tuple[str, ...] | None = None) -> bytes:
    """Write rows the way edX does: every field quoted."""
    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=header or edxorg_program_reports.REPORT_COLUMNS,
        quoting=csv.QUOTE_ALL,
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode()


def _read(report: bytes) -> list[dict[str, str]]:
    return [
        row
        for batch in edxorg_program_reports.read_report(io.BytesIO(report), "test.csv")
        for row in batch.to_pylist()
    ]


def test_report_keeps_every_value_as_the_text_it_holds() -> None:
    """Staging tests for 'null', and Airbyte landed '' as ''."""
    (row,) = _read(_report([_ROW]))

    assert list(row) == list(edxorg_program_reports.RAW_COLUMNS)
    assert row["user_id"] == "70464101"
    assert row["grade"] == "0.04"
    assert row["name"] == ""
    assert row["letter_grade"] == ""
    assert row["date_completed"] == "null"
    assert row["completed_program"] == "False"


def test_a_newline_inside_a_quoted_value_stays_in_its_row() -> None:
    rows = _read(_report([{**_ROW, "NAME": "First\nLast"}, _ROW]))

    assert [row["name"] for row in rows] == ["First\nLast", ""]


def test_a_changed_header_fails() -> None:
    renamed = tuple(
        "LEARNER ID" if column == "USER ID" else column
        for column in edxorg_program_reports.REPORT_COLUMNS
    )
    row = {
        ("LEARNER ID" if column == "USER ID" else column): value
        for column, value in _ROW.items()
    }
    report = _report([row], header=renamed)
    with pytest.raises(
        edxorg_program_reports.LearnerReportHeaderError, match="LEARNER ID"
    ):
        _read(report)


def _land(root: Path, relative: str, content: bytes) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


@pytest.mark.integration
def test_learner_reports_load_and_cohort_reports_do_not(
    test_profile: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    landing = tmp_path / "landing"
    prefix = "edxorg-program-credentials/reports_v2/MITx"
    _land(
        landing,
        f"{prefix}/program-a/learner_report__2026-10-22.csv",
        _report([_ROW, {**_ROW, "USER ID": "10442141"}]),
    )
    _land(
        landing,
        f"{prefix}/program-b/learner_report__2026-10-22.csv",
        _report([{**_ROW, "PROGRAM UUID": "program-b"}]),
    )
    # Beside every learner report, with other columns, and not loaded.
    _land(
        landing,
        f"{prefix}/program-a/program_cohort_report__2026-10-22.csv",
        b'"PROGRAM UUID","COHORT"\n"program-a","2026"\n',
    )
    monkeypatch.setattr(
        edxorg_program_reports.s3fs, "S3FileSystem", lambda: fsspec.filesystem("file")
    )

    pipeline = edxorg_program_reports.edxorg_program_reports_pipeline()
    info = pipeline.run(
        edxorg_program_reports.edxorg_program_reports_source(
            bucket_url=landing.as_uri()
        )
    )
    assert not info.has_failed_jobs

    table = pipeline.dataset()[edxorg_program_reports.RAW_TABLE].arrow()
    assert sorted(table.column("user_id").to_pylist()) == [
        "10442141",
        "70464101",
        "70464101",
    ]
    assert {
        "_source_file",
        "_file_modified_at",
        *config.DLT_LOAD_ID_COLUMN,
        *edxorg_program_reports.RAW_COLUMNS,
    } <= set(table.column_names)
    assert set(table.column("date_completed").to_pylist()) == {"null"}

    # A second run finds nothing past the cursor and appends nothing.
    pipeline.run(
        edxorg_program_reports.edxorg_program_reports_source(
            bucket_url=landing.as_uri()
        )
    )
    assert pipeline.dataset()[edxorg_program_reports.RAW_TABLE].arrow().num_rows == 3
