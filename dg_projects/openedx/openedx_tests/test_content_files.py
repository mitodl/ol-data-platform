"""Tests for Open edX document text extraction.

These cover the pure half of assets/content_files.py -- the dispatch decision
and the failure accounting -- without a Dagster context or a live Tika service.
"""

import pytest
from openedx.assets.content_files import (
    MIN_DOCUMENTS_FOR_RATE_CHECK,
    assert_extraction_healthy,
    build_document_rows,
)

SUPPORTED = {"application/pdf", "text/html", "text/plain"}


def fake_is_supported(content_type):
    return content_type in SUPPORTED


def fake_extract(_file_bytes, _content_type):
    return "extracted text"


def rows_for(members, extract=fake_extract):
    return build_document_rows(
        members,
        course_id="course-v1:MITx+6.00.1x+2T2024",
        source_system="mitx",
        extract=extract,
        is_supported=fake_is_supported,
    )


def test_supported_documents_are_extracted():
    rows, counters = rows_for([("static/syllabus.pdf", b"%PDF-fake")])

    assert counters["candidates"] == 1
    assert counters["extracted"] == 1
    assert rows[0]["content"] == "extracted text"
    assert rows[0]["content_type"] == "application/pdf"
    assert rows[0]["extraction_status"] == "extracted"
    assert rows[0]["course_id"] == "course-v1:MITx+6.00.1x+2T2024"
    assert rows[0]["source_system"] == "mitx"


def test_unsupported_files_are_skipped_not_failed():
    """Images and archives are normal course content, not extraction errors."""
    rows, counters = rows_for(
        [("static/logo.png", b"fake png"), ("static/data.zip", b"fake zip")]
    )

    assert rows == []
    assert counters["skipped"] == 2
    assert counters["candidates"] == 0
    assert counters["failed"] == 0


def test_extraction_error_does_not_lose_the_course():
    """One unparseable document must not abort the other documents."""

    def flaky(_file_bytes, content_type):
        if content_type == "application/pdf":
            msg = "Tika exploded"
            raise ValueError(msg)
        return "text from html"

    rows, counters = rows_for(
        [("static/broken.pdf", b"junk"), ("static/page.html", b"<p>hi</p>")],
        extract=flaky,
    )

    assert counters["failed"] == 1
    assert counters["extracted"] == 1
    assert [r["file_path"] for r in rows] == ["static/page.html"]


def test_empty_extraction_is_recorded_not_dropped():
    """A scanned PDF yielding no text is recorded, so the absence is visible.

    Dropping the row would make "Tika found nothing" look identical to "the
    file was never seen".
    """
    rows, counters = rows_for(
        [("static/scanned.pdf", b"%PDF-scan")], extract=lambda _b, _c: ""
    )

    assert counters["empty"] == 1
    assert len(rows) == 1
    assert rows[0]["content"] is None
    assert rows[0]["extraction_status"] == "empty"


def test_content_type_guessed_from_path():
    rows, _ = rows_for([("static/notes.html", b"<p>x</p>")])
    assert rows[0]["content_type"] == "text/html"


def test_healthy_extraction_passes():
    """A healthy batch publishes -- the guard raises, so no exception is the pass."""
    counters = {"candidates": 10, "extracted": 9, "empty": 1, "failed": 0, "skipped": 3}
    assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_mass_extraction_failure_raises():
    """A course whose documents nearly all fail publishes as 'no documents'."""
    counters = {
        "candidates": 20,
        "extracted": 1,
        "empty": 0,
        "failed": 19,
        "skipped": 0,
    }
    with pytest.raises(RuntimeError, match="Refusing to publish"):
        assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_no_documents_is_not_a_failure():
    """Plenty of courses carry no documents at all; that is not an error."""
    counters = {"candidates": 0, "extracted": 0, "empty": 0, "failed": 0, "skipped": 7}
    assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_small_document_sets_skip_the_rate_check():
    """One bad PDF out of one is 0%, which must not trip the guard."""
    counters = {
        "candidates": MIN_DOCUMENTS_FOR_RATE_CHECK - 1,
        "extracted": 0,
        "empty": 0,
        "failed": MIN_DOCUMENTS_FOR_RATE_CHECK - 1,
        "skipped": 0,
    }
    assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_empty_counts_toward_health():
    """Tika answering with no text is a working extractor, not a failure.

    A scanned-PDF-heavy course would otherwise trip the guard despite Tika
    behaving correctly.
    """
    counters = {
        "candidates": 10,
        "extracted": 0,
        "empty": 10,
        "failed": 0,
        "skipped": 0,
    }
    assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")
