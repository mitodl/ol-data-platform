"""Tests for Open edX document text extraction.

These cover the pure half of assets/content_files.py -- the dispatch decision
and the failure accounting -- without a Dagster context or a live Tika service.
"""

import httpx2 as httpx
import pytest
from openedx.assets.content_files import (
    MIN_DOCUMENTS_FOR_RATE_CHECK,
    TikaUnavailableError,
    assert_extraction_healthy,
    build_document_rows,
    output_digest,
)

SUPPORTED = {"application/pdf", "text/html", "text/plain"}


def fake_is_supported(content_type):
    return content_type in SUPPORTED


def fake_extract(_file_bytes, _content_type):
    return "extracted text"


def rows_for(members, extract=fake_extract):
    """Run build_document_rows over (path, bytes) pairs.

    The real caller hands over a reader per member so unwanted files are never
    read; tests state the bytes directly and this wraps them.
    """
    lazy = [(path, (lambda data=data: data)) for path, data in members]
    return build_document_rows(
        lazy,
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


def test_subtitles_are_not_treated_as_documents():
    """Subtitles belong to the transcript asset, not Tika.

    This is not hypothetical. `mimetypes` maps `.srt` to `text/plain`, which is
    in Tika's supported set, and Tika returns the file verbatim -- timestamps,
    sequence numbers and all. Without this exclusion every `.srt` in a course
    would be extracted twice: once as unusable noise here, and once properly by
    the transcript parser. Filenames below are taken from a real
    course-v1:MITx+14.310x+3T2021 export, which carries 302 `.srt` and 720
    `.srt.sjson` files.
    """
    rows, counters = rows_for(
        [
            ("course/static/subs_-GQxFdhr_qU.srt.sjson", b'{"text": ["hi"]}'),
            ("course/static/00508b03-8131-41de-b383-6732c625a82a-en.srt", b"1\n"),
            ("course/static/14.310_Lecture_15_Segment_03-_Part1.srt", b"1\n"),
        ]
    )

    assert rows == []
    assert counters["transcripts"] == 3
    assert counters["candidates"] == 0


def test_real_archive_documents_are_still_extracted():
    """The exclusion must not swallow the documents that share that directory.

    Filenames taken from the same real export -- note the dots in the names,
    which is why suffix detection has to be last-suffix rather than split-on-dot.
    """
    rows, counters = rows_for(
        [
            ("course/static/14.310x Syllabus.pdf", b"%PDF-fake"),
            ("course/static/14.310x_3T2018.pdf", b"%PDF-fake"),
        ]
    )

    assert counters["candidates"] == 2
    assert {r["content_type"] for r in rows} == {"application/pdf"}


def test_empty_files_are_empty_not_failed():
    """A zero-byte file has no text; that is a fact, not an extraction failure.

    Tika answers 422 for an empty body. Counting that as a failure would both
    waste a round trip and pollute the denominator the health guard divides by.
    A real course-v1:MITx+14.310x+3T2021 export ships nine empty about/
    placeholders like these, which was 15% of a 60-file sample -- enough to drag
    a sparse course toward the guard's floor for no reason.
    """

    def must_not_be_called(_file_bytes, _content_type):
        msg = "Tika must not be called for a zero-byte file"
        raise AssertionError(msg)

    rows, counters = rows_for(
        [
            ("about/short_description.html", b""),
            ("about/entrance_exam_enabled.html", b""),
        ],
        extract=must_not_be_called,
    )

    assert counters["failed"] == 0
    assert counters["empty"] == 2
    assert counters["candidates"] == 2
    assert [r["extraction_status"] for r in rows] == ["empty", "empty"]
    assert all(r["content"] is None for r in rows)


def test_empty_files_do_not_drag_the_health_guard():
    """Empty placeholders count as success, so they cannot trip the floor."""
    counters = {
        "candidates": 20,
        "extracted": 11,
        "empty": 9,
        "failed": 0,
        "skipped": 0,
        "transcripts": 0,
    }
    assert_extraction_healthy(counters, "course-v1:MITx+14.310x+3T2021")


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
    assert [r["file_path"] for r in rows] == ["static/broken.pdf", "static/page.html"]


def test_failed_extraction_is_recorded_not_dropped():
    """A failed document gets a row, or downstream reads it as a deleted file.

    MIT Learn unpublishes content files that disappear from a run's output. Its
    own extractor gets away with recording nothing for a failure because it
    passes the keys out through a `failed_keys` side channel that exempts them
    from the stale pass; a JSONL snapshot has no side channel, so a transient
    Tika failure would silently unpublish the last good text.
    """

    def always_fails(_file_bytes, _content_type):
        msg = "Tika exploded"
        raise ValueError(msg)

    rows, _ = rows_for(
        [("static/broken.pdf", b"junk"), ("static/page.html", b"<p>hi</p>")],
        extract=always_fails,
    )

    assert [r["extraction_status"] for r in rows] == ["failed", "failed"]
    assert all(r["content"] is None for r in rows)
    assert [r["size_bytes"] for r in rows] == [4, 9]


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
    """A bad PDF in a small course is below the floor, so the rate is ignored.

    The exemption is about the *rate* being meaningless on small samples, not
    about small courses being unguarded: one success is enough to show the
    extractor works. A small course where nothing succeeded is covered by
    test_total_failure_raises_below_the_rate_check_floor instead.
    """
    counters = {
        "candidates": MIN_DOCUMENTS_FOR_RATE_CHECK - 1,
        "extracted": 1,
        "empty": 0,
        "failed": MIN_DOCUMENTS_FOR_RATE_CHECK - 2,
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


# --- a broken service must not look like a course with nothing to read ------


def _http_error(status):
    request = httpx.Request("PUT", "https://tika.example/tika")
    response = httpx.Response(status, request=request)
    message = f"HTTP {status}"
    return httpx.HTTPStatusError(message, request=request, response=response)


@pytest.mark.parametrize("status", [401, 403, 500, 502, 503])
def test_service_level_failures_abort_instead_of_counting_as_documents(status):
    """A dead or unauthorised Tika is not N unparseable documents.

    The concrete path: a Vault read failure leaves an empty access token, every
    call 401s, and a course with fewer than MIN_DOCUMENTS_FOR_RATE_CHECK
    candidates skipped the health guard entirely and published an empty JSONL
    as a success.
    """

    def failing_extract(_file_bytes, _content_type):
        raise _http_error(status)

    with pytest.raises(TikaUnavailableError):
        rows_for([("static/syllabus.pdf", b"%PDF-fake")], extract=failing_extract)


def test_transport_errors_abort_but_timeouts_stay_per_document():
    """A connection that never landed is a service fact; a slow file is not.

    One oversized PDF times out on a perfectly healthy Tika, so a timeout is
    still counted per document -- a genuinely dead service produces enough of
    them to trip the all-candidates-failed guard.
    """

    def unreachable(_file_bytes, _content_type):
        message = "no route to host"
        raise httpx.ConnectError(message)

    with pytest.raises(TikaUnavailableError):
        rows_for([("static/a.pdf", b"%PDF-fake")], extract=unreachable)

    def slow(_file_bytes, _content_type):
        message = "too slow"
        raise httpx.ReadTimeout(message)

    rows, counters = rows_for(
        [("static/a.pdf", b"%PDF-fake"), ("static/b.pdf", b"%PDF-fake")], extract=slow
    )
    assert counters["failed"] == 2
    assert [r["extraction_status"] for r in rows] == ["failed", "failed"]


def test_document_level_failures_still_count_as_failures():
    """A 422 really is about this file, so it must not abort the course."""

    def unprocessable(_file_bytes, _content_type):
        raise _http_error(422)

    _, counters = rows_for([("static/a.pdf", b"%PDF-fake")], extract=unprocessable)
    assert counters["failed"] == 1


def test_total_failure_raises_below_the_rate_check_floor():
    """The small-course exemption must not exempt a course that wholly failed."""
    counters = {
        "candidates": 2,
        "extracted": 0,
        "empty": 0,
        "failed": 2,
        "skipped": 0,
    }
    with pytest.raises(RuntimeError, match="all 2 candidate documents failed"):
        assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_no_candidates_is_still_not_a_failure():
    """Zero documents and zero successes is a quiet course, not a broken one."""
    counters = {
        "candidates": 0,
        "extracted": 0,
        "empty": 0,
        "failed": 0,
        "skipped": 7,
    }
    assert_extraction_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


# --- laziness ---------------------------------------------------------------


def test_filtered_out_members_are_never_read():
    """Skipped files must not be pulled into memory just to be discarded.

    A real export is mostly images and archives; reading them to skip them is
    what put the whole 152 MiB archive in the worker's heap at once.
    """
    reads = []

    def _reader(path, data):
        def _read():
            reads.append(path)
            return data

        return _read

    members = [
        ("static/logo.png", _reader("static/logo.png", b"fake png")),
        ("static/data.zip", _reader("static/data.zip", b"fake zip")),
        ("static/subs_a.srt.sjson", _reader("static/subs_a.srt.sjson", b"{}")),
        ("static/syllabus.pdf", _reader("static/syllabus.pdf", b"%PDF-fake")),
    ]

    build_document_rows(
        members,
        course_id="course-v1:MITx+6.00.1x+2T2024",
        source_system="mitx",
        extract=fake_extract,
        is_supported=fake_is_supported,
    )

    assert reads == ["static/syllabus.pdf"]


# --- data_version ----------------------------------------------------------


def test_output_digest_changes_when_the_extracted_text_changes(tmp_path):
    """The version must track the output, not the input bundle.

    Versioning on the bundle hash meant a Tika upgrade, a parser change, or a
    partial-failure run later succeeding all produced different text under the
    same DataVersion and the same S3 key: the corrected output overwrote the
    old one and no downstream data_version_changed() check fired.
    """
    partial = tmp_path / "partial.jsonl"
    partial.write_text('{"content": null}\n')
    complete = tmp_path / "complete.jsonl"
    complete.write_text('{"content": "the real text"}\n')

    assert output_digest(partial) != output_digest(complete)


def test_output_digest_is_stable_for_identical_output(tmp_path):
    """Identical extraction must hash identically, or nothing ever caches."""
    one = tmp_path / "one.jsonl"
    one.write_text('{"content": "same"}\n')
    two = tmp_path / "two.jsonl"
    two.write_text('{"content": "same"}\n')

    assert output_digest(one) == output_digest(two)
