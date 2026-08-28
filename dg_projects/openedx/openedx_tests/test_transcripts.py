"""Tests for Open edX video transcript extraction.

The parity block below is ported VERBATIM from mit-learn's
learning_resources/etl/utils_test.py (test_text_from_srt_content and
test_text_from_sjson_content). It is the contract: the webhook path and the
Celery ETL must agree on how a transcript parses, or a cutover diff shows a
difference that is ours rather than the data's. Do not edit a case here to make
this implementation pass -- fix the implementation, or change both repos
together.
"""

import pytest
from openedx.assets.transcripts import (
    assert_parsing_healthy,
    build_transcript_rows,
    is_transcript,
    text_from_sjson_content,
    text_from_srt_content,
    transcript_text,
)

# --- parity with mit-learn -------------------------------------------------

MIT_LEARN_SRT_CONTENT = (
    "1\n"
    "00:00:00,000 --> 00:00:02,000\n"
    "This is the first subtitle."
    "\n"
    "2\n"
    "00:00:02,000 --> 00:00:04,000\n"
    "This is the second subtitle."
)
MIT_LEARN_SRT_EXPECTED = "This is the first subtitle.\nThis is the second subtitle."

MIT_LEARN_SJSON_CONTENT = """
    {
        "start": [0,2],
        "end": [2,4],
        "text": ["This is the first subtitle.",
             "This is the second subtitle."
        ]
    }
"""
MIT_LEARN_SJSON_EXPECTED = "This is the first subtitle. This is the second subtitle."


def test_srt_parses_exactly_as_mit_learn_does():
    assert text_from_srt_content(MIT_LEARN_SRT_CONTENT) == MIT_LEARN_SRT_EXPECTED


def test_sjson_parses_exactly_as_mit_learn_does():
    assert text_from_sjson_content(MIT_LEARN_SJSON_CONTENT) == MIT_LEARN_SJSON_EXPECTED


# --- behaviour -------------------------------------------------------------


def test_sjson_parse_error_returns_none_not_raises():
    """One malformed transcript must not abort the course."""
    assert text_from_sjson_content("{not valid json") is None


def test_sjson_without_text_key_is_empty():
    assert text_from_sjson_content('{"start": [0], "end": [2]}') == ""


def test_srt_digit_only_caption_is_stripped():
    """A caption line of only digits is removed -- mit-learn does this too.

    The sequence-number rule is a bare ``\\d+\\n``, so it cannot tell a caption
    reading "1984" from a subtitle index. Pinned deliberately: diverging would
    make every such caption a spurious cutover difference.
    """
    srt = "1\n00:00:00,000 --> 00:00:02,000\n1984\nThe year it began."
    assert "1984" not in text_from_srt_content(srt)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("static/subs_abc123.srt.sjson", True),
        ("static/captions.srt", True),
        ("static/CAPTIONS.SRT", True),
        ("static/syllabus.pdf", False),
        ("static/logo.png", False),
        ("html/content.html", False),
    ],
)
def test_is_transcript(path, expected):
    assert is_transcript(path) is expected


def test_sjson_dispatches_on_final_suffix():
    """subs_*.srt.sjson is SJSON, not SRT -- the last suffix wins."""
    text = transcript_text("static/subs_x.srt.sjson", b'{"text": ["hello", "world"]}')
    assert text == "hello world"


def test_non_utf8_transcript_returns_none():
    assert transcript_text("static/broken.srt", b"\xff\xfe\x00bad") is None


def rows_for(members):
    """Run build_transcript_rows over (path, bytes) pairs.

    The real caller hands over a reader per member so non-transcripts are never
    read; tests state the bytes directly and this wraps them.
    """
    lazy = [(path, (lambda data=data: data)) for path, data in members]
    return build_transcript_rows(
        lazy, course_id="course-v1:MITx+6.00.1x+2T2024", source_system="mitx"
    )


def test_non_transcripts_are_not_candidates():
    rows, counters = rows_for(
        [("static/syllabus.pdf", b"%PDF"), ("static/logo.png", b"png")]
    )
    assert rows == []
    assert counters["candidates"] == 0


def test_transcript_row_shape():
    rows, counters = rows_for(
        [("static/subs_x.srt.sjson", b'{"text": ["hello there"]}')]
    )

    assert counters["extracted"] == 1
    assert rows[0]["content"] == "hello there"
    assert rows[0]["file_path"] == "static/subs_x.srt.sjson"
    assert rows[0]["content_type"] == "application/json"
    assert rows[0]["extraction_status"] == "extracted"
    assert rows[0]["course_id"] == "course-v1:MITx+6.00.1x+2T2024"
    assert rows[0]["source_system"] == "mitx"


def test_malformed_transcript_is_recorded_as_failed():
    """A failed parse gets a row, or downstream reads it as a deleted file.

    MIT Learn unpublishes content files that disappear from a run's output, so
    an unparseable transcript that leaves no row would drop the last good text
    for that file.
    """
    rows, counters = rows_for([("static/subs_x.srt.sjson", b"{broken")])
    assert counters["failed"] == 1
    assert rows[0]["extraction_status"] == "failed"
    assert rows[0]["content"] is None
    assert rows[0]["content_type"] == "application/json"


def test_empty_transcript_is_recorded_not_dropped():
    """An empty caption track and a file never seen are different facts."""
    rows, counters = rows_for([("static/subs_x.srt.sjson", b'{"text": []}')])

    assert counters["empty"] == 1
    assert len(rows) == 1
    assert rows[0]["content"] is None
    assert rows[0]["extraction_status"] == "empty"


def test_one_bad_transcript_does_not_lose_the_others():
    rows, counters = rows_for(
        [
            ("static/subs_bad.srt.sjson", b"{broken"),
            ("static/subs_good.srt.sjson", b'{"text": ["fine"]}'),
        ]
    )

    assert counters["failed"] == 1
    assert counters["extracted"] == 1
    assert [r["file_path"] for r in rows] == [
        "static/subs_bad.srt.sjson",
        "static/subs_good.srt.sjson",
    ]


def test_plain_srt_content_type():
    rows, _ = rows_for([("static/captions.srt", MIT_LEARN_SRT_CONTENT.encode())])
    assert rows[0]["content_type"] == "text/plain"


# --- valid JSON that is not a caption track --------------------------------


@pytest.mark.parametrize(
    "content",
    [
        b"null",
        b"[]",
        b'["a", "b"]',
        b'"just a string"',
        b"42",
        b'{"text": null}',
        b'{"text": "not a list"}',
        b'{"text": {"0": "a"}}',
    ],
)
def test_structurally_wrong_sjson_fails_the_file_not_the_course(content):
    """These parse as JSON and then blow up on the caption access.

    `json.loads` accepts every one, so the JSONDecodeError guard does not
    catch them; the AttributeError or TypeError that followed escaped
    build_transcript_rows and aborted the whole course, contradicting the
    per-file failure accounting the counters promise.
    """
    rows, counters = rows_for([("static/subs_x.srt.sjson", content)])

    assert counters["candidates"] == 1
    assert counters["failed"] + counters["empty"] == 1
    assert all(row["extraction_status"] != "extracted" for row in rows)


def test_sjson_ignores_non_string_captions_rather_than_raising():
    """A stray null inside an otherwise good caption list is not fatal."""
    text = text_from_sjson_content('{"text": ["good", null, 7, "also good"]}')
    assert text == "good also good"


def test_one_structurally_wrong_sjson_does_not_lose_the_others():
    rows, counters = rows_for(
        [
            ("static/subs_bad.srt.sjson", b"null"),
            ("static/subs_good.srt.sjson", b'{"text": ["fine"]}'),
        ]
    )

    assert counters["extracted"] == 1
    assert [r["extraction_status"] for r in rows] == ["failed", "extracted"]


# --- total failure ---------------------------------------------------------


def test_all_transcripts_failing_refuses_to_publish():
    """A course whose every transcript failed must not publish as "none"."""
    _, counters = rows_for(
        [
            ("static/subs_a.srt.sjson", b"{broken"),
            ("static/subs_b.srt.sjson", b"{also broken"),
        ]
    )

    with pytest.raises(RuntimeError, match="all 2 transcripts failed to parse"):
        assert_parsing_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_a_course_with_no_transcripts_publishes():
    """Zero candidates is normal -- plenty of courses have no videos."""
    _, counters = rows_for([("static/syllabus.pdf", b"%PDF")])
    assert_parsing_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_a_partial_failure_still_publishes():
    """Parsing is local, so a bad file means a bad file, not a sick service."""
    _, counters = rows_for(
        [
            ("static/subs_bad.srt.sjson", b"{broken"),
            ("static/subs_good.srt.sjson", b'{"text": ["fine"]}'),
        ]
    )
    assert_parsing_healthy(counters, "course-v1:MITx+6.00.1x+2T2024")


def test_a_bom_prefixed_transcript_still_parses():
    """Learn hands these to Tika, which sniffs the charset before parsing.

    None of the ~20k srt/sjson files sampled across 49 production exports
    carried a BOM, so this is insurance against a file that would extract in
    Learn today and fail here.
    """
    text = transcript_text(
        "static/subs_x.srt.sjson", b'\xef\xbb\xbf{"text": ["hello"]}'
    )
    assert text == "hello"


# --- status and content must agree -----------------------------------------


def test_whitespace_only_transcript_is_empty_with_null_content():
    """Status is decided on the stripped text, so content must be too.

    Keying content off `text or None` stored a whitespace-only string under an
    "empty" status, which the document rows never do -- and the two are unioned
    downstream.
    """
    rows, counters = rows_for(
        [("static/subs_x.srt.sjson", b'{"text": ["   ", "\\n"]}')]
    )

    assert counters["empty"] == 1
    assert rows[0]["extraction_status"] == "empty"
    assert rows[0]["content"] is None


def test_status_vocabulary_matches_the_document_asset():
    """Both row sources feed one model, so "parsed" vs "extracted" is a schema
    difference dressed up as a naming choice.
    """
    rows, _ = rows_for([("static/subs_x.srt.sjson", b'{"text": ["hi"]}')])
    assert rows[0]["extraction_status"] == "extracted"


def test_non_transcript_members_are_never_read():
    """An export is mostly non-transcripts; reading them to skip them is waste."""
    reads = []

    def _reader(path, data):
        def _read():
            reads.append(path)
            return data

        return _read

    build_transcript_rows(
        [
            ("static/logo.png", _reader("static/logo.png", b"png")),
            ("static/syllabus.pdf", _reader("static/syllabus.pdf", b"%PDF")),
            (
                "static/subs_x.srt.sjson",
                _reader("static/subs_x.srt.sjson", b'{"text":[]}'),
            ),
        ],
        course_id="course-v1:MITx+6.00.1x+2T2024",
        source_system="mitx",
    )

    assert reads == ["static/subs_x.srt.sjson"]


# --- SRT blank-line handling, which the parity table does not reach ---------


def test_srt_blank_lines_between_blocks_collapse_to_a_space():
    """A standard SRT separates blocks with a blank line.

    The mit-learn parity fixture happens not to contain one, leaving
    _SRT_BLANK_LINES unexercised -- so the regex most likely to diverge from
    mit-learn was the one nothing covered.
    """
    srt = (
        "1\n"
        "00:00:00,000 --> 00:00:02,000\n"
        "First caption.\n"
        "\n"
        "2\n"
        "00:00:02,000 --> 00:00:04,000\n"
        "Second caption.\n"
    )
    text = text_from_srt_content(srt)

    assert "-->" not in text
    assert "First caption." in text
    assert "Second caption." in text
    assert "\n\n" not in text
