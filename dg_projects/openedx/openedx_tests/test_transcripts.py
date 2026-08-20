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
    build_transcript_rows,
    is_transcript,
    text_from_sjson_content,
    text_from_srt_content,
    text_from_vtt_content,
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
        ("static/captions.vtt", True),
        ("static/CAPTIONS.VTT", True),
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


def test_malformed_transcript_counts_as_failed_and_is_not_a_row():
    rows, counters = rows_for([("static/subs_x.srt.sjson", b"{broken")])
    assert counters["failed"] == 1
    assert rows == []


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
    assert [r["file_path"] for r in rows] == ["static/subs_good.srt.sjson"]


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
    assert [r["file_path"] for r in rows] == ["static/subs_good.srt.sjson"]


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


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("static/subs_x.srt.sjson", ".sjson"),
        ("static/captions.srt", ".srt"),
        ("static/captions.vtt", ".vtt"),
    ],
)
def test_transcript_rows_carry_file_extension_like_document_rows(path, expected):
    """A field on one row source and not the other is a schema inconsistency.

    Both feed integrations__learn__content_files, so omitting file_extension
    here made every transcript-derived row read as a null extension.
    """
    rows, _ = rows_for([(path, b'{"text": ["hi"]}')])
    assert rows[0]["file_extension"] == expected


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


# --- WebVTT ----------------------------------------------------------------
#
# The one deliberate divergence from mit-learn, which has no VTT transformer
# and stores Tika's verbatim output instead. These pin what "parsed properly"
# means so the divergence stays intentional rather than drifting.

REALISTIC_VTT = """WEBVTT Kind: captions; Language: en

NOTE This transcript was auto-generated.

intro-cue
00:00:00.500 --> 00:00:03.910 line:90% align:middle
But the problem is that as soon as you
introduce many units,

2
00:00:03.910 --> 00:00:07.030
you introduce many states of the world.

3
00:00:07.030 --> 00:00:11.765
<v Instructor>Take <i>this</i> example.</v>
"""


def test_vtt_keeps_only_the_spoken_text():
    text = text_from_vtt_content(REALISTIC_VTT)

    assert text == (
        "But the problem is that as soon as you introduce many units, "
        "you introduce many states of the world. "
        "Take this example."
    )


@pytest.mark.parametrize(
    "noise",
    ["WEBVTT", "-->", "line:90%", "align:middle", "<v ", "NOTE", "intro-cue"],
)
def test_vtt_structure_never_reaches_the_stored_text(noise):
    """Every one of these survives in what MIT Learn stores for a `.vtt` today."""
    assert noise not in text_from_vtt_content(REALISTIC_VTT)


def test_vtt_cue_identifiers_are_dropped_not_spoken():
    """A cue id sits above the timing line and is metadata, not caption text."""
    vtt = "WEBVTT\n\nslide-14-intro\n00:00:01.000 --> 00:00:02.000\nHello there.\n"
    assert text_from_vtt_content(vtt) == "Hello there."


def test_vtt_style_and_region_blocks_are_excluded():
    """Non-cue blocks carry no `-->`, which is the rule that excludes them."""
    vtt = (
        "WEBVTT\n\n"
        "STYLE\n::cue { color: papayawhip; }\n\n"
        "REGION\nid:fred width:40%\n\n"
        "00:00:01.000 --> 00:00:02.000\nActual caption.\n"
    )
    assert text_from_vtt_content(vtt) == "Actual caption."


def test_vtt_entities_are_unescaped_after_tags_are_stripped():
    """Order matters: an escaped tag in the caption must survive as text."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:02.000\n"
        "Use &lt;b&gt; for bold &amp; &lt;i&gt; for italic.\n"
    )
    assert text_from_vtt_content(vtt) == "Use <b> for bold & <i> for italic."


def test_vtt_inline_timestamps_are_stripped():
    """Karaoke-style mid-cue timestamps are markup, not words."""
    vtt = (
        "WEBVTT\n\n"
        "00:00:01.000 --> 00:00:05.000\n"
        "<00:00:01.000>Now <00:00:02.000>hear <00:00:03.000>this.\n"
    )
    assert text_from_vtt_content(vtt) == "Now hear this."


def test_vtt_with_no_cues_is_empty_not_an_error():
    """A header-only file is an empty caption track, which is a fact."""
    assert text_from_vtt_content("WEBVTT\n\nNOTE nothing here\n") == ""


def test_vtt_handles_crlf_and_a_byte_order_mark():
    """Studio exports are not guaranteed to be LF-only or BOM-free."""
    vtt = "﻿WEBVTT\r\n\r\n00:00:01.000 --> 00:00:02.000\r\nHello there.\r\n"
    assert text_from_vtt_content(vtt) == "Hello there."


def test_vtt_dispatches_through_transcript_text():
    text = transcript_text("static/captions.vtt", REALISTIC_VTT.encode())
    assert text.startswith("But the problem is")


def test_vtt_rows_are_built_like_any_other_transcript():
    rows, counters = rows_for([("static/captions.vtt", REALISTIC_VTT.encode())])

    assert counters["candidates"] == 1
    assert counters["extracted"] == 1
    assert counters["failed"] == 0
    assert rows[0]["file_path"] == "static/captions.vtt"
    assert "-->" not in rows[0]["content"]
