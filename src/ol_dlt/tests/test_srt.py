"""ol_file_types.srt: Learn's SRT parsing, keeping the numbers in captions."""

import re

import pytest

from ol_file_types.srt import text_from_srt


def learn_text_from_srt(content: str) -> str:
    """mit-learn's text_from_srt_content, as of mit-learn main 5b8efb234."""
    content = re.sub(
        r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}(\n|$)", "", content
    )
    content = re.sub(r"\d+\n", "", content)
    return re.sub(r"\n\s*\n", " ", content)


@pytest.mark.parametrize(
    "srt",
    [
        "1\n00:00:00,000 --> 00:00:02,000\nFirst line\nsecond line\n\n"
        "2\n00:00:02,000 --> 00:00:04,000\n\n\n"
        "10\n00:00:04,000 --> 00:00:06,000\nLast.",
        # no blank lines between cues
        "1\n00:00:00,000 --> 00:00:02,000\nOne.\n"
        "2\n00:00:02,000 --> 00:00:04,000\nTwo.",
        # cue settings after the timing, which Learn's timestamp rule misses
        "1\n00:00:00,000 --> 00:00:02,000 X1:100 X2:600\nHello\n\n"
        "2\n00:00:02,000 --> 00:00:04,000 align:start\nBye",
        # timings Learn doesn't recognize
        "1\n00:00:00.000 --> 00:00:02.000\nHello\n\n2\n0:00:02,000-->0:00:04,000\nBye",
        # sequence numbers that aren't directly above their timing
        "1\n\n00:00:00,000 --> 00:00:02,000\nHello\n\n"
        " 2\n00:00:02,000 --> 00:00:04,000\nBye",
        # CRLF, which Learn's rules don't handle either
        "1\r\n00:00:00,000 --> 00:00:02,000\r\nHello\r\n\r\n2\r\n"
        "00:00:02,000 --> 00:00:04,000\r\nBye",
        "1\n00:00:00,000 --> 00:00:02,000\n\n2\n00:00:02,000 --> 00:00:04,000\n",
        "",
    ],
    ids=[
        "multi-line-and-empty-cues",
        "no-blank-lines",
        "cue-settings",
        "unrecognized-timings",
        "offset-sequence-numbers",
        "crlf",
        "only-empty-cues",
        "empty",
    ],
)
def test_matches_learn_when_no_caption_ends_in_a_number(srt: str) -> None:
    """Learn checksums this text, so it only differs where Learn lost digits."""
    assert text_from_srt(srt) == learn_text_from_srt(srt)


@pytest.mark.parametrize(
    ("srt", "expected"),
    [
        (
            "1\n00:00:00,000 --> 00:00:02,000\n1984\nThe year it began.\n\n"
            "2\n00:00:02,000 --> 00:00:04,000\nThe answer is 42\nor 43.\n\n"
            "3\n00:00:04,000 --> 00:00:06,000\n7",
            "1984\nThe year it began. The answer is 42\nor 43. 7",
        ),
        (
            "1\n00:00:00,000 --> 00:00:02,000\nabout 20\n\n"
            "2\n00:00:02,000 --> 00:00:04,000\nmore",
            "about 20 more",
        ),
        # no blank lines: a number directly above a timing is a sequence number
        (
            "1\n00:00:00,000 --> 00:00:02,000\nin 1984\n"
            "2\n00:00:02,000 --> 00:00:04,000\nthen",
            "in 1984\nthen",
        ),
    ],
    ids=["digits-only-and-trailing", "before-a-blank-line", "no-blank-lines"],
)
def test_keeps_numbers_that_end_a_caption_line(srt: str, expected: str) -> None:
    assert learn_text_from_srt(srt) != expected
    assert text_from_srt(srt) == expected


@pytest.mark.parametrize(
    ("srt", "expected"),
    [
        (
            "1\n00:00:00,000 --> 00:00:02,000\n\ue000 is 5\n\ue000\ue000ok",
            "\ue000 is 5\n\ue000\ue000ok",
        ),
        # the guard's own character right after a guarded line's newline
        (
            "1\n00:00:00,000 --> 00:00:02,000\nis 5\n\ue000\n\n"
            "2\n00:00:02,000 --> 00:00:04,000\n\ue0007\ue000",
            "is 5\n\ue000 \ue0007\ue000",
        ),
    ],
    ids=["copies-of-the-guard", "copy-after-a-guarded-line"],
)
def test_content_with_the_guard_character_keeps_it(srt: str, expected: str) -> None:
    assert text_from_srt(srt) == expected


def test_content_with_every_private_use_character_is_read() -> None:
    every = "".join(chr(point) for point in range(0xE000, 0xF900))
    srt = f"1\n00:00:00,000 --> 00:00:02,000\n{every} 42\nok"
    assert text_from_srt(srt) == f"{every} 42\nok"


def test_a_long_guarded_digit_run_is_scanned_once() -> None:
    # \d+\n tried at every digit of a guarded run is quadratic: minutes, not ms.
    digits = "7" * 200_000
    srt = f"1\n00:00:00,000 --> 00:00:02,000\n{digits}\nok"
    assert text_from_srt(srt) == f"{digits}\nok"
