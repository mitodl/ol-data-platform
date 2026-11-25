r"""Caption text from an SRT transcript, shared by the Open edX and Canvas sources.

MIT Learn's ``text_from_srt_content`` (learning_resources/etl/utils.py) removes
cue timings, then every ``\d+\n`` to drop sequence numbers, then collapses blank
lines. The unanchored ``\d+\n`` also removes the digits that end a caption line
("1984", "the answer is 42") whenever another line follows, along with that
newline.

This runs Learn's three substitutions unchanged, except that the digits ending
a caption line are kept. Every other part of the output, including what Learn
does with timings it doesn't recognize and numbers that aren't caption text, is
Learn's, so the text, and the checksum Learn computes over it, differs from
Learn's only where Learn dropped caption digits.
"""

import re

_TIMESTAMP = re.compile(r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}(\n|$)")
# Learn's \d+\n, tried only where a digit run starts. re.sub scans left to right,
# so a match starting inside a run would already have matched from the run's
# start: the output is the same, and a guarded run is scanned once rather than
# once per digit.
_SEQUENCE = re.compile(r"(?<!\d)\d+\n")
_BLANK_LINES = re.compile(r"\n\s*\n")
_ENDS_IN_DIGIT = re.compile(r"\d$")
# The guard sits between a caption's last digit and its newline while Learn's
# substitutions run, so \d+\n can't match there. None of the substitutions match
# it. A copy already in the content is doubled first and undoubled after; a
# guard follows a digit and precedes a newline or space, so it is never next to
# a doubled copy, and a lone one is always a guard.
_GUARD = "\ue000"
_GUARDS_AND_COPIES = re.compile(f"{_GUARD}{_GUARD}|{_GUARD}")


def _caption_lines(lines: list[str]) -> set[int]:
    """Return the indexes of the lines that are caption text.

    A caption line comes after a timing line (one containing ``-->``) in the
    same block of lines, where blocks are separated by blank lines, and isn't
    the line directly above a timing line, which is where a sequence number
    sits when a file leaves out the blank lines between cues.
    """
    captions = set()
    after_timing = False
    for index, line in enumerate(lines):
        if not line.strip():
            after_timing = False
        elif "-->" in line:
            after_timing = True
        elif after_timing and not (
            index + 1 < len(lines) and "-->" in lines[index + 1]
        ):
            captions.add(index)
    return captions


def text_from_srt(content: str) -> str:
    """Return the caption text of SRT content, as Learn does but keeping numbers.

    :param content: the SRT file's text, stripped of surrounding whitespace as
        Learn strips Tika's output before parsing it
    :returns: the captions, cues joined by spaces as Learn joins them
    """
    lines = content.replace(_GUARD, _GUARD * 2).split("\n")
    for index in _caption_lines(lines):
        # The last line has no newline after it, so \d+\n can't reach it.
        if index + 1 < len(lines) and _ENDS_IN_DIGIT.search(lines[index]):
            lines[index] += _GUARD
    content = _TIMESTAMP.sub("", "\n".join(lines))
    content = _SEQUENCE.sub("", content)
    return _GUARDS_AND_COPIES.sub(
        lambda match: _GUARD if len(match[0]) == 2 else "",  # noqa: PLR2004
        _BLANK_LINES.sub(" ", content),
    )
