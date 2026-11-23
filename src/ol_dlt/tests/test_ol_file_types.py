"""Tests for ol_file_types, the platform's list of files to read text from."""

import json

import pytest

import ol_file_types
from ol_file_types import (
    TEXT_FILE_EXTENSIONS,
    file_extension,
    is_text_file,
    notebook_text,
)

# Verbatim from mit-learn learning_resources/constants.py. The Learn views narrow
# the platform's list to these, so each has to be inside it or a file Learn
# reads would have no text.
LEARN_VALID_FILE_TYPES = {
    ".csv",
    ".doc",
    ".docx",
    ".htm",
    ".html",
    ".json",
    ".m",
    ".mat",
    ".md",
    ".pdf",
    ".ppt",
    ".pptx",
    ".ps",
    ".py",
    ".r",
    ".rtf",
    ".sjson",
    ".srt",
    ".txt",
    ".vtt",
    ".xls",
    ".xlsx",
    ".xml",
}
LEARN_VALID_TEXT_FILE_TYPES = {
    ".doc",
    ".docx",
    ".htm",
    ".html",
    ".json",
    ".md",
    ".pdf",
    ".tex",
    ".ppt",
    ".pptx",
    ".rtf",
    ".sjson",
    ".srt",
    ".txt",
    ".vtt",
    ".xml",
}


def test_every_extension_learn_reads_is_read() -> None:
    assert LEARN_VALID_FILE_TYPES <= TEXT_FILE_EXTENSIONS
    assert LEARN_VALID_TEXT_FILE_TYPES <= TEXT_FILE_EXTENSIONS


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("courses/a/lab1.IPYNB", True),
        ("static/notes.tex", True),
        ("static/logo.png", False),
        ("static/archive.tar.gz", False),
        ("static/LICENSE", False),
    ],
)
def test_is_text_file_goes_by_the_last_suffix(path: str, *, expected: bool) -> None:
    assert is_text_file(path) is expected


def test_notebook_text_keeps_cell_sources_and_drops_outputs() -> None:
    notebook = {
        "nbformat": 4,
        "cells": [
            {"cell_type": "markdown", "source": ["# Title\n", "Intro text"]},
            {"cell_type": "code", "source": "   ", "outputs": []},
            {
                "cell_type": "code",
                "source": "print(1)",
                "outputs": [{"text": ["1\n"]}],
            },
            "not a cell",
        ],
    }

    assert notebook_text(json.dumps(notebook).encode()) == (
        "# Title\nIntro text\n\nprint(1)"
    )


def test_notebook_text_reads_nbformat_3_worksheets() -> None:
    notebook = {
        "nbformat": 3,
        "worksheets": [
            {
                "cells": [
                    {"cell_type": "markdown", "source": ["Notes"]},
                    {"cell_type": "code", "input": ["x = 1\n", "y = 2"]},
                ]
            }
        ],
    }

    assert notebook_text(json.dumps(notebook).encode()) == "Notes\n\nx = 1\ny = 2"


def test_a_notebook_with_no_text_has_none() -> None:
    assert notebook_text(b'{"cells": [{"source": ""}]}') is None


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"\xff\xfe",
        b"[]",
        b'{"metadata": {}}',
    ],
)
def test_a_file_that_is_not_a_notebook_raises(body: bytes) -> None:
    with pytest.raises(ValueError, match="notebook"):
        notebook_text(body)


def test_json_nested_past_the_stack_is_not_a_notebook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """How deep json.loads gets before RecursionError depends on the stack size."""

    def too_deep(_body: bytes) -> None:
        raise RecursionError

    monkeypatch.setattr(ol_file_types.json, "loads", too_deep)

    with pytest.raises(ValueError, match="notebook") as raised:
        notebook_text(b"[[[]]]")
    assert isinstance(raised.value.__cause__, RecursionError)


def test_extensionless_files_report_an_empty_extension() -> None:
    """A bare `LICENSE` has no suffix; it must not become the whole filename."""
    assert file_extension("course/static/LICENSE") == ""
    assert file_extension("course/static/subs_abc.srt.sjson") == ".sjson"
