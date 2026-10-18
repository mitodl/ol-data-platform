import re
from contextlib import contextmanager
from types import SimpleNamespace

import polars as pl
import pytest
from ml.lib.courseware import answer_check, local
from ml.lib.courseware.answer_check import (
    EXCERPT_HEAD_CHARS,
    EXCERPT_TAIL_CHARS,
    check_sources,
    check_version,
    excerpt,
)

MODEL = "haiku"
KEY_TEXT = "Problem 1. Solution: x = 3. Problem 2. Solution: y = 4."
READING_TEXT = "Linear regression fits a line to points by least squares."


class _Client:
    """Stands in for an Anthropic client: flags every excerpt that says Solution."""

    def __init__(self, *, fail: bool = False, id_offset: int = 0):
        self.calls: list[str] = []
        self.fail = fail
        self.id_offset = id_offset
        self.messages = self

    @contextmanager
    def stream(self, **kwargs):
        listing = kwargs["messages"][0]["content"]
        self.calls.append(listing)
        entries = re.split(r"(?m)^=== (\d+) \|", listing)[1:]
        verdicts = [
            {
                "source_id": int(index) + self.id_offset,
                "is_answer_document": "Solution:" in body,
                "reason": "answers to problems" if "Solution:" in body else "reading",
            }
            for index, body in zip(entries[::2], entries[1::2], strict=True)
        ]
        tool_input = {"verdicts": verdicts}
        if self.fail:
            tool_input = {"verdicts": [{"source_id": "not a number"}]}
        response = SimpleNamespace(
            stop_reason="tool_use",
            content=[
                SimpleNamespace(
                    type="tool_use", name="record_verdicts", input=tool_input
                )
            ],
        )
        yield SimpleNamespace(get_final_message=lambda: response)


def _sources(rows: list[tuple[str, str, str, str]]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "platform": "mitxonline",
                "courserun_readable_id": run,
                "scope_id": "materials",
                "source_ref": ref,
                "source_kind": kind,
                "source_title": ref,
                "source_text": text,
            }
            for run, ref, kind, text in rows
        ]
    )


def test_flags_answer_documents_and_records_version():
    client = _Client()
    result = check_sources(
        _sources(
            [
                ("run1", "static/ps1.pdf", "document", KEY_TEXT),
                ("run1", "static/reading.pdf", "document", READING_TEXT),
                ("run1", "block-v1:html", "html", KEY_TEXT),
            ]
        ),
        client,
        model_id=MODEL,
    )
    assert dict(result.select("source_ref", "is_answer_document").iter_rows()) == {
        "static/ps1.pdf": True,
        "static/reading.pdf": False,
    }
    assert set(result["check_version"]) == {check_version(MODEL, "low")}
    assert len(client.calls) == 1


def test_a_copied_text_is_checked_once():
    client = _Client()
    result = check_sources(
        _sources(
            [
                ("run1", "static/ps1.pdf", "document", KEY_TEXT),
                ("b2b-copy", "static/ps1.pdf", "document", KEY_TEXT),
            ]
        ),
        client,
        model_id=MODEL,
    )
    assert result.height == 2
    assert result["is_answer_document"].all()
    assert client.calls[0].count("=== ") == 1


def test_stored_verdicts_skip_unchanged_sources_and_cover_new_copies():
    stored = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id=MODEL,
    )
    client = _Client()
    result = check_sources(
        _sources(
            [
                ("run1", "static/ps1.pdf", "document", KEY_TEXT),
                ("run2", "static/ps1.pdf", "document", KEY_TEXT),
                ("run1", "static/reading.pdf", "document", READING_TEXT),
            ]
        ),
        client,
        model_id=MODEL,
        existing=stored,
    )
    # run1's ps1 is stored, run2's copy reuses its verdict, only the reading is sent
    assert sorted(result["courserun_readable_id"] + "/" + result["source_ref"]) == [
        "run1/static/reading.pdf",
        "run2/static/ps1.pdf",
    ]
    assert len(client.calls) == 1
    assert "Linear regression" in client.calls[0]
    assert "Problem 1" not in client.calls[0]


def test_a_new_version_checks_again():
    stored = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id=MODEL,
    )
    result = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id="another-model",
        existing=stored,
    )
    assert result.height == 1


def test_changed_text_checks_again():
    stored = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", READING_TEXT)]),
        _Client(),
        model_id=MODEL,
    )
    result = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id=MODEL,
        existing=stored,
    )
    assert result["is_answer_document"].to_list() == [True]


def test_a_failed_batch_is_left_out():
    result = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(fail=True),
        model_id=MODEL,
    )
    assert result.height == 0
    assert result.schema == pl.Schema(answer_check.ANSWER_CHECK_SCHEMA)


def test_excerpt_keeps_the_opening_and_the_end():
    short = "x" * (EXCERPT_HEAD_CHARS + EXCERPT_TAIL_CHARS)
    assert excerpt(short) == short
    long = "a" * EXCERPT_HEAD_CHARS + "m" * 10_000 + "z" * EXCERPT_TAIL_CHARS
    cut = excerpt(long)
    assert cut.startswith("a" * EXCERPT_HEAD_CHARS)
    assert cut.endswith("z" * EXCERPT_TAIL_CHARS)
    assert "m" not in cut


def test_a_failed_batch_is_checked_on_the_next_run():
    sources = _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)])
    failed = check_sources(sources, _Client(fail=True), model_id=MODEL)
    client = _Client()
    result = check_sources(sources, client, model_id=MODEL, existing=failed)
    assert result["is_answer_document"].to_list() == [True]
    assert len(client.calls) == 1


def test_verdicts_counted_from_one_are_rejected():
    result = check_sources(
        _sources(
            [
                ("run1", "static/ps1.pdf", "document", KEY_TEXT),
                ("run1", "static/reading.pdf", "document", READING_TEXT),
            ]
        ),
        _Client(id_offset=1),
        model_id=MODEL,
    )
    assert result.height == 0


def test_a_reused_verdict_is_dated_when_it_is_recorded():
    stored = check_sources(
        _sources([("run1", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id=MODEL,
    )
    result = check_sources(
        _sources([("run2", "static/ps1.pdf", "document", KEY_TEXT)]),
        _Client(),
        model_id=MODEL,
        existing=stored,
    )
    assert result["checked_at"].to_list()[0] > stored["checked_at"].to_list()[0]


def test_answer_check_dry_run_calls_nothing(tmp_path, monkeypatch, capsys):
    path = tmp_path / "sources.parquet"
    _sources(
        [
            ("run1", "static/ps1.pdf", "document", KEY_TEXT),
            ("b2b-copy", "static/ps1.pdf", "document", KEY_TEXT),
            ("run1", "static/reading.pdf", "document", READING_TEXT),
        ]
    ).write_parquet(path)

    def no_client(**_kwargs):
        pytest.fail("a dry run built a model client")

    monkeypatch.setattr(local, "LLMClientFactory", no_client)
    local.answer_check(path, tmp_path / "out", dry_run=True)
    assert "3 sources of kind document need a verdict; 2 distinct texts" in (
        capsys.readouterr().out
    )
    assert not (tmp_path / "out").exists()
