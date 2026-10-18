"""Content-addressed courseware extraction: copies of a text share one call."""

import polars as pl
import pytest
from ml.lib.courseware import extract, local, scopes
from ml.lib.courseware.extract import (
    REUSED_STOP_REASON,
    _ModelResult,
    content_key,
    extract_scopes,
    extraction_key,
    stored_content_key,
    stored_extraction_key,
)
from ml.lib.courseware.schemas import ConceptClaim, Evidence, ScopeExtraction

TEXT = "K-means needs the number of clusters in advance, unlike hierarchical methods."


def _run_rows(run: str, block_prefix: str, text: str = TEXT) -> pl.DataFrame:
    """One run's sources: a B2B copy has the same text under other ids."""
    return pl.DataFrame(
        {
            "platform": "mitxonline",
            "courserun_readable_id": run,
            "course_title": f"Course {run}",
            "scope_id": f"{block_prefix}-seq",
            "scope_kind": "unit",
            "scope_title": "Lecture 1",
            "scope_position": 1,
            "source_position": [1, 2],
            "source_ref": [f"{block_prefix}-html", f"{block_prefix}-video"],
            "source_kind": ["html", "video"],
            "source_title": ["Intro", "Clustering"],
            "source_text": ["Welcome to the lecture.", text],
        }
    )


def _scope(run: str, block_prefix: str, text: str = TEXT) -> scopes.Scope:
    return scopes.build_scopes(_run_rows(run, block_prefix, text))[0]


def _result() -> _ModelResult:
    evidence = [
        Evidence(source_id="S02", passage="needs the number of clusters in advance")
    ]
    return _ModelResult(
        extraction=ScopeExtraction(
            summary="Introduces k-means.",
            concepts=[
                ConceptClaim(
                    label="k-means clustering",
                    description="Partitions points into k clusters.",
                    concept_kind="method",
                    teaching_status="taught",
                    evidence=evidence,
                )
            ],
            skills=[],
            assumed_knowledge=[],
            assessment_expectations=[],
            learning_outcomes=[],
            declared_prerequisites=[],
            audience=[],
            notes=["The video has no slides."],
        ),
        stop_reason="tool_use",
        input_tokens=100,
        output_tokens=10,
        cache_read_input_tokens=0,
        cache_creation_input_tokens=0,
    )


@pytest.fixture
def calls(monkeypatch) -> list[str]:
    sent: list[str] = []

    def fake_call_model(_client, _model_id, _effort, message):
        sent.append(message)
        return _result()

    monkeypatch.setattr(extract, "call_model", fake_call_model)
    return sent


def _extract(scope_list, **kwargs) -> extract.ExtractionTables:
    return extract_scopes(
        scope_list, client=None, model_id="model", effort="medium", **kwargs
    )


def test_copies_share_a_content_key_but_not_an_extraction_key():
    original, copy = _scope("run-a", "a"), _scope("run-b", "b")
    assert content_key(original, "model", "medium") == content_key(
        copy, "model", "medium"
    )
    assert extraction_key(original, "model", "medium") != extraction_key(
        copy, "model", "medium"
    )
    edited = _scope("run-b", "b", text="Hierarchical clustering needs no k.")
    assert content_key(edited, "model", "medium") != content_key(
        original, "model", "medium"
    )
    assert content_key(original, "model", "high") != content_key(
        original, "model", "medium"
    )


def test_identical_text_in_two_runs_is_extracted_once(calls):
    original, copy = _scope("run-a", "a"), _scope("run-b", "b")
    tables = _extract([original, copy])
    assert len(calls) == 1
    by_run = {
        row["courserun_readable_id"]: row
        for row in tables.extractions.iter_rows(named=True)
    }
    assert by_run["run-a"]["input_tokens"] == 100
    assert by_run["run-b"]["input_tokens"] == 0
    assert by_run["run-b"]["stop_reason"] == REUSED_STOP_REASON
    assert by_run["run-b"]["summary"] == "Introduces k-means."
    assert by_run["run-b"]["notes_json"] == '["The video has no slides."]'
    # each copy is keyed and described as its own run, so dbt needs no change
    for row in by_run.values():
        assert stored_extraction_key(row) == row["extraction_key"]
    assert '"b-video"' in by_run["run-b"]["sources_json"]
    claims = tables.claims.join(tables.extractions, on="extraction_key")
    assert sorted(claims["courserun_readable_id"]) == ["run-a", "run-b"]
    evidence = tables.evidence.join(tables.extractions, on="extraction_key").select(
        "courserun_readable_id", "source_ref", "verification_status", "char_start"
    )
    assert sorted(evidence.iter_rows()) == [
        ("run-a", "a-video", "exact", 8),
        ("run-b", "b-video", "exact", 8),
    ]


def test_a_text_difference_is_extracted_again(calls):
    _extract([_scope("run-a", "a"), _scope("run-b", "b", text="Other text, other k.")])
    assert len(calls) == 2


def test_a_stored_extraction_is_copied_for_a_new_run(calls):
    stored = _extract([_scope("run-a", "a")])
    tables = _extract(
        [_scope("run-a", "a"), _scope("run-b", "b")],
        existing_keys=set(stored.extractions["extraction_key"]),
        reusable=stored,
    )
    assert len(calls) == 1  # only the first extraction
    assert tables.extractions["courserun_readable_id"].to_list() == ["run-b"]
    assert tables.evidence["source_ref"].to_list() == ["b-video"]
    assert tables.claims["label"].to_list() == ["k-means clustering"]


def test_key_on_header_reuses_only_under_the_same_header(calls):
    stored = _extract([_scope("run-a", "a")])
    copy = _scope("run-b", "b")  # another course title, so another header
    row = stored.extractions.row(0, named=True)
    assert stored_content_key(row) == content_key(copy, "model", "medium")
    assert stored_content_key(row, key_on_header=True) != content_key(
        copy, "model", "medium", key_on_header=True
    )
    _extract([copy], reusable=stored, key_on_header=True)
    assert len(calls) == 2


def test_a_failed_call_leaves_its_copies_out(monkeypatch):
    def failing_call_model(*_args):
        raise extract.ExtractionError

    monkeypatch.setattr(extract, "call_model", failing_call_model)
    tables = _extract([_scope("run-a", "a"), _scope("run-b", "b")])
    assert tables.extractions.is_empty()
    assert tables.claims.is_empty()


@pytest.mark.usefixtures("calls")
def test_truncation_notes_name_the_copy_s_own_blocks(monkeypatch):
    monkeypatch.setattr(scopes, "SOURCE_MAX_CHARS", 30)
    tables = _extract([_scope("run-a", "a"), _scope("run-b", "b")])
    notes = {
        row["courserun_readable_id"]: row["notes_json"]
        for row in tables.extractions.iter_rows(named=True)
    }
    assert "S02 (a-video) was cut" in notes["run-a"]
    assert "S02 (b-video) was cut" in notes["run-b"]
    assert "a-video" not in notes["run-b"]
    assert notes["run-b"].startswith('["The video has no slides."')


def test_dry_run_marks_copies_as_reused(tmp_path, capsys):
    sources = tmp_path / "sources.parquet"
    pl.concat([_run_rows("run-a", "a"), _run_rows("run-b", "b")]).write_parquet(sources)
    local.extract(
        sources, tmp_path / "out", model_id="model", effort="medium", dry_run=True
    )
    out = capsys.readouterr().out
    assert out.count(" reused\n") == 1
    assert out.count(" stored\n") == 0
