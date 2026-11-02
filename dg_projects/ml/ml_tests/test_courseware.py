import hashlib
import json
from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

import httpx2
import polars as pl
import pytest
from ml.lib.courseware import extract, local, scopes
from ml.lib.courseware.describe import _concept_inputs
from ml.lib.courseware.extract import (
    CLAIM_SCHEMA,
    EVIDENCE_SCHEMA,
    EXTRACTION_SCHEMA,
    NO_CLAIMS_STOP_REASON,
    TEXT_EXTRACTION_SCHEMA,
    ExtractionTables,
    TextReading,
    _claim_row,
    _ModelResult,
    check_revisions,
    extract_scopes,
    extraction_key,
    header_version,
    scope_inputs,
    stored_readings,
    text_extraction_key,
)
from ml.lib.courseware.prompts import context_message, text_message
from ml.lib.courseware.schemas import (
    AudienceClaim,
    ClaimRevision,
    ConceptClaim,
    ContextJudgment,
    Evidence,
    ScopeExtraction,
)
from ml.lib.courseware.verify import verify_passage
from pydantic import ValidationError

SOURCE = (
    "INSTRUCTOR: K-means follows a completely different methodology than "
    "hierarchical clustering. The main difference at the starting point is that for "
    "k-means clustering, we need to know the number of clusters in advance."
)


@pytest.mark.parametrize(
    ("passage", "status"),
    [
        # case, punctuation and whitespace differences still match exactly
        (
            "k-means follows a completely different   methodology than hierarchical",
            "exact",
        ),
        # one dropped word in a long passage is a near match
        (
            (
                "The main difference at the starting point is that for k-means "
                "clustering we need know the number of clusters in advance"
            ),
            "fuzzy",
        ),
        ("Gradient boosting builds an ensemble of shallow trees.", "unverified"),
    ],
)
def test_verify_passage(passage, status):
    assert verify_passage(passage, SOURCE).status == status


def test_verify_passage_rejects_a_quote_stitched_from_distant_sentences():
    # most of its trigrams are in the source, but in two places a sentence apart
    passage = (
        "k-means follows a completely different methodology we need to know the "
        "number of clusters in advance"
    )
    assert verify_passage(passage, SOURCE) == ("unverified", None, None)


def test_verify_passage_near_match_offsets_cover_the_quoted_stretch():
    match = verify_passage(
        "for k-means clustering we need know the number of clusters in advance", SOURCE
    )
    assert match.status == "fuzzy"
    assert SOURCE[match.char_start : match.char_end] == (
        "for k-means clustering, we need to know the number of clusters in advance"
    )


def test_verify_passage_offsets_index_the_source():
    match = verify_passage("we need to know the number of clusters", SOURCE)
    assert SOURCE[match.char_start : match.char_end] == (
        "we need to know the number of clusters"
    )


def test_verify_passage_unknown_source():
    assert verify_passage("anything at all", None).status == "unknown_source"


def test_verify_passage_matches_whole_words_only():
    assert verify_passage("art", "the start of the course").status == "unverified"
    assert verify_passage("art", "the art of the course").status == "exact"


@pytest.mark.parametrize(
    ("source", "passage", "quoted"),
    [
        # the source keeps a ligature, a superscript and a sharp s that NFKC and
        # casefolding rewrite in the passage
        ("the \ufb01rst moment is x\u00b2", "the \ufb01rst moment is x\u00b2", None),
        ("the \ufb01rst moment", "The first moment", None),
        ("die Stra\u00dfe ist lang", "STRASSE ist lang", "Stra\u00dfe ist lang"),
        ("a cafe\u0301 menu", "caf\u00e9 menu", "cafe\u0301 menu"),
    ],
)
def test_verify_passage_normalizes_the_source_like_the_passage(source, passage, quoted):
    match = verify_passage(passage, source)
    assert match.status == "exact"
    expected = quoted or source
    assert source[match.char_start : match.char_end] == expected


def _rows(texts: list[str], scope_kind: str = "unit") -> pl.DataFrame:
    return pl.DataFrame(
        {
            "platform": "mitxonline",
            "courserun_readable_id": "course-v1:X+Y+Z",
            "course_title": "Course",
            "scope_id": "seq-1",
            "scope_kind": scope_kind,
            "scope_title": "Lecture 1",
            "scope_position": 1,
            "source_position": list(range(1, len(texts) + 1)),
            "source_ref": [f"block-{index}" for index in range(len(texts))],
            "source_kind": "html",
            "source_title": None,
            "source_text": texts,
        }
    )


def test_build_scopes_splits_at_source_boundaries(monkeypatch):
    monkeypatch.setattr(scopes, "SCOPE_INPUT_BUDGET_CHARS", 10)
    built = scopes.build_scopes(_rows(["aaaaaa", "bbbbbb", "cc"]))
    assert [(scope.part, scope.part_count) for scope in built] == [(1, 2), (2, 2)]
    # ids restart in each part, so a part's prompt depends only on its own sources
    assert [source.source_id for source in built[1].sources] == ["S01", "S02"]
    assert built[0].course.outline == ("Lecture 1",)
    # each part records every part's checksum, so the whole reading can be matched
    assert (
        built[0].scope_part_checksums
        == built[1].scope_part_checksums
        == (f"{built[0].input_text_checksum},{built[1].input_text_checksum}")
    )


def test_course_description_from_about_page_or_ocw_description():
    about = _rows(["The about page."], scope_kind="context").with_columns(
        pl.lit("about/overview.html").alias("source_ref"),
        pl.lit("about_page").alias("source_kind"),
    )
    assert scopes.build_scopes(about)[0].course.description == "The about page."
    ocw = _rows(["An OCW course."], scope_kind="context").with_columns(
        pl.lit("ocw").alias("platform"),
        pl.lit("courses/18-06-linear-algebra-spring-2010/").alias("source_ref"),
        pl.lit("course_description").alias("source_kind"),
    )
    assert scopes.build_scopes(ocw)[0].course.description == "An OCW course."


def _course_rows(description: str, second_title: str, text: str) -> pl.DataFrame:
    units = pl.concat(
        [
            _rows([text]),
            _rows(["second lecture"]).with_columns(
                scope_id=pl.lit("seq-2"),
                scope_title=pl.lit(second_title),
                scope_position=pl.lit(2),
            ),
        ]
    )
    context = _rows([description], scope_kind="context").with_columns(
        scope_id=pl.lit("context"),
        scope_title=pl.lit("Course context"),
        scope_position=pl.lit(0),
        source_ref=pl.lit("about/overview.html"),
    )
    return pl.concat([context, units])


def _lecture_one(rows: pl.DataFrame) -> scopes.Scope:
    return next(
        scope for scope in scopes.build_scopes(rows) if scope.scope_id == "seq-1"
    )


class FakeModel:
    """A client whose messages.stream answers each extraction stage's tool.

    The text stage records one taught concept quoting source S01 (none with
    concepts=False); the context stage answers from the queue it was given, then
    with no revisions.
    """

    def __init__(
        self, *, context: list[dict[str, Any]] | None = None, concepts: bool = True
    ) -> None:
        self.calls: list[str] = []
        self.context = list(context or [])
        self.concepts = concepts
        self.messages = self

    def _extraction(self, message: str) -> dict[str, Any]:
        text = message.split("=== SOURCE S01", 1)[1].split("===\n", 1)[1]
        passage = text.split("\n=== END", 1)[0]
        concept = {
            "label": "k-means clustering",
            "description": "Partitions points.",
            "concept_kind": "method",
            "teaching_status": "taught",
            "evidence": [{"source_id": "S01", "passage": passage}],
        }
        return {
            "summary": "s",
            "concepts": [concept] if self.concepts else [],
            "skills": [],
            "assumed_knowledge": [],
            "assessment_expectations": [],
            "learning_outcomes": [],
            "declared_prerequisites": [],
            "audience": [],
            "notes": [],
        }

    def stream(self, **request: Any) -> Any:
        tool = request["tools"][0]["name"]
        self.calls.append(tool)
        if tool == "record_extraction":
            tool_input = self._extraction(request["messages"][0]["content"])
        else:
            tool_input = (
                self.context.pop(0) if self.context else {"revisions": [], "notes": []}
            )
        response = SimpleNamespace(
            stop_reason="tool_use",
            stop_details=None,
            content=[SimpleNamespace(type="tool_use", name=tool, input=tool_input)],
            usage=SimpleNamespace(
                input_tokens=10,
                output_tokens=5,
                cache_read_input_tokens=0,
                cache_creation_input_tokens=0,
            ),
        )
        return nullcontext(SimpleNamespace(get_final_message=lambda: response))


def _use_model(monkeypatch: pytest.MonkeyPatch, model: FakeModel) -> None:
    monkeypatch.setattr(local, "AnthropicBedrock", FakeModel)
    monkeypatch.setattr(
        local,
        "LLMClientFactory",
        lambda **_: SimpleNamespace(get_client=lambda: model),
    )


def _reading() -> TextReading:
    evidence = [Evidence(source_id="S01", passage="some text")]
    return TextReading(
        text_extraction_key="t",
        text_input_digest="d",
        platform="mitxonline",
        courserun_readable_id="course-v1:X+Y+Z",
        scope_id="seq-1",
        summary="s",
        notes=[],
        claims=[
            _claim_row(
                1,
                "concept",
                ConceptClaim(
                    label="regression",
                    description="Fits a line.",
                    concept_kind="method",
                    teaching_status="taught",
                    evidence=evidence,
                ),
            ),
            _claim_row(
                2, "audience", AudienceClaim(statement="Beginners", evidence=evidence)
            ),
        ],
        evidence=[],
    )


def test_text_key_leaves_out_the_course_header_and_extraction_key_does_not():
    base = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    new_description = _lecture_one(_course_rows("Edited.", "Lecture 2", "some text"))
    renamed_other = _lecture_one(_course_rows("About it.", "Renamed", "some text"))
    edited_text = _lecture_one(_course_rows("About it.", "Lecture 2", "new text"))
    # the text stage reads nothing of the course, its own title included
    assert "Lecture 1" not in text_message(base)
    key = text_extraction_key(base, "model", "medium")
    assert text_extraction_key(new_description, "model", "medium") == key
    assert text_extraction_key(renamed_other, "model", "medium") == key
    assert text_extraction_key(edited_text, "model", "medium") != key
    assert text_extraction_key(base, "model", "high") != key
    # the context stage reads the header, so its key covers it
    reading = _reading()
    extraction = extraction_key(base, "model", "medium", reading)
    assert extraction_key(new_description, "model", "medium", reading) != extraction
    assert extraction_key(renamed_other, "model", "medium", reading) != extraction
    assert header_version(new_description.course) != header_version(base.course)


def test_header_edit_reruns_only_the_context_stage(tmp_path, monkeypatch, capsys):
    model = FakeModel()
    _use_model(monkeypatch, model)
    sources = tmp_path / "sources.parquet"
    output_dir = tmp_path / "out"

    def run(description: str, second_title: str) -> list[str]:
        model.calls.clear()
        _course_rows(description, second_title, "some text").write_parquet(sources)
        local.extract(sources, output_dir)
        return sorted(model.calls)

    # three scopes: the course context, seq-1 and seq-2
    assert (
        run("About it.", "Lecture 2")
        == ["record_context"] * 3 + ["record_extraction"] * 3
    )
    assert run("About it.", "Lecture 2") == []
    # a renamed unit changes every scope's outline, and no scope's text
    assert run("About it.", "Renamed") == ["record_context"] * 3
    # the description is the context scope's own text, so only it is read again
    assert run("Edited.", "Renamed") == ["record_context"] * 3 + ["record_extraction"]
    extractions = pl.read_parquet(output_dir / "courseware_claim_extraction.parquet")
    assert extractions.height == 9
    assert extractions["header_version"].n_unique() == 3
    lecture = extractions.filter(pl.col("scope_id") == "seq-1")
    assert lecture["text_extraction_key"].n_unique() == 1
    assert lecture["context_input_digest"].n_unique() == 3
    text = pl.read_parquet(output_dir / "courseware_text_extraction.parquet")
    assert text.height == 4
    capsys.readouterr()


def test_context_stage_revises_status_and_sense_but_keeps_the_text_evidence():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    revision = {"claim_id": "C01", "teaching_status": "reviewed", "sense_hint": "ml"}
    model = FakeModel(context=[{"revisions": [revision], "notes": ["placed"]}])

    tables = extract_scopes([scope], model, model_id="model")

    claim = tables.claims.row(0, named=True)
    assert (
        claim["teaching_status"],
        claim["text_teaching_status"],
        claim["sense_hint"],
    ) == ("reviewed", "taught", "ml")
    assert tables.evidence.select("source_ref", "verification_status").rows() == [
        ("block-0", "exact")
    ]
    extraction = tables.extractions.row(0, named=True)
    text = tables.text_extractions.row(0, named=True)
    assert extraction["text_extraction_key"] == text["text_extraction_key"]
    assert extraction["text_input_digest"] == text["text_input_digest"]
    assert (
        extraction["text_input_digest"]
        == hashlib.sha256(text_message(scope).encode()).hexdigest()
    )
    (reading,) = stored_readings(tables.text_extractions).values()
    assert (
        extraction["context_input_digest"]
        == hashlib.sha256(
            context_message(scope, reading.claims, reading.evidence).encode()
        ).hexdigest()
    )
    assert json.loads(extraction["notes_json"]) == ["placed"]


def test_context_revisions_sent_as_a_json_string_are_decoded():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    revision = {"claim_id": "C01", "teaching_status": "reviewed", "sense_hint": "ml"}
    model = FakeModel(
        context=[{"revisions": json.dumps([revision]) + "}", "notes": "[]"}]
    )

    tables = extract_scopes([scope], model, model_id="model")

    assert model.calls == ["record_extraction", "record_context"]
    assert tables.claims["sense_hint"].to_list() == ["ml"]


def test_context_stage_retries_a_revision_of_a_claim_it_was_not_given():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    unknown = {"claim_id": "C09", "teaching_status": None, "sense_hint": "x"}
    model = FakeModel(context=[{"revisions": [unknown], "notes": []}])

    tables = extract_scopes([scope], model, model_id="model")

    assert model.calls == ["record_extraction", "record_context", "record_context"]
    assert tables.extractions.height == 1
    assert tables.claims["sense_hint"].to_list() == [None]


@pytest.mark.parametrize(
    "revisions",
    [
        [ClaimRevision(claim_id="C03", teaching_status=None, sense_hint="x")],
        [
            ClaimRevision(claim_id="C01", teaching_status=None, sense_hint="x"),
            ClaimRevision(claim_id="C01", teaching_status=None, sense_hint="y"),
        ],
        # an audience claim has no teaching status to revise
        [ClaimRevision(claim_id="C02", teaching_status="taught", sense_hint=None)],
    ],
)
def test_check_revisions_refuses_what_the_text_stage_did_not_record(revisions):
    with pytest.raises(ValueError, match="C0"):
        check_revisions(_reading(), ContextJudgment(revisions=revisions, notes=[]))


def test_a_reading_without_claims_needs_no_context_call():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    model = FakeModel(concepts=False)

    tables = extract_scopes([scope], model, model_id="model")

    assert model.calls == ["record_extraction"]
    assert tables.extractions["stop_reason"].to_list() == [NO_CLAIMS_STOP_REASON]
    assert tables.extractions["input_tokens"].to_list() == [0]


def test_text_stage_reads_each_distinct_text_once_per_run():
    # two scopes with identical sources in one run, and a copy of the run
    run = pl.concat(
        [
            _rows(["shared text"]),
            _rows(["shared text"]).with_columns(scope_id=pl.lit("b")),
        ]
    ).with_columns(source_title=pl.lit("Intro"), source_ref=pl.lit("block"))
    copy = run.with_columns(courserun_readable_id=pl.lit("course-v1:X+Y+COPY"))
    built = [
        *scopes.build_scopes(run),
        *scopes.build_scopes(copy),
    ]
    model = FakeModel()

    tables = extract_scopes(built, model, model_id="model", max_workers=1)

    # a reading is reused within its run only, until cross-run reuse is evaluated
    assert model.calls.count("record_extraction") == 2
    assert model.calls.count("record_context") == 4
    assert tables.text_extractions.height == 2
    # every extraction of the text groups under one digest, so its claims are not
    # counted as independent corroboration
    assert tables.extractions["text_input_digest"].n_unique() == 1


def _run_and_copy(
    column: str = "courserun_readable_id", value: str = "course-v1:X+Y+COPY"
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return a run and a copy of it under another run, with its own block ids."""
    run = _rows(["shared text"]).with_columns(source_title=pl.lit("Intro"))
    return run, run.with_columns(
        pl.lit(value).alias(column), source_ref=pl.lit("copy-block-0")
    )


def test_text_readings_cross_runs_only_when_asked():
    run, copy = _run_and_copy()
    built = [*scopes.build_scopes(run), *scopes.build_scopes(copy)]
    model = FakeModel()

    tables = extract_scopes(
        built, model, model_id="model", max_workers=1, reuse_text_across_runs=True
    )

    assert model.calls.count("record_extraction") == 1
    # each run still places the claims in its own course
    assert model.calls.count("record_context") == 2
    (text,) = tables.text_extractions.iter_rows(named=True)
    copied = tables.extractions.filter(
        pl.col("courserun_readable_id") == "course-v1:X+Y+COPY"
    ).row(0, named=True)
    # the copy keeps the run its reading was made for, and the text's digest
    assert (copied["text_courserun_readable_id"], copied["text_scope_id"]) == (
        text["courserun_readable_id"],
        text["scope_id"],
    )
    assert copied["text_courserun_readable_id"] != copied["courserun_readable_id"]
    assert tables.extractions["text_input_digest"].n_unique() == 1
    # each run's evidence names its own blocks, the copy's included
    refs = tables.evidence.join(
        tables.extractions.select("extraction_key", "courserun_readable_id"),
        on="extraction_key",
    )
    assert dict(
        refs.group_by("courserun_readable_id")
        .agg(pl.col("source_ref").unique().sort())
        .iter_rows()
    ) == {"course-v1:X+Y+Z": ["block-0"], "course-v1:X+Y+COPY": ["copy-block-0"]}


@pytest.mark.parametrize(("across_runs", "text_calls"), [(False, 1), (True, 0)])
# a run is its platform and readable id, so a copy can differ in either
@pytest.mark.parametrize(
    ("column", "value"),
    [("courserun_readable_id", "course-v1:X+Y+COPY"), ("platform", "mitxpro")],
)
def test_local_extract_reuses_another_runs_reading_only_with_the_flag(
    tmp_path, monkeypatch, capsys, across_runs, text_calls, column, value
):
    run, copy = _run_and_copy(column, value)
    model = FakeModel()
    _use_model(monkeypatch, model)
    sources = tmp_path / "sources.parquet"
    output_dir = tmp_path / "out"
    run.write_parquet(sources)
    local.extract(sources, output_dir)
    copy.write_parquet(sources)
    model.calls.clear()
    capsys.readouterr()

    local.extract(
        sources,
        output_dir,
        reuse_text_across_runs=across_runs,
        dry_run=True,
    )
    marks = capsys.readouterr().out.splitlines()[0].split()[-1]
    local.extract(sources, output_dir, reuse_text_across_runs=across_runs)

    assert marks == ("reused" if across_runs else "tokens")
    assert model.calls.count("record_extraction") == text_calls
    assert model.calls.count("record_context") == 1
    capsys.readouterr()


def test_stored_readings_give_the_same_keys_without_a_text_call():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    first = extract_scopes([scope], FakeModel(), model_id="model")
    readings = stored_readings(first.text_extractions)
    stored_keys = set(first.extractions["extraction_key"])
    model = FakeModel()

    assert (
        extract_scopes(
            [scope],
            model,
            model_id="model",
            readings=readings,
            existing_keys=stored_keys,
        ).extractions.height
        == 0
    )
    assert model.calls == []
    # with the extraction lost but its reading stored, only the context stage runs,
    # and the same inputs give the same key
    again = extract_scopes([scope], model, model_id="model", readings=readings)
    assert model.calls == ["record_context"]
    assert set(again.extractions["extraction_key"]) == stored_keys


def test_scope_inputs_match_what_an_extraction_of_the_part_stores(tmp_path, capsys):
    rows = _course_rows("About it.", "Lecture 2", "some text")
    built = scopes.build_scopes(rows)
    scope = _lecture_one(rows)
    extraction = extract_scopes([scope], FakeModel(), model_id="model").extractions
    observed = (
        scope_inputs(built).filter(pl.col("scope_id") == "seq-1").row(0, named=True)
    )
    columns = (
        "scope_part",
        "scope_part_count",
        "input_text_checksum",
        "header_version",
    )
    assert {c: observed[c] for c in columns} == {
        c: extraction.row(0, named=True)[c] for c in columns
    }
    assert scope_inputs(built)["observed_at"].n_unique() == 1
    # observe records every scope of the run, and a dry run records nothing
    sources = tmp_path / "sources.parquet"
    rows.write_parquet(sources)
    local.extract(sources, tmp_path / "dry", dry_run=True)
    assert not (tmp_path / "dry" / "courseware_scope_input.parquet").exists()
    local.observe(sources, tmp_path / "out")
    stored = pl.read_parquet(tmp_path / "out" / "courseware_scope_input.parquet")
    assert sorted(stored["scope_id"]) == ["context", "seq-1", "seq-2"]
    capsys.readouterr()


def test_claim_rows_use_statement_as_label():
    evidence = [Evidence(source_id="S01", passage="for people new to Python")]
    audience = _claim_row(
        1, "audience", AudienceClaim(statement="Beginners", evidence=evidence)
    )
    assert audience["label"] == "Beginners"
    assert audience["description"] is None
    concept = _claim_row(
        2,
        "concept",
        ConceptClaim(
            label="k-means clustering",
            description="Partitions points.",
            concept_kind="method",
            teaching_status="taught",
            evidence=evidence,
        ),
    )
    assert (concept["label"], concept["teaching_status"]) == (
        "k-means clustering",
        "taught",
    )


def test_describe_inputs_group_by_concept_and_checksum_inputs():
    rows = pl.DataFrame(
        {
            "concept_id": ["auc", "auc", "pandas"],
            "label": ["AUC", "area under the ROC curve (AUC)", "pandas"],
            "concept_kind": ["concept", None, "tool"],
            "description": ["Ranks models.", "Summarizes ROC.", "Loads CSVs."],
            "course_title": ["Course A", "Course B", "Course A"],
            "context_rank": [1, 2, 1],
        }
    )
    first, second = _concept_inputs(rows)
    assert first["labels"] == ["AUC", "area under the ROC curve (AUC)"]
    assert first["notes"] == ["Course A: Ranks models.", "Course B: Summarizes ROC."]
    assert second["kind"] == "tool"
    changed = _concept_inputs(rows.with_columns(pl.lit("Other.").alias("description")))
    assert changed[0]["input_checksum"] != first["input_checksum"]


def test_extraction_ignores_extra_keys_the_model_adds():
    extraction = ScopeExtraction.model_validate(
        {
            "summary": "s",
            "concepts": [],
            "skills": [],
            "assumed_knowledge": [],
            "assessment_expectations": [],
            "learning_outcomes": [],
            "declared_prerequisites": [],
            "audience": [
                {
                    "statement": "Beginners",
                    "evidence": [{"source_id": "S01", "passage": "p", "position": 0}],
                }
            ],
            "notes": [],
        }
    )
    assert extraction.audience[0].evidence[0].source_id == "S01"
    assert ScopeExtraction.model_json_schema()["additionalProperties"] is False


def test_string_fields_keep_text_that_parses_as_json():
    evidence = Evidence.model_validate({"source_id": "S01", "passage": "[1, 2, 3]"})
    assert evidence.passage == "[1, 2, 3]"
    claim = ConceptClaim.model_validate(
        {
            "label": '{"a": 1}',
            "description": "A dict literal.",
            "concept_kind": "concept",
            "teaching_status": "taught",
            "evidence": '[{"source_id": "S01", "passage": "a b c"}]',
        }
    )
    assert claim.label == '{"a": 1}'
    assert claim.evidence == [Evidence(source_id="S01", passage="a b c")]


@pytest.mark.parametrize(
    "evidence",
    [
        '[{"source_id": "S01", "passage": "a b c"}]',
        '  [{"source_id": "S01", "passage": "a b c"}]]} ',
        ['{"source_id": "S01", "passage": "a b c"}'],
    ],
)
def test_json_encoded_containers_are_decoded(evidence):
    claim = AudienceClaim.model_validate({"statement": "s", "evidence": evidence})
    assert claim.evidence == [Evidence(source_id="S01", passage="a b c")]


@pytest.mark.parametrize(
    "evidence",
    [
        # an object where the field takes a list
        '{"source_id": "S01", "passage": "a b c"}',
        # text after the array that is not a closing bracket
        '[{"source_id": "S01", "passage": "a b c"}] and more',
        '[{"source_id": "S01", "passage": "a b c"',
    ],
)
def test_strings_that_do_not_encode_the_field_s_container_still_fail(evidence):
    with pytest.raises(ValidationError, match="list_type"):
        AudienceClaim.model_validate({"statement": "s", "evidence": evidence})


def test_local_extract_replaces_claims_a_cut_off_run_left_behind(tmp_path, monkeypatch):
    # a run cut off after writing claims left a row for an extraction it never
    # stored; the next run extracts that scope again and keeps one copy
    sources = tmp_path / "sources.parquet"
    _rows(["some text"]).write_parquet(sources)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    orphan = pl.DataFrame(
        [{"extraction_key": "k1", "label": "old"}], schema=CLAIM_SCHEMA
    )
    orphan.write_parquet(output_dir / "courseware_claim.parquet")
    tables = ExtractionTables(
        text_extractions=pl.DataFrame(schema=TEXT_EXTRACTION_SCHEMA),
        extractions=pl.DataFrame([{"extraction_key": "k1"}], schema=EXTRACTION_SCHEMA),
        claims=pl.DataFrame(
            [{"extraction_key": "k1", "label": "new"}], schema=CLAIM_SCHEMA
        ),
        evidence=pl.DataFrame(schema=EVIDENCE_SCHEMA),
    )
    monkeypatch.setattr(local, "AnthropicBedrock", object)
    monkeypatch.setattr(
        local, "LLMClientFactory", lambda **_: SimpleNamespace(get_client=object)
    )
    monkeypatch.setattr(local, "extract_scopes", lambda *_, **__: tables)
    local.extract(sources, output_dir)
    claims = pl.read_parquet(output_dir / "courseware_claim.parquet")
    assert claims["label"].to_list() == ["new"]
    stored = pl.read_parquet(output_dir / "courseware_claim_extraction.parquet")
    assert stored["extraction_key"].to_list() == ["k1"]


def test_extract_scopes_keeps_the_batch_when_a_stream_drops(monkeypatch):
    built = scopes.build_scopes(
        pl.concat(
            [_rows(["first"]), _rows(["second"]).with_columns(scope_id=pl.lit("seq-2"))]
        )
    )
    empty = ScopeExtraction.model_validate(
        {
            "summary": "s",
            "concepts": [],
            "skills": [],
            "assumed_knowledge": [],
            "assessment_expectations": [],
            "learning_outcomes": [],
            "declared_prerequisites": [],
            "audience": [],
            "notes": [],
        }
    )

    def call_model(*args, **_):
        message = args[-1]
        if "second" in message:
            msg = "peer closed connection"
            raise httpx2.RemoteProtocolError(msg)
        return _ModelResult(empty, "end_turn", 1, 1, 0, 0)

    monkeypatch.setattr(extract, "call_model", call_model)
    tables = extract.extract_scopes(built, object(), model_id="model", max_workers=1)
    assert tables.extractions["scope_id"].to_list() == ["seq-1"]


def _older_outputs(output_dir) -> None:
    """Write a claim and an extraction as v1 stored them, and a read without header."""
    rows = _course_rows("About it.", "Lecture 2", "some text")
    tables = extract_scopes([_lecture_one(rows)], FakeModel(), model_id="model")
    tables.extractions.drop(
        "text_extraction_key", "text_input_digest", "context_input_digest"
    ).write_parquet(output_dir / "courseware_claim_extraction.parquet")
    tables.claims.drop("text_teaching_status", "sense_hint").write_parquet(
        output_dir / "courseware_claim.parquet"
    )
    tables.evidence.write_parquet(output_dir / "courseware_claim_evidence.parquet")
    scope_inputs(scopes.build_scopes(rows)).drop("header_version").write_parquet(
        output_dir / "courseware_scope_input.parquet"
    )


def test_conform_adds_the_columns_older_tables_lack(tmp_path, capsys):
    _older_outputs(tmp_path)
    keys = pl.read_parquet(tmp_path / "courseware_claim.parquet")["extraction_key"]
    local.conform(tmp_path)
    local.conform(tmp_path)  # idempotent
    for table, schema in local.OUTPUT_SCHEMAS.items():
        path = tmp_path / f"{table}.parquet"
        # a table the directory never had is not created
        if table == local.TEXT_EXTRACTION_TABLE:
            assert not path.exists()
            continue
        assert pl.read_parquet(path).schema == pl.Schema(schema)
    claims = pl.read_parquet(tmp_path / "courseware_claim.parquet")
    assert claims["extraction_key"].equals(keys)
    assert claims["sense_hint"].to_list() == [None]
    assert capsys.readouterr().out.splitlines() == [
        "3 tables brought to the current schema",
        "0 tables brought to the current schema",
    ]


def test_conform_refuses_a_column_the_schema_dropped(tmp_path):
    _older_outputs(tmp_path)
    path = tmp_path / "courseware_claim.parquet"
    pl.read_parquet(path).with_columns(retired=pl.lit(1)).write_parquet(path)
    with pytest.raises(ValueError, match="retired"):
        local.conform(tmp_path)


def test_conform_refuses_a_column_whose_type_changed(tmp_path):
    _older_outputs(tmp_path)
    path = tmp_path / "courseware_claim.parquet"
    pl.read_parquet(path).with_columns(claim_ordinal=pl.lit(1.5)).write_parquet(path)
    with pytest.raises(ValueError, match="claim_ordinal"):
        local.conform(tmp_path)
    assert pl.read_parquet(path)["claim_ordinal"].to_list() == [1.5]


def test_extract_and_observe_refuse_older_tables_before_calling_the_model(
    tmp_path, monkeypatch, capsys
):
    sources = tmp_path / "sources.parquet"
    _course_rows("About it.", "Lecture 2", "some text").write_parquet(sources)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _older_outputs(output_dir)
    model = FakeModel()
    _use_model(monkeypatch, model)
    # without the check the model is called and the append fails after it
    with pytest.raises(ValueError, match="run `local conform` first"):
        local.extract(sources, output_dir)
    with pytest.raises(ValueError, match="run `local conform` first"):
        local.observe(sources, output_dir)
    assert model.calls == []
    local.conform(output_dir)
    local.observe(sources, output_dir)
    observed = pl.read_parquet(output_dir / "courseware_scope_input.parquet")
    # the older read keeps a null header; the new one records it
    assert observed["header_version"].null_count() == observed.height // 2
    assert observed["header_version"].drop_nulls().n_unique() == 1
    capsys.readouterr()


class VaryingModel(FakeModel):
    """Reads the same text differently on each call, as a real model may."""

    def _extraction(self, message: str) -> dict[str, Any]:
        extraction = super()._extraction(message)
        for concept in extraction["concepts"]:
            concept["description"] = f"Reading {len(self.calls)}."
        return extraction


def test_reuse_across_runs_keeps_a_runs_own_reading():
    run, copy = _run_and_copy()
    model = VaryingModel()
    original = extract_scopes(scopes.build_scopes(run), model, model_id="model")
    own = extract_scopes(scopes.build_scopes(copy), model, model_id="model")
    text = pl.concat([original.text_extractions, own.text_extractions])
    stored_keys = set(own.extractions["extraction_key"])
    model.calls.clear()

    again = extract_scopes(
        scopes.build_scopes(copy),
        model,
        model_id="model",
        readings=stored_readings(text, across_runs=True),
        existing_keys=stored_keys,
        reuse_text_across_runs=True,
    )

    # the copy's own reading is newer than the original's, and still the one used
    assert model.calls == []
    assert again.extractions.height == 0


def test_extract_refuses_a_table_whose_columns_are_out_of_order(
    tmp_path, monkeypatch, capsys
):
    sources = tmp_path / "sources.parquet"
    rows = _course_rows("About it.", "Lecture 2", "some text")
    rows.write_parquet(sources)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    tables = extract_scopes([_lecture_one(rows)], FakeModel(), model_id="model")
    path = output_dir / "courseware_claim.parquet"
    # the right names and types, but the append concatenates by position
    tables.claims.select(reversed(tables.claims.columns)).write_parquet(path)
    model = FakeModel()
    _use_model(monkeypatch, model)
    with pytest.raises(ValueError, match="run `local conform` first"):
        local.extract(sources, output_dir)
    assert model.calls == []
    local.conform(output_dir)
    assert pl.read_parquet(path).schema == pl.Schema(extract.CLAIM_SCHEMA)
    capsys.readouterr()
