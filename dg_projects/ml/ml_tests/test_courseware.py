from types import SimpleNamespace

import httpx2
import polars as pl
import pytest
from ml.lib.courseware import extract, local, scopes
from ml.lib.courseware.describe import _concept_inputs
from ml.lib.courseware.extract import (
    CLAIM_SCHEMA,
    EVIDENCE_SCHEMA,
    EXTRACTION_SCHEMA,
    ExtractionTables,
    _claim_row,
    _ModelResult,
    extraction_key,
)
from ml.lib.courseware.schemas import (
    AudienceClaim,
    ConceptClaim,
    Evidence,
    ScopeExtraction,
)
from ml.lib.courseware.verify import verify_passage

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


def test_extraction_key_changes_with_input_text():
    first = scopes.build_scopes(_rows(["some text"]))[0]
    second = scopes.build_scopes(_rows(["other text"]))[0]
    assert extraction_key(first, "model", "medium") == extraction_key(
        first, "model", "medium"
    )
    assert extraction_key(first, "model", "medium") != extraction_key(
        second, "model", "medium"
    )
    assert extraction_key(first, "model", "medium") != extraction_key(
        first, "model", "high"
    )


def test_claim_rows_use_statement_as_label():
    evidence = [Evidence(source_id="S01", passage="for people new to Python")]
    audience = _claim_row(
        "key", 1, "audience", AudienceClaim(statement="Beginners", evidence=evidence)
    )
    assert audience["label"] == "Beginners"
    assert audience["description"] is None
    concept = _claim_row(
        "key",
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
