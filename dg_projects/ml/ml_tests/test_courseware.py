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
    EXTRACTOR_ID,
    ExtractionTables,
    _claim_row,
    _ModelResult,
    extraction_key,
    header_version,
    scope_inputs,
    stored_extraction_key,
)
from ml.lib.courseware.extract import _rows as _rows_for
from ml.lib.courseware.prompts import scope_message
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


def test_extraction_key_ignores_the_course_header():
    base = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    new_description = _lecture_one(_course_rows("Edited.", "Lecture 2", "some text"))
    renamed_other = _lecture_one(_course_rows("About it.", "Renamed", "some text"))
    edited_text = _lecture_one(_course_rows("About it.", "Lecture 2", "new text"))
    key = extraction_key(base, "model", "medium")
    assert extraction_key(new_description, "model", "medium") == key
    assert extraction_key(renamed_other, "model", "medium") == key
    assert extraction_key(edited_text, "model", "medium") != key
    # the header is recorded, and keyed on only when asked
    assert header_version(new_description.course) != header_version(base.course)
    assert header_version(renamed_other.course) != header_version(base.course)
    assert extraction_key(
        new_description, "model", "medium", key_on_header=True
    ) != extraction_key(base, "model", "medium", key_on_header=True)


def _result() -> _ModelResult:
    empty = ScopeExtraction(
        summary="s",
        concepts=[],
        skills=[],
        assumed_knowledge=[],
        assessment_expectations=[],
        learning_outcomes=[],
        declared_prerequisites=[],
        audience=[],
        notes=[],
    )
    return _ModelResult(empty, "tool_use", 1, 1, 0, 0)


def test_stored_extraction_rows_recompute_their_key():
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    key = extraction_key(scope, "model", "medium")
    extraction, _, _ = _rows_for(scope, key, "model", "medium", _result())
    assert stored_extraction_key(extraction) == key
    assert extraction["header_version"] == header_version(scope.course)


def _old_style_outputs(output_dir) -> scopes.Scope:
    """Write one extraction keyed the old way, with a claim and its evidence."""
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    extraction, _, _ = _rows_for(scope, "old-key", "model", "medium", _result())
    del extraction["header_version"]
    pl.DataFrame([extraction]).write_parquet(
        output_dir / "courseware_claim_extraction.parquet"
    )
    claims = pl.DataFrame({"extraction_key": ["old-key"], "claim_ordinal": [1]})
    claims.write_parquet(output_dir / "courseware_claim.parquet")
    claims.with_columns(evidence_ordinal=pl.lit(1)).write_parquet(
        output_dir / "courseware_claim_evidence.parquet"
    )
    return scope


def _stored_keys(output_dir) -> list[list[str]]:
    return [
        pl.read_parquet(output_dir / f"{table}.parquet")["extraction_key"].to_list()
        for table in (
            local.EXTRACTION_TABLE,
            "courseware_claim",
            "courseware_claim_evidence",
        )
    ]


def test_rekey_recomputes_keys_without_re_extracting(tmp_path):
    scope = _old_style_outputs(tmp_path)
    local.rekey(tmp_path)
    local.rekey(tmp_path)  # idempotent
    key = extraction_key(scope, "model", "medium")
    assert _stored_keys(tmp_path) == [[key]] * 3
    stored = pl.read_parquet(tmp_path / "courseware_claim_extraction.parquet")
    assert stored["header_version"].to_list() == [None]
    assert stored.columns == list(EXTRACTION_SCHEMA)


def test_rekey_recovers_from_a_crash_after_the_first_write(tmp_path, monkeypatch):
    scope = _old_style_outputs(tmp_path)
    write = local._write
    written: list[str] = []

    def crash_after_first(rows, output_dir, table):
        if written:
            raise KeyboardInterrupt
        write(rows, output_dir, table)
        written.append(table)

    monkeypatch.setattr(local, "_write", crash_after_first)
    with pytest.raises(KeyboardInterrupt):
        local.rekey(tmp_path)
    # the extraction table still holds the old keys, so the mapping survives
    assert _stored_keys(tmp_path)[0] == ["old-key"]
    monkeypatch.setattr(local, "_write", write)
    local.rekey(tmp_path)
    assert _stored_keys(tmp_path) == [[extraction_key(scope, "model", "medium")]] * 3


def test_extract_refuses_keys_that_need_rekey(tmp_path, capsys):
    sources = tmp_path / "sources.parquet"
    _course_rows("About it.", "Lecture 2", "some text").write_parquet(sources)
    output_dir = tmp_path / "out"
    output_dir.mkdir()
    _old_style_outputs(output_dir)

    def dry_run():
        local.extract(
            sources, output_dir, model_id="model", effort="medium", dry_run=True
        )

    with pytest.raises(ValueError, match="local rekey"):
        dry_run()  # no header_version column
    extractions = pl.read_parquet(output_dir / "courseware_claim_extraction.parquet")
    extractions.with_columns(header_version=pl.lit(None, pl.String)).write_parquet(
        output_dir / "courseware_claim_extraction.parquet"
    )
    with pytest.raises(ValueError, match="local rekey"):
        dry_run()  # the column, but a null-header row on an old key
    local.rekey(output_dir)
    dry_run()
    assert capsys.readouterr().out.count(" stored\n") == 1


def _stored_marks(tmp_path, capsys, description: str, *, key_on_header: bool) -> int:
    sources = tmp_path / "sources.parquet"
    _course_rows(description, "Lecture 2", "some text").write_parquet(sources)
    local.extract(
        sources,
        tmp_path / "out",
        model_id="model",
        effort="medium",
        key_on_header=key_on_header,
        dry_run=True,
    )
    return capsys.readouterr().out.count(" stored\n")


@pytest.mark.parametrize("stored_on_header", [False, True])
def test_either_key_scheme_skips_what_the_other_extracted(
    tmp_path, capsys, stored_on_header
):
    scope = _lecture_one(_course_rows("About it.", "Lecture 2", "some text"))
    key = extraction_key(scope, "model", "medium", key_on_header=stored_on_header)
    extraction, _, _ = _rows_for(scope, key, "model", "medium", _result())
    (tmp_path / "out").mkdir()
    pl.DataFrame([extraction]).write_parquet(
        tmp_path / "out" / "courseware_claim_extraction.parquet"
    )
    assert _stored_marks(tmp_path, capsys, "About it.", key_on_header=False) == 1
    assert _stored_marks(tmp_path, capsys, "About it.", key_on_header=True) == 1
    # a header change is re-extracted only when asked for
    assert _stored_marks(tmp_path, capsys, "Edited.", key_on_header=False) == 1
    assert _stored_marks(tmp_path, capsys, "Edited.", key_on_header=True) == 0


def test_scope_inputs_match_what_an_extraction_of_the_part_stores(tmp_path, capsys):
    rows = _course_rows("About it.", "Lecture 2", "some text")
    built = scopes.build_scopes(rows)
    scope = _lecture_one(rows)
    extraction, _, _ = _rows_for(scope, "key", "model", "medium", _result())
    observed = (
        scope_inputs(built).filter(pl.col("scope_id") == "seq-1").row(0, named=True)
    )
    columns = ("scope_part", "scope_part_count", "input_text_checksum")
    assert {c: observed[c] for c in columns} == {c: extraction[c] for c in columns}
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


def _golden_scope() -> scopes.Scope:
    course = scopes.Course(
        platform="mitxonline",
        courserun_readable_id="course-v1:X+Y+Z",
        title="Course",
        outline=("Lecture 1", "Lecture 2"),
        description="",
    )
    source = scopes.Source(
        source_id="S01",
        source_ref="block-1",
        source_kind="html",
        title="Intro",
        text="Some text.",
        total_chars=10,
    )
    return scopes.Scope(
        course=course,
        scope_id="seq-2",
        scope_kind="unit",
        scope_title="Lecture 2",
        scope_position=2,
        part=1,
        part_count=2,
        scope_part_checksums="a,b",
        sources=(source,),
    )


GOLDEN_SCOPE_MESSAGE = """COURSE: Course

OUTLINE (> marks this scope)
- Lecture 1
> Lecture 2

DESCRIPTION
None available.

SCOPE: Lecture 2 (kind=unit)
This is part 1 of 2 of the scope; the other parts are extracted separately.

=== SOURCE S01 | kind=html | title='Intro' ===
Some text.
=== END S01 ==="""


def test_scope_message_layout_is_pinned_to_the_extractor_id():
    # The key hashes the scope's inputs, not the message, so a layout change under
    # the same EXTRACTOR_ID would reuse every stored key.
    assert (EXTRACTOR_ID, scope_message(_golden_scope())) == (
        "courseware_claims_v1",
        GOLDEN_SCOPE_MESSAGE,
    ), (
        "scope_message changed: bump EXTRACTOR_ID in ml.lib.courseware.extract, "
        "then update EXTRACTOR_ID and GOLDEN_SCOPE_MESSAGE here"
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
