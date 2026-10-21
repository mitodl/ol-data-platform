"""Tests for ml.lib.summarize."""

from typing import Any, Self

import polars as pl
import pyarrow as pa
import pytest
from ml.lib import summarize
from ol_llm import (
    BatchFailed,
    BatchKeys,
    LLMInvalidOutput,
    ManagedPrompt,
    TextTemplate,
)
from ol_llm.testing import override_models
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, UserPromptPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

MODEL = "anthropic:claude-haiku-4-5"
TEXT = "turn one\n---\nturn two"
TEMPLATE = TextTemplate(template="Summarize: {{conversation_text}}", version="v4")
PROMPT = ManagedPrompt(name="feedback-summary", template=TEMPLATE, source="opik")
SUMMARIZER = summarize.Summarizer(
    use_case=summarize.summary_use_case(MODEL), prompt=PROMPT, run_id="run-1"
)


def _conversation_row(**overrides: object) -> dict[str, object]:
    conversation_ref = overrides.get("conversation_ref", "1")
    row = {
        # Distinct from conversation_ref -- these tests don't reimplement dbt's
        # surrogate-key hash, just a fake, stable, per-conversation pk.
        "feedback_conversation_pk": f"pk-{conversation_ref}",
        "source_slug": "zendesk",
        "conversation_ref": conversation_ref,
        "turn_count": 2,
        "conversation_text": TEXT,
        "conversation_text_chars": 600,
    }
    row.update(overrides)
    return row


def _sent_text(messages: list[ModelMessage]) -> str:
    return next(
        part.content
        for message in messages
        for part in getattr(message, "parts", ())
        if isinstance(part, UserPromptPart) and isinstance(part.content, str)
    )


def _summarize(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
    """Echo the conversation back, and reject any containing "too long"."""
    text = _sent_text(messages)
    if "too long" in text:
        raise ModelHTTPError(400, "claude-haiku-4-5", body="prompt is too long")
    return ModelResponse(parts=[TextPart(f"summary of: {text}")])


def test_needs_summary_summarizes_any_conversation_with_text() -> None:
    row = _conversation_row(turn_count=1, conversation_text_chars=10)

    assert summarize.needs_summary(row) is True


def test_needs_summary_rejects_null_conversation_text() -> None:
    """conversation_text can be null (redaction hasn't reached it yet) even when
    conversation_text_chars is set. Sending None to the LLM must never happen.
    """
    row = _conversation_row(conversation_text=None, conversation_text_chars=10_000)

    assert summarize.needs_summary(row) is False


def test_summary_use_case_is_on_the_batch_profile_in_the_ml_project() -> None:
    use_case = summarize.summary_use_case(MODEL)

    assert use_case.app == "dagster-ml"
    assert use_case.name == "feedback-summary"
    assert use_case.profile == "batch"
    assert use_case.model == MODEL


def test_fetch_summary_prompt_uses_the_embedded_default_with_tracing_off() -> None:
    prompt = summarize.fetch_summary_prompt()

    assert prompt.source == "embedded"
    assert prompt.version is None
    assert prompt.template.template == summarize.SUMMARY_PROMPT


def _stored(**columns: list[object]) -> pl.DataFrame:
    return pl.DataFrame(columns, schema_overrides={"idempotency_key": pl.String})


def _key(text: str, keys: Any = None) -> str:
    return (keys or SUMMARIZER.keys).key(text)


def test_filter_unsummarized_keeps_new_conversations_with_or_without_text() -> None:
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", conversation_text=None),
        ]
    )
    already_summarized_df = pl.DataFrame(schema={"feedback_conversation_pk": pl.String})

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["1", "2"]
    assert result.columns == source_df.columns


def test_filter_unsummarized_drops_conversations_summarized_with_the_same_key() -> None:
    row = _conversation_row(conversation_ref="1")
    source_df = pl.DataFrame([row])
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1"],
        idempotency_key=[_key(TEXT)],
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result.height == 0


def test_filter_unsummarized_resubmits_when_the_text_prompt_or_model_changes() -> None:
    """A new turn, a prompt edit, and a model change each change the key."""
    row = _conversation_row(conversation_ref="1")
    source_df = pl.DataFrame([row])
    edited = ManagedPrompt(
        name="feedback-summary",
        template=TextTemplate(template="Summarize briefly: {{conversation_text}}"),
        source="opik",
    )
    stale_keys = [
        _key("an earlier version of the conversation"),
        _key(TEXT, BatchKeys(model=MODEL, prompt=edited)),
        _key(TEXT, BatchKeys(model="anthropic:claude-sonnet-5", prompt=PROMPT)),
    ]

    for stale_key in stale_keys:
        already_summarized_df = _stored(
            feedback_conversation_pk=["pk-1"], idempotency_key=[stale_key]
        )

        result = summarize.filter_unsummarized(
            source_df, already_summarized_df, SUMMARIZER.keys
        )

        assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_ignores_whether_the_prompt_came_from_opik() -> None:
    """An Opik outage at the start of a run must not re-summarize every row."""
    row = _conversation_row(conversation_ref="1")
    embedded = ManagedPrompt(
        name="feedback-summary",
        template=TextTemplate(template=TEMPLATE.template),
        source="embedded",
        degraded_reasons=("embedded_prompt",),
    )
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1"],
        idempotency_key=[_key(TEXT)],
    )

    result = summarize.filter_unsummarized(
        pl.DataFrame([row]),
        already_summarized_df,
        BatchKeys(model=MODEL, prompt=embedded),
    )

    assert result.height == 0


def test_filter_unsummarized_resubmits_keyless_rows_once_they_have_text() -> None:
    """A skipped row and a row summarized before the key existed both have none."""
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", conversation_text=None),
        ]
    )
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1", "pk-2"], idempotency_key=[None, None]
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_reads_a_table_from_before_the_key() -> None:
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1")])
    already_summarized_df = pl.DataFrame({"feedback_conversation_pk": ["pk-1"]})

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_keeps_a_summary_whose_text_went_missing() -> None:
    source_df = pl.DataFrame(
        [_conversation_row(conversation_ref="1", conversation_text=None)]
    )
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1"], idempotency_key=[_key("the old text")]
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result.height == 0


def test_summarizer_sends_the_rendered_prompt_and_returns_the_summary() -> None:
    with override_models(FunctionModel(_summarize)):
        result = SUMMARIZER.summarize(_conversation_row(conversation_text="help"))

    assert result.output == "summary of: Summarize: help"
    assert result.model == MODEL
    assert result.prompt_version == "v4"


def test_summarizer_fails_a_summary_that_stays_empty() -> None:
    """A thinking model can return no text at all; that row fails, not succeeds."""
    calls: list[int] = []

    def empty(_messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
        calls.append(1)
        return ModelResponse(parts=[TextPart("  ")])

    with override_models(FunctionModel(empty)), pytest.raises(LLMInvalidOutput):
        SUMMARIZER.summarize(_conversation_row())

    # Sent back once (output_retries=1) before failing.
    assert len(calls) == 2


def test_summarized_frame_records_the_key_and_provenance() -> None:
    row = _conversation_row(conversation_ref="1")
    with override_models(FunctionModel(_summarize)):
        result = SUMMARIZER.summarize(row)

    frame = SUMMARIZER.summarized_frame([(row, result)])

    record = frame.row(0, named=True)
    assert frame.schema == pl.Schema(summarize.SUMMARIZE_CHECKPOINT_SCHEMA)
    assert record["conversation_summary"] == result.output
    assert record["summary_model_version"] == MODEL
    assert record["prompt_version"] == "v4"
    assert record["prompt_template_hash"] == PROMPT.template_hash
    assert record["idempotency_key"] == _key(TEXT)
    assert record["embedding_input"] == "summary"
    assert record["summarized_at"] is not None


def test_skipped_frame_types_its_null_columns() -> None:
    """Regression: Polars infers dtype=Null for an all-None Series, which Iceberg
    (format v2) rejects outright when writing the table.
    """
    frame = summarize.skipped_frame([_conversation_row(conversation_text=None)])

    record = frame.row(0, named=True)
    assert frame.schema == pl.Schema(summarize.SUMMARIZE_CHECKPOINT_SCHEMA)
    assert record["conversation_summary"] is None
    assert record["summary_model_version"] is None
    assert record["idempotency_key"] is None
    assert record["embedding_input"] == "concatenated_turns"


def _summary_row(**overrides: object) -> dict[str, object]:
    conversation_ref = overrides.get("conversation_ref", "1")
    row = {
        "feedback_conversation_pk": f"pk-{conversation_ref}",
        "source_slug": "zendesk",
        "conversation_ref": conversation_ref,
        "turn_count": 3,
        "conversation_summary": "a summary",
        "summary_model_version": "claude-haiku-4-5",
        "embedding_input": "summary",
    }
    row.update(overrides)
    return row


class _FakeField:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeSchema:
    def __init__(self, column_names: list[str]) -> None:
        self.column_names = column_names
        self.fields = [_FakeField(name) for name in column_names]


class _FakeSchemaUpdate:
    def __init__(self, table: "_FakeTable") -> None:
        self._table = table

    def union_by_name(self, schema: Any) -> None:
        # Mirrors real union_by_name: appends any not-yet-seen field at the end
        # of the table's column order, never inserting it where the incoming
        # schema happens to place it.
        for name in schema.names:
            if name not in self._table._column_names:
                self._table._column_names.append(name)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeTable:
    def __init__(self) -> None:
        self.upserts: list[dict[str, object]] = []
        self._column_names: list[str] = []

    def update_schema(self) -> _FakeSchemaUpdate:
        return _FakeSchemaUpdate(self)

    def schema(self) -> _FakeSchema:
        return _FakeSchema(self._column_names)

    def upsert(self, **kwargs: object) -> None:
        self.upserts.append(kwargs)


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.create_calls: list[str] = []

    def create_table_if_not_exists(
        self,
        identifier: str,
        **kwargs: Any,
    ) -> _FakeTable:
        self.create_calls.append(identifier)
        if not self._table._column_names:
            schema = kwargs.get("schema")
            if schema is not None:
                self._table._column_names = list(schema.names)
        return self._table


def test_checkpoint_chunk_upserts_a_non_empty_chunk() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    chunk_df = pl.DataFrame([_summary_row(), _summary_row(conversation_ref="2")])

    summarize.checkpoint_chunk(catalog, "some_db.feedback_summaries", chunk_df)

    assert catalog.create_calls == ["some_db.feedback_summaries"]
    assert len(table.upserts) == 1
    assert table.upserts[0]["join_cols"] == summarize.JOIN_COLS


def test_checkpoint_chunk_skips_empty_chunks_without_touching_the_catalog() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    empty_df = pl.DataFrame(
        schema={
            "feedback_conversation_pk": pl.String,
            "source_slug": pl.String,
            "conversation_ref": pl.String,
        }
    )

    summarize.checkpoint_chunk(catalog, "some_db.feedback_summaries", empty_df)

    assert catalog.create_calls == []
    assert table.upserts == []


def test_summarize_and_checkpoint_upserts_skipped_rows_then_each_chunk() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    df = pl.DataFrame(
        [
            *(_conversation_row(conversation_ref=str(i)) for i in range(5)),
            _conversation_row(conversation_ref="5", conversation_text=None),
        ]
    )

    with override_models(FunctionModel(_summarize)):
        result, report = summarize.summarize_and_checkpoint(
            df,
            SUMMARIZER,
            (catalog, "some_db.feedback_summaries"),
            batch_size=2,
            max_concurrency=1,
        )

    assert result.height == 6
    assert report.succeeded == 5
    # The skipped row, then chunks of 2, 2, and 1.
    assert len(table.upserts) == 4
    skipped = table.upserts[0]["df"]
    assert isinstance(skipped, pa.Table)
    assert skipped.num_rows == 1
    assert table.upserts[0]["join_cols"] == summarize.JOIN_COLS


def test_summarize_and_checkpoint_leaves_a_failed_conversation_out() -> None:
    """A conversation that fails is not upserted, so the next run tries it again."""
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1", conversation_text="too long"),
            _conversation_row(conversation_ref="2"),
        ]
    )

    with override_models(FunctionModel(_summarize)):
        result, report = summarize.summarize_and_checkpoint(
            df,
            SUMMARIZER,
            (catalog, "some_db.feedback_summaries"),
            max_concurrency=1,
        )

    assert result["conversation_ref"].to_list() == ["2"]
    assert len(report.failures) == 1
    assert report.failures[0].row["conversation_ref"] == "1"


def test_summarize_and_checkpoint_raises_when_every_call_fails() -> None:
    """100% failure must not look like "nothing new to summarize"."""
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref=str(i), conversation_text="too long")
            for i in range(3)
        ]
    )

    with (
        override_models(FunctionModel(_summarize)),
        pytest.raises(BatchFailed, match="3 of 3 rows failed"),
    ):
        summarize.summarize_and_checkpoint(
            df,
            SUMMARIZER,
            (catalog, "some_db.feedback_summaries"),
            max_concurrency=1,
        )
