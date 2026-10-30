"""Tests for ml.lib.summarize."""

from typing import Any

import boto3
import polars as pl
import pytest
from botocore.stub import Stubber
from ml.lib import summarize
from ml.lib.iceberg_helpers import upsert_chunk
from ml.resources import opik_auth
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
from pydantic_ai.models import override_allow_model_requests
from pydantic_ai.models.bedrock import BedrockConverseModel
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.providers.bedrock import BedrockProvider
from pyiceberg.catalog.sql import SqlCatalog

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
    """feedback_summaries rows, by default written by this run's model at turn 2."""
    rows = len(columns["feedback_conversation_pk"])
    columns = {
        "turn_count": [2] * rows,
        "summary_model_version": [MODEL] * rows,
        **columns,
    }
    return pl.DataFrame(
        columns,
        schema_overrides={
            "idempotency_key": pl.String,
            "summary_model_version": pl.String,
        },
    )


def _key(text: str, keys: Any = None) -> str:
    return (keys or SUMMARIZER.keys).key(text)


def test_filter_unsummarized_keeps_new_conversations_with_or_without_text() -> None:
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", conversation_text=None),
        ]
    )
    already_summarized_df = pl.DataFrame(
        schema={
            "feedback_conversation_pk": pl.String,
            "turn_count": pl.Int64,
            "summary_model_version": pl.String,
        }
    )

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


def test_filter_unsummarized_resubmits_skipped_rows_once_they_have_text() -> None:
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", conversation_text=None),
        ]
    )
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1", "pk-2"],
        summary_model_version=[None, None],
        idempotency_key=[None, None],
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_reads_a_table_from_before_the_key() -> None:
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", turn_count=3),
        ]
    )
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1", "pk-2"],
            "turn_count": [2, 2],
            "summary_model_version": ["claude-haiku-4-5", "claude-haiku-4-5"],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["2"]


def test_filter_unsummarized_keeps_keyless_rows_with_the_same_turns() -> None:
    """Rows summarized before the key existed are not all redone on deploy or on
    a model change: neither their prompt_version nor their model is compared.
    """
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", turn_count=3),
            _conversation_row(conversation_ref="3"),
        ]
    )
    already_summarized_df = _stored(
        feedback_conversation_pk=["pk-1", "pk-2", "pk-3"],
        summary_model_version=[
            "claude-haiku-4-5",
            "claude-haiku-4-5",
            "claude-sonnet-4-5",
        ],
        prompt_version=["v2", "v2", "v2"],
        idempotency_key=[None, None, None],
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, SUMMARIZER.keys
    )

    assert result["conversation_ref"].to_list() == ["2"]


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


def test_summarizer_caps_the_summary_length_and_thinking() -> None:
    sent: list[AgentInfo] = []

    def record(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        sent.append(info)
        return _summarize(messages, info)

    with override_models(FunctionModel(record)):
        SUMMARIZER.summarize(_conversation_row())

    [info] = sent
    assert info.model_settings is not None
    assert info.model_settings["max_tokens"] == summarize.SUMMARY_MAX_TOKENS


def test_summarizer_sends_bedrock_haiku_low_effort() -> None:
    """The thinking setting reaches Bedrock as Anthropic's effort. FunctionModel
    drops it (its profile has no thinking), so this stubs Bedrock's Converse.
    """
    client = boto3.client(
        "bedrock-runtime",
        region_name="us-east-1",
        aws_access_key_id="test",  # pragma: allowlist secret
        aws_secret_access_key="test",  # pragma: allowlist secret
    )
    sent: list[dict[str, Any]] = []
    client.meta.events.register(
        "provide-client-params.bedrock-runtime.Converse",
        lambda params, **_: sent.append(params),
    )
    stubber = Stubber(client)
    stubber.add_response(
        "converse",
        {
            "output": {
                "message": {"role": "assistant", "content": [{"text": "A summary."}]}
            },
            "stopReason": "end_turn",
            "usage": {"inputTokens": 1, "outputTokens": 1, "totalTokens": 2},
            "metrics": {"latencyMs": 1},
        },
    )
    model = BedrockConverseModel(
        "us.anthropic.claude-haiku-5-5",
        provider=BedrockProvider(bedrock_client=client),
    )
    summarizer = summarize.Summarizer(
        use_case=summarize.summary_use_case("bedrock:us.anthropic.claude-haiku-5-5"),
        prompt=SUMMARIZER.prompt,
    )

    with (
        stubber,
        override_models(model),
        override_allow_model_requests(allow_model_requests=True),
    ):
        summarizer.summarize(_conversation_row())

    [params] = sent
    assert params["additionalModelRequestFields"]["output_config"] == {"effort": "low"}


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


def test_summarizer_retries_a_summary_cut_off_at_max_tokens() -> None:
    """Thinking shares max_tokens, so a reply can stop mid-summary; the partial
    text is sent back, not stored.
    """
    replies = [
        ModelResponse(parts=[TextPart("The learner wanted")], finish_reason="length"),
        ModelResponse(parts=[TextPart("The learner wanted a refund.")]),
    ]

    def cut_off_once(_messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
        return replies.pop(0)

    with override_models(FunctionModel(cut_off_once)):
        result = SUMMARIZER.summarize(_conversation_row())

    assert result.output == "The learner wanted a refund."


def test_summarizer_retries_a_summary_bedrock_marked_malformed() -> None:
    """Bedrock's malformed_model_output arrives as finish_reason "error"."""
    replies = [
        ModelResponse(parts=[TextPart("The learner")], finish_reason="error"),
        ModelResponse(parts=[TextPart("The learner wanted a refund.")]),
    ]

    def malformed_once(
        _messages: list[ModelMessage], _info: AgentInfo
    ) -> ModelResponse:
        return replies.pop(0)

    with override_models(FunctionModel(malformed_once)):
        result = SUMMARIZER.summarize(_conversation_row())

    assert result.output == "The learner wanted a refund."


def test_summarizer_fails_a_summary_cut_off_twice() -> None:
    def cut_off(_messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("The learner")], finish_reason="length")

    with override_models(FunctionModel(cut_off)), pytest.raises(LLMInvalidOutput):
        SUMMARIZER.summarize(_conversation_row())


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


TABLE = "some_db.feedback_summaries"


def _stored_rows(catalog: SqlCatalog) -> pl.DataFrame:
    return catalog.load_table(TABLE).scan().to_polars().sort("conversation_ref")


def test_summarize_and_checkpoint_upserts_skipped_rows_then_each_chunk(
    iceberg_catalog: SqlCatalog, monkeypatch: pytest.MonkeyPatch
) -> None:
    written: list[pl.DataFrame] = []

    def recording_upsert_chunk(*args: Any) -> pl.DataFrame:
        written.append(args[2])
        return upsert_chunk(*args)

    monkeypatch.setattr(summarize, "upsert_chunk", recording_upsert_chunk)
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
            (iceberg_catalog, TABLE),
            batch_size=2,
            max_concurrency=1,
        )

    assert result.height == 6
    assert report.succeeded == 5
    # The skipped row, then chunks of 2, 2, and 1.
    assert [chunk.height for chunk in written] == [1, 2, 2, 1]
    assert written[0]["conversation_ref"].to_list() == ["5"]
    stored = _stored_rows(iceberg_catalog)
    assert stored["conversation_ref"].to_list() == [str(i) for i in range(6)]
    assert stored["embedding_input"].to_list() == ["summary"] * 5 + [
        "concatenated_turns"
    ]


def test_summarize_and_checkpoint_leaves_a_failed_conversation_out(
    iceberg_catalog: SqlCatalog,
) -> None:
    """A conversation that fails is not upserted, so the next run tries it again."""
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
            (iceberg_catalog, TABLE),
            max_concurrency=1,
        )

    assert result["conversation_ref"].to_list() == ["2"]
    assert _stored_rows(iceberg_catalog)["conversation_ref"].to_list() == ["2"]
    assert len(report.failures) == 1
    assert report.failures[0].row["conversation_ref"] == "1"


def test_summarize_and_checkpoint_raises_when_every_call_fails(
    iceberg_catalog: SqlCatalog,
) -> None:
    """100% failure must not look like "nothing new to summarize"."""
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
            (iceberg_catalog, TABLE),
            max_concurrency=1,
        )


def test_fetch_summary_prompt_warns_when_opik_differs_from_the_embedded_copy(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(summarize, "get_prompt", lambda *_args, **_kwargs: PROMPT)

    with caplog.at_level("WARNING", logger="ml.lib.summarize"):
        prompt = summarize.fetch_summary_prompt()

    assert prompt is PROMPT
    assert "differs from SUMMARY_PROMPT" in caplog.text


def test_fetch_summary_prompt_seeds_a_missing_label_then_fetches_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    labeled: list[tuple[str, str]] = []

    def _get_prompt(*_args: Any, **_kwargs: Any) -> ManagedPrompt[TextTemplate]:
        if not labeled:
            msg = "no qa label"
            raise LookupError(msg)
        return PROMPT

    monkeypatch.setattr(summarize, "get_prompt", _get_prompt)
    monkeypatch.setattr(
        summarize,
        "seed_prompt_environment",
        lambda name, template: labeled.append((name, template)),
    )

    assert summarize.fetch_summary_prompt() is PROMPT
    assert labeled == [("feedback-summary", summarize.SUMMARY_PROMPT)]


def test_fetch_summary_prompt_raises_when_production_has_no_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _missing(*_args: Any, **_kwargs: Any) -> ManagedPrompt[TextTemplate]:
        msg = "no production label"
        raise LookupError(msg)

    monkeypatch.setattr(summarize, "get_prompt", _missing)
    monkeypatch.setattr(opik_auth, "PROMPT_ENVIRONMENT", "production")
    monkeypatch.setattr(opik_auth, "get_opik_client", object)

    with pytest.raises(opik_auth.PromptLabelMissingError, match="labeled production"):
        summarize.fetch_summary_prompt()


def test_summarize_and_checkpoint_updates_a_table_from_before_the_key_columns(
    iceberg_catalog: SqlCatalog,
) -> None:
    """A table created before idempotency_key and prompt_template_hash existed gets
    both appended, its stored conversations are replaced, and the returned frame
    is in the table's column order, which the io_manager's upsert casts by.
    """
    old_columns = [
        name
        for name in summarize.SUMMARIZE_CHECKPOINT_SCHEMA
        if name not in {"prompt_template_hash", "idempotency_key"}
    ]
    existing = pl.DataFrame(
        [
            {
                **summarize._identity(_conversation_row(conversation_ref=ref)),
                "conversation_summary": "an old summary",
                "embedding_input": "summary",
            }
            for ref in ("1", "2")
        ],
        schema={
            name: summarize.SUMMARIZE_CHECKPOINT_SCHEMA[name] for name in old_columns
        },
    )
    iceberg_catalog.create_table(TABLE, schema=existing.to_arrow().schema).append(
        existing.to_arrow()
    )
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="3"),
        ]
    )

    with override_models(FunctionModel(_summarize)):
        result, _ = summarize.summarize_and_checkpoint(
            df,
            SUMMARIZER,
            (iceberg_catalog, TABLE),
            max_concurrency=1,
        )

    table_columns = iceberg_catalog.load_table(TABLE).schema().column_names
    assert table_columns == [*old_columns, "prompt_template_hash", "idempotency_key"]
    assert result.columns == table_columns
    stored = _stored_rows(iceberg_catalog)
    assert stored["conversation_ref"].to_list() == ["1", "2", "3"]
    assert stored["idempotency_key"].is_null().to_list() == [False, True, False]


def test_summarize_and_checkpoint_handles_a_table_in_another_column_order(
    iceberg_catalog: SqlCatalog,
) -> None:
    """Every conversation needs a summary, so the skipped frame is empty and keeps
    SUMMARIZE_CHECKPOINT_SCHEMA's order, while the written chunk comes back in the
    table's order. The empty frame must not reach the concat.
    """
    columns = list(reversed(summarize.SUMMARIZE_CHECKPOINT_SCHEMA))
    iceberg_catalog.create_table(
        TABLE,
        schema=pl.DataFrame(
            schema={
                name: summarize.SUMMARIZE_CHECKPOINT_SCHEMA[name] for name in columns
            }
        )
        .to_arrow()
        .schema,
    )
    df = pl.DataFrame([_conversation_row(conversation_ref="1")])

    with override_models(FunctionModel(_summarize)):
        result, _ = summarize.summarize_and_checkpoint(
            df,
            SUMMARIZER,
            (iceberg_catalog, TABLE),
            max_concurrency=1,
        )

    assert result.columns == columns
    assert result["conversation_ref"].to_list() == ["1"]
