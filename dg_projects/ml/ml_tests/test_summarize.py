"""Tests for ml.lib.summarize."""

from typing import Self

import polars as pl
import pytest
from anthropic import Anthropic, AnthropicBedrock
from ml.lib import summarize
from openai import OpenAI


class _FakeSummaryClient:
    model_version = "test-model"

    def summarize(self, conversation_text: str) -> str:
        return f"summary of: {conversation_text}"


def _conversation_row(**overrides: object) -> dict[str, object]:
    conversation_ref = overrides.get("conversation_ref", "1")
    row = {
        # Distinct from conversation_ref -- these tests don't reimplement dbt's
        # surrogate-key hash, just a fake, stable, per-conversation pk.
        "feedback_conversation_pk": f"pk-{conversation_ref}",
        "source_slug": "zendesk",
        "conversation_ref": conversation_ref,
        "turn_count": 2,
        "conversation_text": "turn one\n---\nturn two",
        "conversation_text_chars": 600,
    }
    row.update(overrides)
    return row


def test_needs_summary_skips_single_turn_conversations() -> None:
    row = _conversation_row(turn_count=1, conversation_text_chars=10_000)

    assert summarize.needs_summary(row) is False


def test_needs_summary_skips_short_multi_turn_conversations() -> None:
    row = _conversation_row(conversation_text_chars=499)

    assert summarize.needs_summary(row) is False


def test_needs_summary_summarizes_long_multi_turn_conversations() -> None:
    row = _conversation_row(conversation_text_chars=500)

    assert summarize.needs_summary(row) is True


def test_needs_summary_rejects_null_conversation_text() -> None:
    """conversation_text_chars is pre-redaction length; conversation_text can be
    null (the redaction join isn't wired in upstream yet) even when chars clears
    the threshold. Sending None to the LLM must never happen.
    """
    row = _conversation_row(conversation_text=None, conversation_text_chars=10_000)

    assert summarize.needs_summary(row) is False


def test_summarize_conversations_applies_skip_rule() -> None:
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2", turn_count=1),
        ]
    )

    result = summarize.summarize_conversations(df, _FakeSummaryClient())

    summarized = result.filter(pl.col("conversation_ref") == "1").row(0, named=True)
    assert summarized["conversation_summary"] == "summary of: turn one\n---\nturn two"
    assert summarized["summary_model_version"] == "test-model"
    # "local": no OPIK_URL_OVERRIDE in tests, so get_prompt_version always falls
    # back rather than trying to reach a real Opik instance.
    assert summarized["prompt_version"] == "local"
    assert summarized["embedding_input"] == "summary"
    assert summarized["turn_count"] == 2

    skipped = result.filter(pl.col("conversation_ref") == "2").row(0, named=True)
    assert skipped["conversation_summary"] is None
    assert skipped["summary_model_version"] is None
    assert skipped["prompt_version"] is None
    assert skipped["embedding_input"] == "concatenated_turns"
    assert skipped["turn_count"] == 1


def test_summarize_conversations_types_null_columns_when_batch_is_all_skipped() -> None:
    """An all-skipped batch must not produce a Null-typed column.

    Regression: Polars infers dtype=Null for an all-None Series, which Iceberg
    (format v2) rejects outright when writing the table.
    """
    df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=1)])

    result = summarize.summarize_conversations(df, _FakeSummaryClient())

    assert result.schema["conversation_summary"] == pl.String
    assert result.schema["summary_model_version"] == pl.String
    assert result.schema["prompt_version"] == pl.String


def test_filter_unsummarized_drops_already_summarized_rows_with_same_turn_count() -> (
    None
):
    source_df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1", turn_count=2),
            _conversation_row(conversation_ref="2", turn_count=2),
        ]
    )
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
        }
    )

    result = summarize.filter_unsummarized(source_df, already_summarized_df)

    assert result["conversation_ref"].to_list() == ["2"]


class _FakeLLM:
    """Stands in for LLMClientFactory: building a real one needs real credentials."""

    def __init__(self, client: object, client_class: str = "openai") -> None:
        self._client = client
        self.client_class = client_class

    def get_client(self) -> object:
        return self._client


def test_build_summary_client_uses_default_model_for_anthropic() -> None:
    client = summarize.build_summary_client(
        _FakeLLM(Anthropic(api_key="sk-ant-test"))  # pragma: allowlist secret
    )

    assert isinstance(client, summarize.AnthropicSummaryClient)
    assert client.model_version == summarize.SUMMARY_MODEL_VERSION


def test_build_summary_client_uses_bedrock_default_for_bedrock() -> None:
    """A plain Anthropic API id (SUMMARY_MODEL_VERSION) is never valid on
    Bedrock -- the bedrock client must get BEDROCK_SUMMARY_MODEL_VERSION instead.
    """
    client = summarize.build_summary_client(
        _FakeLLM(AnthropicBedrock(aws_region="us-east-1"))
    )

    assert isinstance(client, summarize.AnthropicSummaryClient)
    assert client.model_version == summarize.BEDROCK_SUMMARY_MODEL_VERSION


def test_build_summary_client_honors_model_version_override_for_openai() -> None:
    """FeedbackSummariesConfig.model_version (passed through as model_version here)
    overrides SUMMARY_MODEL_VERSION -- how a run tries a different model.
    """
    client = summarize.build_summary_client(
        _FakeLLM(OpenAI(api_key="sk-test")),  # pragma: allowlist secret
        model_version="gpt-4o-mini",
    )

    assert isinstance(client, summarize.OpenAISummaryClient)
    assert client.model_version == "gpt-4o-mini"


def test_build_summary_client_honors_model_version_override_for_anthropic() -> None:
    client = summarize.build_summary_client(
        _FakeLLM(Anthropic(api_key="sk-ant-test")),  # pragma: allowlist secret
        model_version="claude-sonnet-5",
    )

    assert isinstance(client, summarize.AnthropicSummaryClient)
    assert client.model_version == "claude-sonnet-5"


def test_build_summary_client_rejects_claude_model_for_real_openai() -> None:
    """client_class="openai" is the real api.openai.com -- it can never serve a
    Claude-namespaced model id, so a left-unset SUMMARY_MODEL_VERSION default is
    caught here rather than failing later with an opaque 404 from OpenAI.
    """
    with pytest.raises(ValueError, match="looks like an Anthropic model id"):
        summarize.build_summary_client(
            _FakeLLM(
                OpenAI(api_key="sk-test"),  # pragma: allowlist secret
                client_class="openai",
            ),
            model_version="claude-haiku-4-5",
        )


def test_build_summary_client_allows_claude_model_for_openai_compatible() -> None:
    """client_class="openai_compatible" is a configurable base_url (e.g. Parley)
    that may legitimately proxy Claude models under this same id -- unlike plain
    "openai", it gets no such guarantee to reject on.
    """
    client = summarize.build_summary_client(
        _FakeLLM(
            OpenAI(
                api_key="sk-test",  # pragma: allowlist secret
                base_url="https://parley.example.com",
            ),
            client_class="openai_compatible",
        ),
        model_version="claude-haiku-4-5",
    )

    assert isinstance(client, summarize.OpenAISummaryClient)
    assert client.model_version == "claude-haiku-4-5"


def test_build_summary_client_honors_bedrock_model_version_override() -> None:
    client = summarize.build_summary_client(
        _FakeLLM(AnthropicBedrock(aws_region="us-east-1")),
        bedrock_model_version="us.anthropic.claude-sonnet-4-5-20250929-v1:0",
    )

    assert isinstance(client, summarize.AnthropicSummaryClient)
    assert client.model_version == "us.anthropic.claude-sonnet-4-5-20250929-v1:0"


class _FakeMessage:
    def __init__(self, content: list[object]) -> None:
        self.content = content
        self.usage = None


class _FakeAnthropicClient:
    """Stands in for the anthropic SDK client: only messages.create is used."""

    def __init__(self, content: list[object]) -> None:
        self._content = content
        self.messages = type(
            "_Messages", (), {"create": lambda _self, **_kwargs: self._response()}
        )()

    def _response(self) -> _FakeMessage:
        return _FakeMessage(self._content)


def test_anthropic_summary_client_treats_empty_content_as_no_summary() -> None:
    """A model with thinking on by default can spend the whole max_tokens budget
    on hidden thinking and return content=[] rather than erroring -- this must
    come back as None (like a refusal), not raise IndexError.
    """
    client = summarize.AnthropicSummaryClient(
        _FakeAnthropicClient(content=[]), "claude-sonnet-5"
    )

    assert client.summarize("some conversation text") is None


class _FakeUsage:
    def __init__(self, input_tokens: int, output_tokens: int) -> None:
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens


class _FakeMessageWithUsage:
    def __init__(self, content: list[object], usage: _FakeUsage) -> None:
        self.content = content
        self.usage = usage


class _FakeContentBlock:
    def __init__(self, text: str) -> None:
        self.text = text


def test_anthropic_summary_client_attaches_usage_to_the_opik_span(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        summarize, "attach_llm_usage", lambda **kwargs: calls.append(kwargs)
    )

    class _Client:
        def __init__(self) -> None:
            self.messages = type(
                "_Messages",
                (),
                {
                    "create": lambda _self, **_kwargs: _FakeMessageWithUsage(
                        [_FakeContentBlock("a summary")], _FakeUsage(10, 5)
                    )
                },
            )()

    client = summarize.AnthropicSummaryClient(_Client(), "claude-haiku-4-5")

    result = client.summarize("some conversation text")

    assert result == "a summary"
    assert calls == [
        {
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15,
            },
            "model": "claude-haiku-4-5",
            "provider": "anthropic",
        }
    ]


def test_filter_unsummarized_resubmits_conversations_with_new_turns() -> None:
    """A ticket that gained a comment since it was summarized is re-submitted."""
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=3)])
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
        }
    )

    result = summarize.filter_unsummarized(source_df, already_summarized_df)

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_resubmits_on_stale_model_version() -> None:
    """A conversation LLM-summarized under an old model/prompt is re-submitted."""
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=2)])
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
            "summary_model_version": ["old-model"],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_model_version="new-model"
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_does_not_resubmit_skipped_rows_on_model_change() -> None:
    """A row skipped last time (null summary_model_version) isn't touched by a
    model change -- the skip decision was never model-dependent.
    """
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=1)])
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [1],
            "summary_model_version": [None],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_model_version="new-model"
    )

    assert result.height == 0


def test_filter_unsummarized_resubmits_on_stale_prompt_version() -> None:
    """A conversation summarized under an old Opik prompt revision is
    re-submitted even though the model id is unchanged.
    """
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=2)])
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [2],
            "prompt_version": ["v1"],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_prompt_version="v2"
    )

    assert result["conversation_ref"].to_list() == ["1"]


def test_filter_unsummarized_does_not_resubmit_skipped_rows_on_prompt_change() -> None:
    """A row skipped last time (null prompt_version) isn't touched by a prompt
    change -- the skip decision was never prompt-dependent.
    """
    source_df = pl.DataFrame([_conversation_row(conversation_ref="1", turn_count=1)])
    already_summarized_df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["pk-1"],
            "source_slug": ["zendesk"],
            "conversation_ref": ["1"],
            "turn_count": [1],
            "prompt_version": [None],
        }
    )

    result = summarize.filter_unsummarized(
        source_df, already_summarized_df, current_prompt_version="v2"
    )

    assert result.height == 0


class _FailingSummaryClient:
    model_version = "test-model"

    def summarize(self, conversation_text: str) -> str:  # noqa: ARG002
        msg = "simulated API failure"
        raise RuntimeError(msg)


def test_summarize_conversations_drops_failed_conversations_without_losing_batch() -> (
    None
):
    """One LLM failure must not cost the rest of the batch (#2542 checkpointing)."""
    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1"),
            _conversation_row(conversation_ref="2"),
        ]
    )

    result = summarize.summarize_conversations(df, _FailingSummaryClient())

    assert result.height == 0


def test_summarize_conversations_keeps_successful_rows_when_one_fails() -> None:
    class _PartiallyFailingClient:
        model_version = "test-model"

        def summarize(self, conversation_text: str) -> str:
            if conversation_text == "fail me":
                msg = "simulated API failure"
                raise RuntimeError(msg)
            return f"summary of: {conversation_text}"

    df = pl.DataFrame(
        [
            _conversation_row(conversation_ref="1", conversation_text="fail me"),
            _conversation_row(conversation_ref="2"),
        ]
    )

    result = summarize.summarize_conversations(df, _PartiallyFailingClient())

    assert result["conversation_ref"].to_list() == ["2"]
    assert result.row(0, named=True)["conversation_summary"] == (
        "summary of: turn one\n---\nturn two"
    )


def test_summarize_conversations_treats_a_none_summary_as_a_failure() -> None:
    """A refusal/content-filter finish (message.content is None) must not be
    recorded as a success -- it would never be retried otherwise.
    """

    class _RefusingClient:
        model_version = "test-model"

        def summarize(self, conversation_text: str) -> str | None:  # noqa: ARG002
            return None

    df = pl.DataFrame([_conversation_row(conversation_ref="1")])

    result = summarize.summarize_conversations(df, _RefusingClient())

    assert result.height == 0


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


class _FakeSchemaUpdate:
    def union_by_name(self, schema: object) -> None:
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeTable:
    def __init__(self) -> None:
        self.upserts: list[dict[str, object]] = []

    def update_schema(self) -> _FakeSchemaUpdate:
        return _FakeSchemaUpdate()

    def upsert(self, **kwargs: object) -> None:
        self.upserts.append(kwargs)


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self._table = table
        self.create_calls: list[str] = []

    def create_table_if_not_exists(
        self,
        identifier: str,
        **kwargs: object,  # noqa: ARG002
    ) -> _FakeTable:
        self.create_calls.append(identifier)
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


def test_summarize_and_checkpoint_upserts_each_chunk() -> None:
    table = _FakeTable()
    catalog = _FakeCatalog(table)
    df = pl.DataFrame([_conversation_row(conversation_ref=str(i)) for i in range(5)])

    result = summarize.summarize_and_checkpoint(
        df,
        _FakeSummaryClient(),
        (catalog, "some_db.feedback_summaries"),
        batch_size=2,
    )

    assert result.height == 5
    # 3 chunks of size 2, 2, 1 -- one upsert call per chunk
    assert len(table.upserts) == 3


def test_summarize_and_checkpoint_aborts_early_on_a_systemic_failure() -> None:
    """A credential-type failure shouldn't burn through every remaining chunk with
    the same error -- a whole chunk with zero successes should abort the run
    instead of trying every chunk.
    """

    class _AlwaysFailingClient:
        model_version = "test-model"

        def summarize(self, conversation_text: str) -> str:  # noqa: ARG002
            msg = "simulated auth failure"
            raise RuntimeError(msg)

    table = _FakeTable()
    catalog = _FakeCatalog(table)
    batch_size = 2
    df = pl.DataFrame([_conversation_row(conversation_ref=str(i)) for i in range(10)])
    errors: list[str] = []

    result = summarize.summarize_and_checkpoint(
        df,
        _AlwaysFailingClient(),
        (catalog, "some_db.feedback_summaries"),
        batch_size=batch_size,
        errors=errors,
    )

    assert result.height == 0
    # Aborted after the first fully-failed chunk, not all 10 rows.
    assert len(errors) == batch_size
    assert len(errors) < df.height
