import contextlib
import os

import polars as pl
from anthropic import Anthropic
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    asset,
)
from ml.lib.summarize import (
    JOIN_COLS,
    SUMMARY_MODEL_VERSION,
    SUMMARY_PROMPT,
    filter_unsummarized,
    summarize_conversations,
)
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import (
    get_dbt_model_as_dataframe,
)
from openai import OpenAI
from pydantic import Field
from pyiceberg.exceptions import NoSuchTableError

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    database_name = f"ol_warehouse_production_{_schema_suffix}_intermediate"
else:
    database_name = "ol_warehouse_production_intermediate"


class _AnthropicSummaryClient:
    """Adapts an Anthropic client to the summarize.SummaryClient protocol."""

    def __init__(self, client: Anthropic) -> None:
        self._client = client

    def summarize(self, conversation_text: str) -> str:
        message = self._client.messages.create(
            model=SUMMARY_MODEL_VERSION,
            max_tokens=200,
            messages=[
                {
                    "role": "user",
                    "content": SUMMARY_PROMPT.format(
                        conversation_text=conversation_text
                    ),
                }
            ],
        )
        return message.content[0].text


class _OpenAISummaryClient:
    """Adapts an OpenAI-compatible client to the summarize.SummaryClient protocol."""

    def __init__(self, client: OpenAI, model: str) -> None:
        self._client = client
        self._model = model

    def summarize(self, conversation_text: str) -> str:
        response = self._client.chat.completions.create(
            model=self._model,
            messages=[
                {
                    "role": "user",
                    "content": SUMMARY_PROMPT.format(
                        conversation_text=conversation_text
                    ),
                }
            ],
        )
        return response.choices[0].message.content


def _build_summary_client(
    llm: LLMClientFactory,
) -> _AnthropicSummaryClient | _OpenAISummaryClient:
    client = llm.get_client()
    if isinstance(client, Anthropic):
        return _AnthropicSummaryClient(client)
    return _OpenAISummaryClient(client, model=SUMMARY_MODEL_VERSION)


class FeedbackSummariesConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description="Re-summarize every eligible conversation, not only new ones.",
    )
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of upstream conversations read, for local tests.",
    )


@asset(
    code_version="feedback_summaries_v1",
    group_name="feedback",
    key=AssetKey(["intermediate", "feedback_summaries"]),
    deps=[AssetKey(["intermediate", "int__feedback__conversation"])],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_summaries",
    metadata={
        "schema": database_name,
        "write_mode": "upsert",
        "upsert_options": {"join_cols": JOIN_COLS},
    },
)
def feedback_summaries(
    context: AssetExecutionContext,
    config: FeedbackSummariesConfig,
    llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    Summarize multi-turn conversations via LLM; skip single-turn/short ones (§A.1).

    The one per-record LLM call in the design - see feedback_ml_approach.md §A.1 for
    the skip threshold and measured cost.
    """
    source_lazy = get_dbt_model_as_dataframe(
        database_name=database_name,
        table_name="int__feedback__conversation",
    )
    if config.sample_limit is not None:
        source_lazy = source_lazy.limit(config.sample_limit)
    source_df = source_lazy.collect()

    already_summarized_df = pl.DataFrame(schema=dict.fromkeys(JOIN_COLS, pl.String))
    if not config.full_refresh:
        with contextlib.suppress(NoSuchTableError):
            already_summarized_df = (
                get_dbt_model_as_dataframe(
                    database_name=database_name,
                    table_name="feedback_summaries",
                )
                .select(JOIN_COLS)
                .collect()
            )

    unsummarized_df = filter_unsummarized(source_df, already_summarized_df)
    client = _build_summary_client(llm)
    summaries_df = summarize_conversations(unsummarized_df, client)

    context.log.info(
        "Summarized %d new conversations (%d already summarized, %d total upstream)",
        summaries_df.height,
        already_summarized_df.height,
        source_df.height,
    )

    return summaries_df
