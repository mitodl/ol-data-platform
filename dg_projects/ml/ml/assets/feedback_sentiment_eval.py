import os
from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    Failure,
    MetadataValue,
    asset,
)
from ml.lib.embed import resolve_embedding_model
from ml.lib.sentiment_eval import (
    build_sentiment_client,
    labeled_sentiment_sample,
    run_sentiment_eval,
)
from ml.resources.llm import LLMClientFactory
from ol_orchestrate.lib.constants import DAGSTER_ENV
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from pydantic import Field

if DAGSTER_ENV == "dev":
    _schema_suffix = os.environ.get("DBT_SCHEMA_SUFFIX")
    intermediate_database_name = (
        f"ol_warehouse_production_{_schema_suffix}_intermediate"
    )
    dimensional_database_name = f"ol_warehouse_production_{_schema_suffix}_dimensional"
else:
    intermediate_database_name = "ol_warehouse_production_intermediate"
    dimensional_database_name = "ol_warehouse_production_dimensional"

SENTIMENT_EVAL_RESULT_SCHEMA = {
    "eval_run_id": pl.String,
    "method": pl.String,
    "accuracy": pl.Float64,
    "negative_precision": pl.Float64,
    "negative_recall": pl.Float64,
    "call_count": pl.Int64,
    "n_train": pl.Int64,
    "n_test": pl.Int64,
    "run_at": pl.Datetime(time_zone="UTC"),
}


class FeedbackSentimentEvalConfig(Config):
    include_llm: bool = Field(
        default=False,
        description=(
            "Run the LLM arm too. Off by default -- it's the only one of the "
            "three methods with a real per-call cost, so it's opt-in rather "
            "than incurred on every eval run."
        ),
    )
    test_fraction: float = Field(
        default=0.2,
        description="Fraction of the labeled sample held out for scoring.",
    )
    embedding_model: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9-]+:.+$",
        description=(
            "Which feedback_embeddings model to evaluate against, in ol-llm's "
            "provider:model form, as feedback_embeddings' embedding_model takes it. "
            "Unset uses whichever model feedback_embeddings' default config writes "
            "(EMBEDDING_MODEL, ml.lib.embed)."
        ),
    )
    embedding_dim: int | None = Field(default=None, description="See above.")
    model_version: str | None = Field(
        default=None,
        description="Override the LLM arm's model id. Ignored if include_llm=False.",
    )
    bedrock_model_version: str | None = Field(
        default=None,
        description="Same as model_version, but for client_class='bedrock'.",
    )


@asset(
    key=AssetKey(["intermediate", "feedback_sentiment_eval"]),
    code_version="feedback_sentiment_eval_v2",
    group_name="feedback",
    io_manager_key="io_manager",
    pool="feedback_sentiment_eval",
    metadata={
        "schema": intermediate_database_name,
        "write_mode": "append",
        "schema_update_mode": "update",
    },
)
def feedback_sentiment_eval(
    context: AssetExecutionContext,
    config: FeedbackSentimentEvalConfig,
    llm: LLMClientFactory,
) -> pl.DataFrame:
    """
    One-time bake-off: explicit+embedding-kNN vs. local-classifier vs. LLM.

    Not a production sentiment pipeline -- a decision aid, scored against the
    explicitly rated sample. Whichever method wins gets its own production
    asset later; this just records the comparison so the choice is auditable.
    """
    embedding_model = resolve_embedding_model(
        config.embedding_model, config.embedding_dim
    )
    # Before the reads, so a missing Opik prompt fails in seconds.
    sentiment_client = None
    if config.include_llm:
        sentiment_client = build_sentiment_client(
            llm, config.model_version, config.bedrock_model_version
        )

    afact_df = (
        get_dbt_model_as_dataframe(
            database_name=dimensional_database_name,
            table_name="afact_feedback_conversation",
        )
        .select(["feedback_conversation_pk", "explicit_rating"])
        .collect()
    )
    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name,
            table_name="feedback_embeddings",
        )
        .filter(embedding_model.matches())
        .select(["feedback_conversation_pk", "embedding_vector"])
        .collect()
    )
    conversation_df = (
        get_dbt_model_as_dataframe(
            database_name=intermediate_database_name,
            table_name="int__feedback__conversation",
        )
        .select(["feedback_conversation_pk", "conversation_text"])
        .collect()
    )

    joined = afact_df.join(
        embeddings_df, on="feedback_conversation_pk", how="inner"
    ).join(conversation_df, on="feedback_conversation_pk", how="left")
    labeled_df = labeled_sentiment_sample(joined)

    if labeled_df.height < 10:  # noqa: PLR2004 -- below this, a train/test split is meaningless
        msg = (
            f"Only {labeled_df.height} explicitly rated conversations have a "
            f"matching embedding (model_version={embedding_model.model_version}, "
            f"dim={embedding_model.dim}); too few for a meaningful eval."
        )
        raise Failure(msg)
    # A total-count check alone doesn't guarantee both classes are represented --
    # an all-positive sample, or one whose few negatives all land in an unstratified
    # test split, leaves LogisticRegression.fit a single-class training set to crash
    # on instead of producing the bake-off (train_test_split_indices stratifies the
    # split, but only once both classes exist here to stratify).
    class_counts = labeled_df["sentiment"].value_counts()
    if class_counts.height < 2:  # noqa: PLR2004
        msg = (
            f"Only {class_counts.height} distinct sentiment label(s) "
            f"({class_counts['sentiment'].to_list()}) among the explicitly rated "
            "sample; need both positive and negative examples for a meaningful eval."
        )
        raise Failure(msg)

    results = run_sentiment_eval(
        labeled_df,
        embedding_dim=embedding_model.dim,
        sentiment_client=sentiment_client,
        test_fraction=config.test_fraction,
    )

    eval_run_id = context.run.root_run_id or context.run.run_id
    run_at = datetime.now(tz=UTC)
    result_rows = [
        {
            "eval_run_id": eval_run_id,
            "method": method,
            "accuracy": metrics["accuracy"],
            "negative_precision": metrics["negative_precision"],
            "negative_recall": metrics["negative_recall"],
            "call_count": metrics["call_count"],
            "n_train": results["n_train"],
            "n_test": results["n_test"],
            "run_at": run_at,
        }
        for method, metrics in results["methods"].items()
    ]

    scores = {
        f"{method}_{metric}": round(metrics[metric], 4)
        for method, metrics in results["methods"].items()
        for metric in ("accuracy", "negative_precision", "negative_recall")
        if metrics[metric] is not None
    }
    context.log.info(
        "Sentiment eval %s (n_train=%d, n_test=%d): %s",
        eval_run_id,
        results["n_train"],
        results["n_test"],
        scores,
    )
    context.add_output_metadata(
        {name: MetadataValue.float(value) for name, value in scores.items()}
        | {
            "n_train": MetadataValue.int(results["n_train"]),
            "n_test": MetadataValue.int(results["n_test"]),
        }
    )
    return pl.DataFrame(result_rows).cast(SENTIMENT_EVAL_RESULT_SCHEMA)
