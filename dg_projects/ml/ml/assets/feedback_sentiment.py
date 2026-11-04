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
from ml.lib.cluster import DEFAULT_PLATFORMS
from ml.lib.embed import resolve_embedding_model
from ml.lib.sentiment_eval import (
    labeled_sentiment_sample,
    predict_unrated_sentiment,
    validate_negative_thresholds,
)
from ml.lib.warehouse import warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from pydantic import Field, field_validator

database_name = warehouse_database("intermediate")

# Repeated 5-fold CV on the rated conversations: precision 0.83 (0.77-0.89 across
# folds), recall 0.33. Lower thresholds lose much more precision than they gain recall.
PRODUCTION_NEGATIVE_THRESHOLD = 0.35

# Every training label is Zendesk or content feedback, so tutor and forum course
# questions are out of distribution: about half came out negative, mostly wrongly.
PREDICTED_SOURCES = ("zendesk",)
# A content-feedback idea is a suggestion, not a verdict, so it gets no sentiment.
NO_SENTIMENT_RATINGS = ("idea",)
# The Appzi widget's suggestion tickets, the Zendesk form of the same idea. An idea
# sent with no answer is a bare "Idea" (appzi_feedback_text in macros/appzi.sql),
# followed by the turn separator from int__feedback__conversation if a reply came.
NO_SENTIMENT_TEXT_PATTERN = r"^Idea(?::|\s*(?:$|\n---\n))"

SENTIMENT_ASSIGNMENT_SCHEMA = {
    "feedback_conversation_pk": pl.String,
    "sentiment_slug": pl.String,
    "negative_probability": pl.Float64,
    "sentiment_model_version": pl.String,
    "embedding_model_version": pl.String,
    "predicted_at": pl.Datetime(time_zone="UTC"),
}


class FeedbackSentimentConfig(Config):
    negative_threshold: float = Field(
        default=PRODUCTION_NEGATIVE_THRESHOLD,
        description=(
            "A conversation is negative when the classifier's P(negative) is at "
            "least this. feedback_sentiment_eval scores the candidates."
        ),
    )

    @field_validator("negative_threshold")
    @classmethod
    def threshold_between_zero_and_one(cls, threshold: float) -> float:
        """Reject a bad threshold at launch, before the warehouse reads."""
        validate_negative_thresholds([threshold])
        return threshold

    sources: list[str] = Field(
        default=list(PREDICTED_SOURCES),
        description=(
            "The source_slugs to predict for. Training still uses every rated "
            "conversation. Add a source only after scoring the model on it."
        ),
    )
    platforms: list[str] | None = Field(
        default=DEFAULT_PLATFORMS,
        description=(
            "The platforms to predict for, the same default as the summaries and "
            "clusters (ml.lib.cluster). Null predicts every platform."
        ),
    )
    embedding_model: str | None = Field(
        default=None,
        pattern=r"^[a-z0-9-]+:.+$",
        description=(
            "Which feedback_embeddings model to train and predict on, in ol-llm's "
            "provider:model form. Unset uses EMBEDDING_MODEL (ml.lib.embed)."
        ),
    )
    embedding_dim: int | None = Field(default=None, description="See above.")


@asset(
    key=AssetKey(["intermediate", "feedback_sentiment_assignments"]),
    code_version="feedback_sentiment_v1",
    group_name="feedback",
    deps=[
        AssetKey(["intermediate", "feedback_embeddings"]),
        AssetKey(["intermediate", "int__feedback__conversation"]),
    ],
    automation_condition=upstream_or_code_changes(),
    io_manager_key="io_manager",
    pool="feedback_sentiment",
    metadata={
        "schema": database_name,
        # Overwrite so a conversation that leaves the scope (a platform change)
        # loses its prediction; an upsert would keep it in afact forever.
        "write_mode": "overwrite",
        "schema_update_mode": "update",
    },
)
def feedback_sentiment(
    context: AssetExecutionContext, config: FeedbackSentimentConfig
) -> pl.DataFrame:
    """
    Predict sentiment for embedded conversations without an explicit verdict.

    Tier 2 of the sentiment ladder (feedback_ml_approach.md §E): a logistic
    regression on the embeddings, trained on the explicitly rated conversations.
    Each run retrains on every rated row and overwrites the table, so it holds
    exactly the conversations in scope now. afact_feedback_conversation uses the
    result only where it has no verdict.
    Only config.sources on config.platforms are predicted, and never an idea.
    """
    embedding_model = resolve_embedding_model(
        config.embedding_model, config.embedding_dim
    )
    conversation_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="int__feedback__conversation"
        )
        .select(
            "feedback_conversation_pk",
            "source_slug",
            "platform",
            "explicit_rating",
            pl.col("conversation_text")
            .str.contains(NO_SENTIMENT_TEXT_PATTERN)
            .fill_null(value=False)
            .alias("is_idea_text"),
        )
        .collect()
    )
    labeled_df = labeled_sentiment_sample(conversation_df)
    unrated_df = conversation_df.join(
        labeled_df, on="feedback_conversation_pk", how="anti"
    ).filter(
        pl.col("source_slug").is_in(config.sources)
        & (
            pl.col("explicit_rating").is_null()
            | ~pl.col("explicit_rating").is_in(NO_SENTIMENT_RATINGS)
        )
        & ~pl.col("is_idea_text")
    )
    if config.platforms is not None:
        unrated_df = unrated_df.filter(pl.col("platform").is_in(config.platforms))
    # Read only the vectors these rows need: every vector at once is several GB.
    needed_pks = pl.concat(
        [labeled_df["feedback_conversation_pk"], unrated_df["feedback_conversation_pk"]]
    )
    embeddings_df = (
        get_dbt_model_as_dataframe(
            database_name=database_name, table_name="feedback_embeddings"
        )
        .filter(
            embedding_model.matches()
            & pl.col("feedback_conversation_pk").is_in(needed_pks.to_list())
        )
        .select(["feedback_conversation_pk", "embedding_vector"])
        .collect()
    )
    labeled_df = labeled_df.join(
        embeddings_df, on="feedback_conversation_pk", how="inner"
    )
    unrated_df = unrated_df.join(
        embeddings_df, on="feedback_conversation_pk", how="inner"
    )

    class_counts = labeled_df["sentiment"].value_counts()
    if class_counts.height < 2:  # noqa: PLR2004
        msg = (
            f"Rated conversations with a {embedding_model.model_version} embedding "
            f"have labels {class_counts['sentiment'].to_list()}; the classifier "
            "needs both positive and negative examples."
        )
        raise Failure(msg)

    negative_probabilities, sentiments = predict_unrated_sentiment(
        labeled_df,
        unrated_df,
        embedding_dim=embedding_model.dim,
        negative_threshold=config.negative_threshold,
    )
    result = pl.DataFrame(
        {
            "feedback_conversation_pk": unrated_df["feedback_conversation_pk"],
            "sentiment_slug": sentiments,
            "negative_probability": negative_probabilities,
            "sentiment_model_version": (
                f"local_classifier@{config.negative_threshold!r}"
            ),
            "embedding_model_version": embedding_model.model_version,
            "predicted_at": datetime.now(tz=UTC),
        }
    ).cast(SENTIMENT_ASSIGNMENT_SCHEMA)

    n_negative = int((result["sentiment_slug"] == "negative").sum())
    context.log.info(
        "Trained on %d rated conversations; predicted %d (%d negative)",
        labeled_df.height,
        result.height,
        n_negative,
    )
    context.add_output_metadata(
        {
            "n_train": MetadataValue.int(labeled_df.height),
            "n_predicted": MetadataValue.int(result.height),
            "n_predicted_negative": MetadataValue.int(n_negative),
            "negative_threshold": MetadataValue.float(config.negative_threshold),
        }
    )
    return result
