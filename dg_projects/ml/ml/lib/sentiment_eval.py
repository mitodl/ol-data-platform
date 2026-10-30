"""One-time sentiment-method bake-off: explicit+embedding-kNN vs.
explicit+local-classifier vs. explicit+LLM, scored against the explicitly
rated sample.

Not a production pipeline step -- a decision aid. Run once (or occasionally),
read the accuracy, negative precision/recall and cost, and pick the tier-2
method by hand; the winner then gets its own production asset, same as
clustering/categorization.
"""

import logging
import os
from typing import Any, Protocol

import numpy as np
import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from ml.lib.courseware.extract import Effort
from ml.lib.llm_client_adapters import (
    build_llm_client,
    call_anthropic,
    call_openai,
    message_text,
    raise_if_claude_model_on_openai,
)
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import render_prompt, traced
from openai import OpenAI
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier

# Matches afact_feedback_conversation.sql's sentiment_fk mapping; any other
# explicit_rating value stays unlabeled.
EXPLICIT_RATING_TO_SENTIMENT = {
    "good": "positive",
    "like": "positive",
    "positive": "positive",
    "bad": "negative",
    "dislike": "negative",
    "negative": "negative",
}

SENTIMENT_LABELS = ("positive", "negative")

# How many neighbours the kNN candidate votes across. Small and odd (no ties on
# a binary label) -- this is a bake-off, not the tuned production value.
EMBEDDING_KNN_NEIGHBORS = int(os.environ.get("EMBEDDING_KNN_NEIGHBORS", "5"))

SENTIMENT_MODEL_VERSION = os.environ.get("SENTIMENT_MODEL_VERSION", "claude-haiku-5-5")
BEDROCK_SENTIMENT_MODEL_VERSION = os.environ.get(
    "BEDROCK_SENTIMENT_MODEL_VERSION", "us.anthropic.claude-haiku-5-5"
)
# The answer is one word, but thinking counts toward max_tokens: 16 was enough
# for claude-haiku-4-5, and claude-haiku-5-5 thinks first by default.
SENTIMENT_MAX_TOKENS = int(os.environ.get("SENTIMENT_MAX_TOKENS", "1024"))
SENTIMENT_EFFORT: Effort = "low"

SENTIMENT_PROMPT_NAME = "feedback-sentiment-classify"
CONVERSATION_PLACEHOLDER = "{{conversation_text}}"
SENTIMENT_PROMPT = (
    "Classify the sentiment of this support conversation from the requester's "
    "point of view, as expressed at the end of the conversation. Respond with "
    "only one word: positive or negative.\n\n{{conversation_text}}"
)

logger = logging.getLogger(__name__)


def fetch_sentiment_prompt() -> str:
    """Return the sentiment prompt template, preferring Opik's Prompt Library entry.

    Fetched once per run, before any LLM call, so every conversation is scored
    with one text, and a prompt the run can't use fails the run instead of
    turning each call into a wrong answer.

    :raises PromptLabelMissingError: in Production, when no version of the
        prompt is labeled production.
    :raises ValueError: when the template has no conversation placeholder.
    """
    template = render_prompt(SENTIMENT_PROMPT_NAME, SENTIMENT_PROMPT)
    if CONVERSATION_PLACEHOLDER not in template:
        msg = (
            f"Opik prompt {SENTIMENT_PROMPT_NAME!r} has no "
            f"{CONVERSATION_PLACEHOLDER} placeholder"
        )
        raise ValueError(msg)
    return template


def _render(template: str, conversation_text: str) -> str:
    return template.replace(CONVERSATION_PLACEHOLDER, conversation_text)


def labeled_sentiment_sample(
    conversation_df: pl.DataFrame,
) -> pl.DataFrame:
    """Filter to the explicitly rated rows and map explicit_rating to
    sentiment.

    Args:
        conversation_df: (at least) feedback_conversation_pk, explicit_rating,
            embedding_vector, conversation_text columns.

    Returns:
        The same rows, restricted to the explicit_rating values in
        EXPLICIT_RATING_TO_SENTIMENT, with a new `sentiment` column
        ('positive'/'negative').
    """
    return conversation_df.filter(
        pl.col("explicit_rating").is_in(list(EXPLICIT_RATING_TO_SENTIMENT))
    ).with_columns(
        pl.col("explicit_rating")
        .replace_strict(EXPLICIT_RATING_TO_SENTIMENT)
        .alias("sentiment")
    )


def train_test_split_indices(
    labels: np.ndarray, test_fraction: float = 0.2, random_state: int = 42
) -> tuple[np.ndarray, np.ndarray]:
    """Return (train_idx, test_idx) -- a stratified deterministic holdout split.

    Stratified per label value: a plain shuffle can put every example of the
    minority class in the same partition (real rated samples skew heavily
    positive), leaving LogisticRegression.fit a single-class training set to
    crash on instead of producing the bake-off. A singleton class keeps its
    one example in train rather than losing it to an empty split.
    """
    rng = np.random.default_rng(random_state)
    train_idx: list[np.ndarray] = []
    test_idx: list[np.ndarray] = []
    for label in np.unique(labels):
        shuffled = rng.permutation(np.flatnonzero(labels == label))
        n_test = (
            max(1, round(len(shuffled) * test_fraction)) if len(shuffled) > 1 else 0
        )
        train_idx.append(shuffled[n_test:])
        test_idx.append(shuffled[:n_test])
    return np.concatenate(train_idx), np.concatenate(test_idx)


def score_predictions(
    predictions: list[str | None] | np.ndarray, test_labels: np.ndarray
) -> dict[str, float | None]:
    """Accuracy, plus precision and recall on the negative class.

    Rated conversations are mostly positive, so an all-positive guess already
    scores high accuracy; negative precision/recall show how well a method
    finds the complaints. Each is None when its denominator is zero.
    A None prediction (a failed LLM call) is wrong and is never a negative.
    """
    predicted = np.array(predictions, dtype=object)
    actual = np.asarray(test_labels, dtype=object)
    predicted_negative = predicted == "negative"
    actual_negative = actual == "negative"
    true_negative = int((predicted_negative & actual_negative).sum())
    n_predicted_negative = int(predicted_negative.sum())
    n_actual_negative = int(actual_negative.sum())
    return {
        "accuracy": float((predicted == actual).mean()),
        "negative_precision": (
            true_negative / n_predicted_negative if n_predicted_negative else None
        ),
        "negative_recall": (
            true_negative / n_actual_negative if n_actual_negative else None
        ),
    }


def embedding_knn_predictions(
    train_vectors: np.ndarray,
    train_labels: np.ndarray,
    test_vectors: np.ndarray,
    n_neighbors: int = EMBEDDING_KNN_NEIGHBORS,
) -> np.ndarray:
    """Predictions of a cosine-distance kNN vote over already-computed embeddings.

    Zero extra model cost (§E) -- reuses the one embedding already computed for
    clustering, just a nearest-neighbour lookup against the labeled sample.
    """
    classifier = KNeighborsClassifier(
        n_neighbors=min(n_neighbors, len(train_labels)), metric="cosine"
    )
    classifier.fit(train_vectors, train_labels)
    return classifier.predict(test_vectors)


def local_classifier_predictions(
    train_vectors: np.ndarray,
    train_labels: np.ndarray,
    test_vectors: np.ndarray,
    random_state: int = 42,
) -> np.ndarray:
    """Predictions of a logistic regression trained on the labeled embeddings.

    CPU-cheap, no per-record API cost (§E's local-classifier option) -- a
    simple linear model over the same embeddings kNN uses, rather than a
    separate text model.
    """
    classifier = LogisticRegression(max_iter=1000, random_state=random_state)
    classifier.fit(train_vectors, train_labels)
    return classifier.predict(test_vectors)


class SentimentClient(Protocol):
    model_version: str

    def classify(self, conversation_text: str) -> str | None: ...


def _extract_sentiment_word(text: str) -> str | None:
    lowered = text.strip().lower()
    for label in SENTIMENT_LABELS:
        if label in lowered:
            return label
    return None


class AnthropicSentimentClient:
    """Adapts an Anthropic-compatible client (incl. AnthropicBedrock) to
    SentimentClient.
    """

    def __init__(
        self, client: Anthropic | AnthropicBedrock, model_version: str
    ) -> None:
        self._client = client
        self.model_version = model_version
        self.prompt_template = SENTIMENT_PROMPT

    @traced("feedback_sentiment_classify_anthropic", tags=["feedback_sentiment"])
    def classify(self, conversation_text: str) -> str | None:
        message = call_anthropic(
            self._client,
            self.model_version,
            max_tokens=SENTIMENT_MAX_TOKENS,
            effort=SENTIMENT_EFFORT,
            prompt=_render(self.prompt_template, conversation_text),
        )
        if (text := message_text(message)) is None:
            return None
        return _extract_sentiment_word(text)


class OpenAISentimentClient:
    """Adapts an OpenAI-compatible client to SentimentClient."""

    def __init__(
        self, client: OpenAI, model_version: str, *, client_class: str = "openai"
    ) -> None:
        raise_if_claude_model_on_openai(
            client_class=client_class,
            model_version=model_version,
            config_hint=(
                "FeedbackSentimentEvalConfig.model_version (or SENTIMENT_MODEL_VERSION)"
            ),
        )
        self._client = client
        self.model_version = model_version
        self.prompt_template = SENTIMENT_PROMPT

    @traced("feedback_sentiment_classify_openai", tags=["feedback_sentiment"])
    def classify(self, conversation_text: str) -> str | None:
        response = call_openai(
            self._client,
            self.model_version,
            prompt=_render(self.prompt_template, conversation_text),
        )
        content = response.choices[0].message.content
        if not content:
            return None
        return _extract_sentiment_word(content)


def build_sentiment_client(
    llm: LLMClientFactory,
    model_version: str | None = None,
    bedrock_model_version: str | None = None,
) -> AnthropicSentimentClient | OpenAISentimentClient:
    """Build the client whose model_version comes from run config, else a default.

    Bedrock's model id namespace differs from the plain Anthropic/OpenAI one,
    hence the model_version/bedrock_model_version split.
    """
    client = build_llm_client(
        llm,
        anthropic_client_cls=AnthropicSentimentClient,
        openai_client_cls=OpenAISentimentClient,
        model_version=model_version,
        bedrock_model_version=bedrock_model_version,
        default_model_version=SENTIMENT_MODEL_VERSION,
        default_bedrock_model_version=BEDROCK_SENTIMENT_MODEL_VERSION,
    )
    client.prompt_template = fetch_sentiment_prompt()
    return client


def llm_classifier_predictions(
    client: SentimentClient,
    test_texts: list[str],
) -> list[str | None]:
    """Predictions of one LLM call per test conversation; None where a call failed.

    The call count (one per text) is the actual cost driver callers should weigh
    against the other two methods' near-zero marginal cost, per §E's
    accuracy-vs-cost framing.
    """
    predictions: list[str | None] = []
    for text in test_texts:
        try:
            predictions.append(client.classify(text))
        except Exception:
            logger.warning(
                "LLM sentiment call failed for one conversation", exc_info=True
            )
            predictions.append(None)
    return predictions


def run_sentiment_eval(  # noqa: PLR0913 -- one independently meaningful eval knob per arg
    labeled_df: pl.DataFrame,
    embedding_dim: int,
    sentiment_client: SentimentClient | None,
    test_fraction: float = 0.2,
    n_neighbors: int = EMBEDDING_KNN_NEIGHBORS,
    random_state: int = 42,
) -> dict[str, Any]:
    """Run the three-way bake-off on one labeled sample; return per-method results.

    Args:
        labeled_df: output of labeled_sentiment_sample -- needs
            embedding_vector, sentiment, conversation_text columns.
        embedding_dim: the embedding_vector's fixed length (for the array cast).
        sentiment_client: an LLM sentiment client, or None to skip that arm
            (it's the only one with a real per-call cost -- worth being able to
            run the other two without incurring it).
    """
    vectors = labeled_df["embedding_vector"].list.to_array(embedding_dim).to_numpy()
    labels = labeled_df["sentiment"].to_numpy()
    texts = labeled_df["conversation_text"].to_list()
    train_idx, test_idx = train_test_split_indices(
        labels, test_fraction=test_fraction, random_state=random_state
    )

    test_labels = labels[test_idx]
    methods: dict[str, dict[str, Any]] = {
        "embedding_knn": score_predictions(
            embedding_knn_predictions(
                vectors[train_idx],
                labels[train_idx],
                vectors[test_idx],
                n_neighbors=n_neighbors,
            ),
            test_labels,
        )
        | {"call_count": 0},
        "local_classifier": score_predictions(
            local_classifier_predictions(
                vectors[train_idx],
                labels[train_idx],
                vectors[test_idx],
                random_state=random_state,
            ),
            test_labels,
        )
        | {"call_count": 0},
    }
    if sentiment_client is not None:
        test_texts = [texts[i] for i in test_idx]
        methods["llm"] = score_predictions(
            llm_classifier_predictions(sentiment_client, test_texts), test_labels
        ) | {"call_count": len(test_texts)}

    return {"methods": methods, "n_train": len(train_idx), "n_test": len(test_idx)}
