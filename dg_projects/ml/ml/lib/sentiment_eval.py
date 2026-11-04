"""One-time sentiment-method bake-off: explicit+embedding-kNN vs.
explicit+local-classifier vs. explicit+LLM, scored against the explicitly
rated sample.

Not a production pipeline step -- a decision aid. Run once (or occasionally),
read the accuracy, negative precision/recall and cost, and pick the tier-2
method by hand; the winner then gets its own production asset, same as
clustering/categorization.
"""

import os
import time
from collections.abc import Sequence
from typing import Any, Protocol

import numpy as np
import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from dagster import get_dagster_logger
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

# The classifier labels a conversation negative when P(negative) >= threshold. Rows
# below 0.5 trade precision for recall; 0.5 is the plain predict() decision.
DEFAULT_NEGATIVE_THRESHOLD = 0.5
CLASSIFIER_NEGATIVE_THRESHOLDS = (0.5, 0.4, 0.35, 0.3, 0.25, 0.2)

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
# The calls run one at a time, so a run is silent for a long while without this.
LLM_PROGRESS_EVERY = 100

SENTIMENT_PROMPT_NAME = "feedback-sentiment-classify"
CONVERSATION_PLACEHOLDER = "{{conversation_text}}"
SENTIMENT_PROMPT = (
    "Classify the sentiment of this support conversation from the requester's "
    "point of view, as expressed at the end of the conversation. Respond with "
    "only one word: positive or negative.\n\n{{conversation_text}}"
)


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
) -> dict[str, float | int | None]:
    """Accuracy, plus precision and recall on the negative class, with their counts.

    Rated conversations are mostly positive, so an all-positive guess already
    scores high accuracy; negative precision/recall show how well a method
    finds the complaints. Each is None when its denominator is zero.
    A None prediction (a failed LLM call) is wrong and is never a negative.
    """
    predicted = np.array(predictions, dtype=object)
    actual = np.asarray(test_labels, dtype=object)
    predicted_negative = predicted == "negative"
    actual_negative = actual == "negative"
    n_true_negative = int((predicted_negative & actual_negative).sum())
    n_predicted_negative = int(predicted_negative.sum())
    n_test_negative = int(actual_negative.sum())
    return {
        "accuracy": float((predicted == actual).mean()),
        "negative_precision": (
            n_true_negative / n_predicted_negative if n_predicted_negative else None
        ),
        "negative_recall": (
            n_true_negative / n_test_negative if n_test_negative else None
        ),
        "n_test_negative": n_test_negative,
        "n_predicted_negative": n_predicted_negative,
        "n_true_negative": n_true_negative,
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


def local_classifier_negative_probabilities(
    train_vectors: np.ndarray,
    train_labels: np.ndarray,
    test_vectors: np.ndarray,
    random_state: int = 42,
) -> np.ndarray:
    """P(negative) per test row from a logistic regression on the labeled embeddings.

    CPU-cheap, no per-record API cost (§E's local-classifier option) -- a
    simple linear model over the same embeddings kNN uses, rather than a
    separate text model.
    """
    classifier = LogisticRegression(max_iter=1000, random_state=random_state)
    classifier.fit(train_vectors, train_labels)
    negative_column = list(classifier.classes_).index("negative")
    return classifier.predict_proba(test_vectors)[:, negative_column]


def predictions_at_threshold(
    negative_probabilities: np.ndarray, threshold: float
) -> np.ndarray:
    """Label each row negative when its P(negative) is at least threshold."""
    return np.where(negative_probabilities >= threshold, "negative", "positive")


def predict_unrated_sentiment(
    labeled_df: pl.DataFrame,
    unrated_df: pl.DataFrame,
    embedding_dim: int,
    negative_threshold: float,
) -> tuple[np.ndarray, np.ndarray]:
    """Train the local classifier on every labeled row and score the unrated ones.

    The production use of the classifier feedback_sentiment_eval scores, so the
    two stay the same model.

    Args:
        labeled_df: output of labeled_sentiment_sample -- needs embedding_vector
            and sentiment columns, with both labels present.
        unrated_df: rows to predict -- needs embedding_vector.

    Returns:
        (P(negative), sentiment label) per unrated_df row, in its order.
    """
    if unrated_df.is_empty():
        return np.empty(0), np.empty(0, dtype=object)
    negative_probabilities = local_classifier_negative_probabilities(
        labeled_df["embedding_vector"].list.to_array(embedding_dim).to_numpy(),
        labeled_df["sentiment"].to_numpy(),
        unrated_df["embedding_vector"].list.to_array(embedding_dim).to_numpy(),
    )
    return negative_probabilities, predictions_at_threshold(
        negative_probabilities, negative_threshold
    )


def validate_negative_thresholds(thresholds: Sequence[float]) -> None:
    """Raise ValueError unless every threshold is between 0 and 1."""
    if not all(0 < threshold < 1 for threshold in thresholds):
        msg = f"Thresholds must be between 0 and 1: {list(thresholds)}"
        raise ValueError(msg)


def classifier_method_name(threshold: float) -> str:
    if threshold == DEFAULT_NEGATIVE_THRESHOLD:
        return "local_classifier"
    # repr is the shortest exact form, so distinct thresholds never share a name.
    return f"local_classifier@{threshold!r}"


class SentimentClient(Protocol):
    model_version: str

    def classify(self, conversation_text: str) -> str | None: ...


def _extract_sentiment_word(text: str) -> str | None:
    lowered = text.strip().lower()
    found = [label for label in SENTIMENT_LABELS if label in lowered]
    # A word match can't tell which word is the answer when the reply has both.
    return found[0] if len(found) == 1 else None


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
    log = get_dagster_logger()
    predictions: list[str | None] = []
    traceback_logged = False
    start = time.monotonic()
    for done, text in enumerate(test_texts, start=1):
        try:
            predictions.append(client.classify(text))
        except Exception:
            # Only the first: the progress line counts the rest, and an outage
            # would otherwise log one warning per call.
            if not traceback_logged:
                log.warning("LLM sentiment call failed", exc_info=True)
                traceback_logged = True
            predictions.append(None)
        if done % LLM_PROGRESS_EVERY == 0 or done == len(test_texts):
            log.info(
                "LLM sentiment %d/%d (%.0fs, %d without an answer)",
                done,
                len(test_texts),
                time.monotonic() - start,
                predictions.count(None),
            )
    return predictions


def _log_scores(method: str, scores: dict[str, Any]) -> None:
    get_dagster_logger().info(
        "%s: accuracy %.4f, found %d of %d negatives (%d predicted negative)",
        method,
        scores["accuracy"],
        scores["n_true_negative"],
        scores["n_test_negative"],
        scores["n_predicted_negative"],
    )


def run_sentiment_eval(  # noqa: PLR0913 -- one independently meaningful eval knob per arg
    labeled_df: pl.DataFrame,
    embedding_dim: int,
    sentiment_client: SentimentClient | None,
    test_fraction: float = 0.2,
    n_neighbors: int = EMBEDDING_KNN_NEIGHBORS,
    random_state: int = 42,
    classifier_negative_thresholds: Sequence[float] = CLASSIFIER_NEGATIVE_THRESHOLDS,
) -> dict[str, Any]:
    """Run the three-way bake-off on one labeled sample; return per-method results.

    Args:
        labeled_df: output of labeled_sentiment_sample -- needs
            embedding_vector, sentiment, conversation_text columns.
        embedding_dim: the embedding_vector's fixed length (for the array cast).
        sentiment_client: an LLM sentiment client, or None to skip that arm
            (it's the only one with a real per-call cost -- worth being able to
            run the other two without incurring it).
        classifier_negative_thresholds: one local_classifier result per value;
            each must be between 0 and 1. DEFAULT_NEGATIVE_THRESHOLD is always
            scored too, so every run has a local_classifier row to compare.

    :raises ValueError: when a threshold is not between 0 and 1.
    """
    validate_negative_thresholds(classifier_negative_thresholds)
    vectors = labeled_df["embedding_vector"].list.to_array(embedding_dim).to_numpy()
    labels = labeled_df["sentiment"].to_numpy()
    texts = labeled_df["conversation_text"].to_list()
    train_idx, test_idx = train_test_split_indices(
        labels, test_fraction=test_fraction, random_state=random_state
    )

    log = get_dagster_logger()
    test_labels = labels[test_idx]
    log.info(
        "Split %d train / %d test, %d negative in test",
        len(train_idx),
        len(test_idx),
        int((test_labels == "negative").sum()),
    )
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
    }
    negative_probabilities = local_classifier_negative_probabilities(
        vectors[train_idx],
        labels[train_idx],
        vectors[test_idx],
        random_state=random_state,
    )
    thresholds = set(classifier_negative_thresholds) | {DEFAULT_NEGATIVE_THRESHOLD}
    for threshold in sorted(thresholds, reverse=True):
        methods[classifier_method_name(threshold)] = score_predictions(
            predictions_at_threshold(negative_probabilities, threshold), test_labels
        ) | {"call_count": 0}
    for method, scores in methods.items():
        _log_scores(method, scores)
    if sentiment_client is not None:
        test_texts = [texts[i] for i in test_idx]
        methods["llm"] = score_predictions(
            llm_classifier_predictions(sentiment_client, test_texts), test_labels
        ) | {"call_count": len(test_texts)}
        _log_scores("llm", methods["llm"])

    return {"methods": methods, "n_train": len(train_idx), "n_test": len(test_idx)}
