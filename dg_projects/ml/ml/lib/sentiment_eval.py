"""One-time sentiment-method bake-off: explicit+embedding-kNN vs.
explicit+local-classifier vs. explicit+LLM, scored against the Zendesk-CSAT
labeled sample.

Not a production pipeline step -- a decision aid. Run once (or occasionally),
read the accuracy/cost tradeoff, and pick the tier-2 method by hand; the
winner then gets its own production asset, same as clustering/categorization.
"""

import logging
import os
from typing import Any, Protocol

import numpy as np
import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import render_prompt, traced
from openai import OpenAI
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier

# Matches afact_feedback_conversation.sql's mapping of
# ticket_satisfaction_rating_score -- only 'good'/'bad' are verdicts, everything
# else stays unlabeled.
EXPLICIT_RATING_TO_SENTIMENT = {"good": "positive", "bad": "negative"}

SENTIMENT_LABELS = ("positive", "negative")

# How many neighbours the kNN candidate votes across. Small and odd (no ties on
# a binary label) -- this is a bake-off, not the tuned production value.
EMBEDDING_KNN_NEIGHBORS = int(os.environ.get("EMBEDDING_KNN_NEIGHBORS", "5"))

SENTIMENT_MODEL_VERSION = os.environ.get("SENTIMENT_MODEL_VERSION", "claude-haiku-4-5")
BEDROCK_SENTIMENT_MODEL_VERSION = os.environ.get(
    "BEDROCK_SENTIMENT_MODEL_VERSION",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)
SENTIMENT_MAX_TOKENS = int(os.environ.get("SENTIMENT_MAX_TOKENS", "16"))

SENTIMENT_PROMPT = (
    "Classify the sentiment of this support conversation from the requester's "
    "point of view, as expressed at the end of the conversation. Respond with "
    "only one word: positive or negative.\n\n{{conversation_text}}"
)

logger = logging.getLogger(__name__)


def _sentiment_prompt(conversation_text: str) -> str:
    """SENTIMENT_PROMPT rendered, preferring Opik's Prompt Library entry if set up."""
    return render_prompt(
        "feedback-sentiment-classify",
        SENTIMENT_PROMPT,
        conversation_text=conversation_text,
    )


def labeled_sentiment_sample(
    conversation_df: pl.DataFrame,
) -> pl.DataFrame:
    """Filter to the Zendesk-CSAT-labeled rows and map explicit_rating to
    sentiment.

    Args:
        conversation_df: (at least) feedback_conversation_pk, explicit_rating,
            embedding_vector, conversation_text columns.

    Returns:
        The same rows, restricted to explicit_rating in ('good', 'bad'), with a
        new `sentiment` column ('positive'/'negative').
    """
    return conversation_df.filter(
        pl.col("explicit_rating").is_in(list(EXPLICIT_RATING_TO_SENTIMENT))
    ).with_columns(
        pl.col("explicit_rating")
        .replace_strict(EXPLICIT_RATING_TO_SENTIMENT)
        .alias("sentiment")
    )


def train_test_split_indices(
    n: int, test_fraction: float = 0.2, random_state: int = 42
) -> tuple[np.ndarray, np.ndarray]:
    """Return (train_idx, test_idx) -- a simple deterministic holdout split.

    Not stratified: the labeled sample is a bake-off input the caller controls,
    not a production training set, so a plain shuffle is enough to compare the
    three methods on the same split.
    """
    rng = np.random.default_rng(random_state)
    indices = rng.permutation(n)
    n_test = max(1, round(n * test_fraction))
    return indices[n_test:], indices[:n_test]


def embedding_knn_accuracy(
    train_vectors: np.ndarray,
    train_labels: np.ndarray,
    test_vectors: np.ndarray,
    test_labels: np.ndarray,
    n_neighbors: int = EMBEDDING_KNN_NEIGHBORS,
) -> float:
    """Accuracy of a cosine-distance kNN vote over already-computed embeddings.

    Zero extra model cost (§E) -- reuses the one embedding already computed for
    clustering, just a nearest-neighbour lookup against the labeled sample.
    """
    classifier = KNeighborsClassifier(
        n_neighbors=min(n_neighbors, len(train_labels)), metric="cosine"
    )
    classifier.fit(train_vectors, train_labels)
    predictions = classifier.predict(test_vectors)
    return float((predictions == test_labels).mean())


def local_classifier_accuracy(
    train_vectors: np.ndarray,
    train_labels: np.ndarray,
    test_vectors: np.ndarray,
    test_labels: np.ndarray,
    random_state: int = 42,
) -> float:
    """Accuracy of a logistic regression trained on the labeled embeddings.

    CPU-cheap, no per-record API cost (§E's local-classifier option) -- a
    simple linear model over the same embeddings kNN uses, rather than a
    separate text model.
    """
    classifier = LogisticRegression(max_iter=1000, random_state=random_state)
    classifier.fit(train_vectors, train_labels)
    predictions = classifier.predict(test_vectors)
    return float((predictions == test_labels).mean())


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

    @traced("feedback_sentiment_classify_anthropic")
    def classify(self, conversation_text: str) -> str | None:
        message = self._client.messages.create(
            model=self.model_version,
            max_tokens=SENTIMENT_MAX_TOKENS,
            messages=[
                {
                    "role": "user",
                    "content": _sentiment_prompt(conversation_text),
                }
            ],
        )
        if not message.content:
            return None
        return _extract_sentiment_word(message.content[0].text)


class OpenAISentimentClient:
    """Adapts an OpenAI-compatible client to SentimentClient."""

    def __init__(self, client: OpenAI, model_version: str) -> None:
        self._client = client
        self.model_version = model_version

    @traced("feedback_sentiment_classify_openai")
    def classify(self, conversation_text: str) -> str | None:
        response = self._client.chat.completions.create(
            model=self.model_version,
            messages=[
                {
                    "role": "user",
                    "content": _sentiment_prompt(conversation_text),
                }
            ],
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

    Mirrors ml.lib.summarize.build_summary_client's dispatch and
    model_version/bedrock_model_version split.
    """
    client = llm.get_client()
    if isinstance(client, AnthropicBedrock):
        return AnthropicSentimentClient(
            client, bedrock_model_version or BEDROCK_SENTIMENT_MODEL_VERSION
        )
    if isinstance(client, Anthropic):
        return AnthropicSentimentClient(
            client, model_version or SENTIMENT_MODEL_VERSION
        )
    return OpenAISentimentClient(client, model_version or SENTIMENT_MODEL_VERSION)


def llm_classifier_accuracy(
    client: SentimentClient,
    test_texts: list[str],
    test_labels: np.ndarray,
) -> tuple[float, int]:
    """Accuracy of one LLM call per test conversation.

    Returns (accuracy, call_count) -- call_count is the actual cost driver
    callers should weigh against the other two methods' near-zero marginal
    cost, per §E's accuracy-vs-cost framing.
    """
    predictions = []
    for text in test_texts:
        try:
            predictions.append(client.classify(text))
        except Exception:
            logger.warning(
                "LLM sentiment call failed for one conversation", exc_info=True
            )
            predictions.append(None)
    correct = sum(
        1
        for pred, actual in zip(predictions, test_labels, strict=True)
        if pred == actual
    )
    return correct / len(test_labels), len(test_texts)


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
        len(labeled_df), test_fraction=test_fraction, random_state=random_state
    )

    methods: dict[str, dict[str, Any]] = {
        "embedding_knn": {
            "accuracy": embedding_knn_accuracy(
                vectors[train_idx],
                labels[train_idx],
                vectors[test_idx],
                labels[test_idx],
                n_neighbors=n_neighbors,
            ),
            "call_count": 0,
        },
        "local_classifier": {
            "accuracy": local_classifier_accuracy(
                vectors[train_idx],
                labels[train_idx],
                vectors[test_idx],
                labels[test_idx],
                random_state=random_state,
            ),
            "call_count": 0,
        },
    }
    if sentiment_client is not None:
        test_texts = [texts[i] for i in test_idx]
        accuracy, call_count = llm_classifier_accuracy(
            sentiment_client, test_texts, labels[test_idx]
        )
        methods["llm"] = {"accuracy": accuracy, "call_count": call_count}

    return {"methods": methods, "n_train": len(train_idx), "n_test": len(test_idx)}
