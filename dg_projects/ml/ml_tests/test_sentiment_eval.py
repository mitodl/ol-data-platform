"""Tests for ml.lib.sentiment_eval."""

import numpy as np
import polars as pl
import pytest
from anthropic import Anthropic, AnthropicBedrock
from ml.lib import sentiment_eval
from openai import OpenAI


class _FakeLLM:
    """Stands in for LLMClientFactory: a real one needs a Vault resource to build."""

    def __init__(self, client: object, client_class: str = "openai") -> None:
        self._client = client
        self.client_class = client_class

    def get_client(self) -> object:
        return self._client


def test_labeled_sentiment_sample_maps_good_bad_and_drops_the_rest() -> None:
    df = pl.DataFrame(
        {
            "feedback_conversation_pk": ["1", "2", "3"],
            "explicit_rating": ["good", "bad", "offered"],
        }
    )

    result = sentiment_eval.labeled_sentiment_sample(df)

    assert result["feedback_conversation_pk"].to_list() == ["1", "2"]
    assert result["sentiment"].to_list() == ["positive", "negative"]


def test_train_test_split_indices_covers_every_row_exactly_once() -> None:
    labels = np.array(["positive"] * 5 + ["negative"] * 5)
    train_idx, test_idx = sentiment_eval.train_test_split_indices(
        labels, test_fraction=0.3
    )

    assert len(train_idx) + len(test_idx) == 10
    assert set(train_idx.tolist()) | set(test_idx.tolist()) == set(range(10))
    assert not (set(train_idx.tolist()) & set(test_idx.tolist()))


def test_train_test_split_indices_is_deterministic() -> None:
    labels = np.array(["positive"] * 10 + ["negative"] * 10)
    first = sentiment_eval.train_test_split_indices(labels, random_state=7)
    second = sentiment_eval.train_test_split_indices(labels, random_state=7)

    assert first[0].tolist() == second[0].tolist()
    assert first[1].tolist() == second[1].tolist()


def test_train_test_split_indices_stratifies_the_minority_class() -> None:
    """An unstratified shuffle can put every negative in the same partition --
    stratifying per label guarantees both classes appear in train and test.
    """
    labels = np.array(["positive"] * 18 + ["negative"] * 2)

    train_idx, test_idx = sentiment_eval.train_test_split_indices(
        labels, test_fraction=0.3, random_state=1
    )

    assert "negative" in labels[train_idx]
    assert "negative" in labels[test_idx]


def _separable_dataset(n_per_class: int = 20, seed: int = 0):
    rng = np.random.default_rng(seed)
    positive = rng.normal(loc=5.0, scale=0.1, size=(n_per_class, 4))
    negative = rng.normal(loc=-5.0, scale=0.1, size=(n_per_class, 4))
    vectors = np.vstack([positive, negative])
    labels = np.array(["positive"] * n_per_class + ["negative"] * n_per_class)
    return vectors, labels


def test_embedding_knn_accuracy_is_high_on_separable_classes() -> None:
    vectors, labels = _separable_dataset()
    train_idx, test_idx = sentiment_eval.train_test_split_indices(labels)

    accuracy = sentiment_eval.embedding_knn_accuracy(
        vectors[train_idx], labels[train_idx], vectors[test_idx], labels[test_idx]
    )

    assert accuracy == 1.0


def test_local_classifier_accuracy_is_high_on_separable_classes() -> None:
    vectors, labels = _separable_dataset()
    train_idx, test_idx = sentiment_eval.train_test_split_indices(labels)

    accuracy = sentiment_eval.local_classifier_accuracy(
        vectors[train_idx], labels[train_idx], vectors[test_idx], labels[test_idx]
    )

    assert accuracy == 1.0


def test_build_sentiment_client_uses_default_model_for_anthropic() -> None:
    client = sentiment_eval.build_sentiment_client(
        _FakeLLM(Anthropic(api_key="sk-ant-test"))  # pragma: allowlist secret
    )

    assert isinstance(client, sentiment_eval.AnthropicSentimentClient)
    assert client.model_version == sentiment_eval.SENTIMENT_MODEL_VERSION


def test_build_sentiment_client_uses_bedrock_default_for_bedrock() -> None:
    client = sentiment_eval.build_sentiment_client(
        _FakeLLM(AnthropicBedrock(aws_region="us-east-1"))
    )

    assert isinstance(client, sentiment_eval.AnthropicSentimentClient)
    assert client.model_version == sentiment_eval.BEDROCK_SENTIMENT_MODEL_VERSION


def test_build_sentiment_client_dispatches_to_openai() -> None:
    client = sentiment_eval.build_sentiment_client(
        _FakeLLM(OpenAI(api_key="sk-test")),  # pragma: allowlist secret
        model_version="gpt-4o-mini",
    )

    assert isinstance(client, sentiment_eval.OpenAISentimentClient)
    assert client.model_version == "gpt-4o-mini"


def test_build_sentiment_client_rejects_claude_model_for_real_openai() -> None:
    with pytest.raises(ValueError, match="looks like an Anthropic model id"):
        sentiment_eval.build_sentiment_client(
            _FakeLLM(
                OpenAI(api_key="sk-test"),  # pragma: allowlist secret
                client_class="openai",
            ),
            model_version="claude-haiku-4-5",
        )


def test_build_sentiment_client_allows_claude_model_for_openai_compatible() -> None:
    client = sentiment_eval.build_sentiment_client(
        _FakeLLM(
            OpenAI(
                api_key="sk-test",  # pragma: allowlist secret
                base_url="https://parley.example.com",
            ),
            client_class="openai_compatible",
        ),
        model_version="claude-haiku-4-5",
    )

    assert isinstance(client, sentiment_eval.OpenAISentimentClient)
    assert client.model_version == "claude-haiku-4-5"


class _FakeSentimentClient:
    model_version = "test-model"

    def __init__(self, canned: list[str | None]) -> None:
        self._canned = iter(canned)

    def classify(self, conversation_text: str) -> str | None:  # noqa: ARG002
        return next(self._canned)


def test_llm_classifier_accuracy_counts_correct_predictions() -> None:
    client = _FakeSentimentClient(["positive", "negative", "positive"])

    accuracy, call_count = sentiment_eval.llm_classifier_accuracy(
        client, ["a", "b", "c"], np.array(["positive", "negative", "negative"])
    )

    assert call_count == 3
    assert accuracy == 2 / 3


def test_llm_classifier_accuracy_treats_a_failed_call_as_wrong() -> None:
    class _FailingClient:
        model_version = "test-model"

        def classify(self, conversation_text: str) -> str | None:  # noqa: ARG002
            msg = "boom"
            raise ValueError(msg)

    accuracy, call_count = sentiment_eval.llm_classifier_accuracy(
        _FailingClient(), ["a"], np.array(["positive"])
    )

    assert call_count == 1
    assert accuracy == 0.0


def test_run_sentiment_eval_skips_llm_arm_when_client_is_none() -> None:
    vectors, labels = _separable_dataset(n_per_class=10)
    df = pl.DataFrame(
        {
            "embedding_vector": vectors.tolist(),
            "sentiment": labels.tolist(),
            "conversation_text": [f"text {i}" for i in range(len(labels))],
        }
    )

    result = sentiment_eval.run_sentiment_eval(
        df, embedding_dim=4, sentiment_client=None
    )

    assert set(result["methods"].keys()) == {"embedding_knn", "local_classifier"}
    assert result["n_train"] + result["n_test"] == len(labels)


def test_run_sentiment_eval_includes_llm_arm_when_client_given() -> None:
    vectors, labels = _separable_dataset(n_per_class=10)
    df = pl.DataFrame(
        {
            "embedding_vector": vectors.tolist(),
            "sentiment": labels.tolist(),
            "conversation_text": [f"text {i}" for i in range(len(labels))],
        }
    )
    client = _FakeSentimentClient(["positive"] * len(labels))

    result = sentiment_eval.run_sentiment_eval(
        df, embedding_dim=4, sentiment_client=client
    )

    assert "llm" in result["methods"]
    assert result["methods"]["llm"]["call_count"] == result["n_test"]
