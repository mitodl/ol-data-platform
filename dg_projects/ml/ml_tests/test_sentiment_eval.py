"""Tests for ml.lib.sentiment_eval."""

import numpy as np
import polars as pl
import pytest
from anthropic import Anthropic, AnthropicBedrock
from anthropic.types import TextBlock, ThinkingBlock
from ml.lib import sentiment_eval
from ml.resources.opik_auth import PromptLabelMissingError
from ml_tests.fake_anthropic import FakeAnthropicClient, FakeMessage
from openai import OpenAI


class _FakeLLM:
    """Stands in for LLMClientFactory: a real one needs a Vault resource to build."""

    def __init__(self, client: object, client_class: str = "openai") -> None:
        self._client = client
        self.client_class = client_class

    def get_client(self) -> object:
        return self._client


def test_labeled_sentiment_sample_maps_every_verdict_and_drops_the_rest() -> None:
    ratings = [
        "good",
        "like",
        "positive",
        "bad",
        "dislike",
        "negative",
        "offered",
        "unoffered",
        "idea",
    ]
    df = pl.DataFrame(
        {
            "feedback_conversation_pk": [str(i) for i in range(len(ratings))],
            "explicit_rating": ratings,
        }
    )

    result = sentiment_eval.labeled_sentiment_sample(df)

    assert result["feedback_conversation_pk"].to_list() == [
        "0",
        "1",
        "2",
        "3",
        "4",
        "5",
    ]
    assert result["sentiment"].to_list() == ["positive"] * 3 + ["negative"] * 3


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


def test_embedding_knn_predictions_are_right_on_separable_classes() -> None:
    vectors, labels = _separable_dataset()
    train_idx, test_idx = sentiment_eval.train_test_split_indices(labels)

    predictions = sentiment_eval.embedding_knn_predictions(
        vectors[train_idx], labels[train_idx], vectors[test_idx]
    )

    assert predictions.tolist() == labels[test_idx].tolist()


def test_local_classifier_predictions_are_right_on_separable_classes() -> None:
    vectors, labels = _separable_dataset()
    train_idx, test_idx = sentiment_eval.train_test_split_indices(labels)

    predictions = sentiment_eval.local_classifier_predictions(
        vectors[train_idx], labels[train_idx], vectors[test_idx]
    )

    assert predictions.tolist() == labels[test_idx].tolist()


def test_score_predictions_reports_negative_precision_and_recall() -> None:
    labels = np.array(["positive"] * 6 + ["negative"] * 4)
    predictions = ["positive"] * 5 + ["negative"] + ["negative"] * 2 + [None] * 2

    scores = sentiment_eval.score_predictions(predictions, labels)

    assert scores == {
        "accuracy": 0.7,
        "negative_precision": 2 / 3,
        "negative_recall": 0.5,
    }


def test_score_predictions_leaves_precision_unset_with_no_negative_prediction() -> None:
    scores = sentiment_eval.score_predictions(
        ["positive", "positive"], np.array(["positive", "negative"])
    )

    assert scores["negative_precision"] is None
    assert scores["negative_recall"] == 0.0


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


def test_build_sentiment_client_fails_when_production_has_no_prompt_label(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _missing(*_args: object, **_kwargs: object) -> str:
        msg = "No version of Opik prompt 'feedback-sentiment-classify' is labeled"
        raise PromptLabelMissingError(msg)

    monkeypatch.setattr(sentiment_eval, "render_prompt", _missing)

    with pytest.raises(PromptLabelMissingError):
        sentiment_eval.build_sentiment_client(
            _FakeLLM(Anthropic(api_key="sk-ant-test"))  # pragma: allowlist secret
        )


def test_fetch_sentiment_prompt_rejects_a_template_without_the_placeholder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sentiment_eval, "render_prompt", lambda *_args: "Classify: {{text}}"
    )

    with pytest.raises(ValueError, match="placeholder"):
        sentiment_eval.fetch_sentiment_prompt()


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
            model_version="claude-haiku-5-5",
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
        model_version="claude-haiku-5-5",
    )

    assert isinstance(client, sentiment_eval.OpenAISentimentClient)
    assert client.model_version == "claude-haiku-5-5"


class _FakeSentimentClient:
    model_version = "test-model"

    def __init__(self, canned: list[str | None]) -> None:
        self._canned = iter(canned)

    def classify(self, conversation_text: str) -> str | None:  # noqa: ARG002
        return next(self._canned)


def test_llm_classifier_predictions_returns_one_per_text() -> None:
    client = _FakeSentimentClient(["positive", "negative", "positive"])

    predictions = sentiment_eval.llm_classifier_predictions(client, ["a", "b", "c"])

    assert predictions == ["positive", "negative", "positive"]


def test_llm_classifier_predictions_leaves_a_failed_call_unset() -> None:
    class _FailingClient:
        model_version = "test-model"

        def classify(self, conversation_text: str) -> str | None:  # noqa: ARG002
            msg = "boom"
            raise ValueError(msg)

    predictions = sentiment_eval.llm_classifier_predictions(_FailingClient(), ["a"])

    assert predictions == [None]


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


def test_anthropic_sentiment_client_leaves_a_reply_naming_both_words_unlabeled() -> (
    None
):
    client = sentiment_eval.AnthropicSentimentClient(
        FakeAnthropicClient(
            FakeMessage([TextBlock(type="text", text="Negative, nothing positive")])
        ),
        "claude-haiku-5-5",
    )

    assert client.classify("some conversation text") is None


def test_anthropic_sentiment_client_reads_the_word_after_a_thinking_block() -> None:
    """claude-haiku-5-5 thinks by default, so the reply opens with a thinking block."""
    fake = FakeAnthropicClient(
        FakeMessage(
            [
                ThinkingBlock(type="thinking", thinking="", signature="sig"),
                TextBlock(type="text", text="Negative"),
            ]
        )
    )
    client = sentiment_eval.AnthropicSentimentClient(fake, "claude-haiku-5-5")

    assert client.classify("some conversation text") == "negative"
    assert fake.requests[0]["output_config"] == {
        "effort": sentiment_eval.SENTIMENT_EFFORT
    }


def test_anthropic_sentiment_client_leaves_a_cut_off_reply_unlabeled() -> None:
    client = sentiment_eval.AnthropicSentimentClient(
        FakeAnthropicClient(
            FakeMessage(
                [ThinkingBlock(type="thinking", thinking="", signature="sig")],
                stop_reason="max_tokens",
            )
        ),
        "claude-haiku-5-5",
    )

    assert client.classify("some conversation text") is None


@pytest.mark.parametrize("stop_reason", ["max_tokens", "model_context_window_exceeded"])
def test_anthropic_sentiment_client_leaves_partial_text_unlabeled(
    stop_reason: str,
) -> None:
    """A reply cut off mid-answer can still carry a sentiment word."""
    client = sentiment_eval.AnthropicSentimentClient(
        FakeAnthropicClient(
            FakeMessage(
                [TextBlock(type="text", text="negative")], stop_reason=stop_reason
            )
        ),
        "claude-haiku-5-5",
    )

    assert client.classify("some conversation text") is None
