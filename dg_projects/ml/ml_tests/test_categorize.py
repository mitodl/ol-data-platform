"""Tests for ml.lib.categorize."""

import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from ml.lib import categorize
from openai import OpenAI


class _FakeLLM:
    """Stands in for LLMClientFactory: a real one needs a Vault resource to build."""

    def __init__(self, client: object) -> None:
        self._client = client

    def get_client(self) -> object:
        return self._client


def test_new_category_slug_matches_slugify_sql_shape() -> None:
    assert categorize.new_category_slug("Payment / Refund!!") == "payment_refund"


def test_new_category_slug_strips_leading_and_trailing_separators() -> None:
    assert categorize.new_category_slug("  Login Issue  ") == "login_issue"


def test_parse_category_response_reads_plain_json() -> None:
    result = categorize._parse_category_response(
        '{"category_label": "Login Issue", "category_description": "Can\'t log in."}'
    )

    assert result == {
        "category_label": "Login Issue",
        "category_description": "Can't log in.",
    }


def test_parse_category_response_strips_a_code_fence() -> None:
    text = (
        '```json\n{"category_label": "Login Issue", '
        '"category_description": "Can\'t log in."}\n```'
    )

    result = categorize._parse_category_response(text)

    assert result["category_label"] == "Login Issue"


def test_build_category_label_client_uses_default_model_for_anthropic() -> None:
    client = categorize.build_category_label_client(
        _FakeLLM(Anthropic(api_key="sk-ant-test"))  # pragma: allowlist secret
    )

    assert isinstance(client, categorize.AnthropicCategoryLabelClient)
    assert client.model_version == categorize.CATEGORY_MODEL_VERSION


def test_build_category_label_client_uses_bedrock_default_for_bedrock() -> None:
    client = categorize.build_category_label_client(
        _FakeLLM(AnthropicBedrock(aws_region="us-east-1"))
    )

    assert isinstance(client, categorize.AnthropicCategoryLabelClient)
    assert client.model_version == categorize.BEDROCK_CATEGORY_MODEL_VERSION


def test_build_category_label_client_dispatches_to_openai() -> None:
    client = categorize.build_category_label_client(
        _FakeLLM(OpenAI(api_key="sk-test")),  # pragma: allowlist secret
        model_version="gpt-4o-mini",
    )

    assert isinstance(client, categorize.OpenAICategoryLabelClient)
    assert client.model_version == "gpt-4o-mini"


def _conversation_row(
    cluster_id: int, text: str | None, tag: str | None
) -> dict[str, int | str | None]:
    return {
        "cluster_id": cluster_id,
        "conversation_text": text,
        "category_label": tag,
    }


def test_build_cluster_prompt_inputs_excludes_noise_cluster() -> None:
    df = pl.DataFrame(
        [
            _conversation_row(-1, "noise turn", None),
            _conversation_row(0, "real turn", "billing"),
        ]
    )

    result = categorize.build_cluster_prompt_inputs(df)

    assert list(result.keys()) == [0]


def test_build_cluster_prompt_inputs_picks_most_common_tag() -> None:
    df = pl.DataFrame(
        [
            _conversation_row(0, "a", "billing"),
            _conversation_row(0, "b", "billing"),
            _conversation_row(0, "c", "login"),
        ]
    )

    result = categorize.build_cluster_prompt_inputs(df, dominant_tag_count=1)

    assert result[0]["dominant_tags"] == ["billing"]
    assert result[0]["total_conversations"] == 3


def test_build_cluster_prompt_inputs_caps_sample_size() -> None:
    df = pl.DataFrame([_conversation_row(0, f"turn {i}", None) for i in range(20)])

    result = categorize.build_cluster_prompt_inputs(df, sample_size=5)

    assert len(result[0]["samples"]) == 5


def test_build_cluster_prompt_inputs_drops_null_text_from_samples() -> None:
    df = pl.DataFrame(
        [
            _conversation_row(0, None, "billing"),
            _conversation_row(0, "real text", "billing"),
        ]
    )

    result = categorize.build_cluster_prompt_inputs(df)

    assert result[0]["samples"] == ["real text"]


class _FakeCategoryClient:
    model_version = "test-model"

    def __init__(self, responses: dict[int, dict[str, str]]) -> None:
        self._responses = responses

    def propose(
        self,
        dominant_tags: list[str],  # noqa: ARG002
        samples: list[str],
    ) -> dict[str, str]:
        # Keyed by sample count so each test cluster gets a distinct canned reply.
        return self._responses[len(samples)]


def test_propose_categories_builds_one_row_per_cluster() -> None:
    cluster_prompt_inputs = {
        0: {
            "samples": ["a", "b"],
            "dominant_tags": ["billing"],
            "total_conversations": 2,
        },
        1: {"samples": ["c"], "dominant_tags": [], "total_conversations": 1},
    }
    client = _FakeCategoryClient(
        {
            2: {"category_label": "Billing Issue", "category_description": "desc"},
            1: {"category_label": "Login Issue", "category_description": "desc2"},
        }
    )

    result = categorize.propose_categories(cluster_prompt_inputs, client, "run-1")

    assert result.height == 2
    assert set(result["category_slug"].to_list()) == {"billing_issue", "login_issue"}
    assert result["cluster_run_id"].to_list() == ["run-1", "run-1"]


def test_propose_categories_skips_a_cluster_with_no_samples() -> None:
    cluster_prompt_inputs = {
        0: {"samples": [], "dominant_tags": [], "total_conversations": 0},
    }
    client = _FakeCategoryClient({})

    result = categorize.propose_categories(cluster_prompt_inputs, client, "run-1")

    assert result.height == 0


def test_propose_categories_skips_a_cluster_whose_call_fails() -> None:
    class _FailingClient:
        model_version = "test-model"

        def propose(
            self,
            dominant_tags: list[str],  # noqa: ARG002
            samples: list[str],  # noqa: ARG002
        ) -> dict[str, str]:
            msg = "boom"
            raise ValueError(msg)

    cluster_prompt_inputs = {
        0: {"samples": ["a"], "dominant_tags": [], "total_conversations": 1},
    }

    result = categorize.propose_categories(
        cluster_prompt_inputs, _FailingClient(), "run-1"
    )

    assert result.height == 0
