"""LLM cluster labeling: proposing dim_feedback_category candidates from clusters.

One LLM call per cluster -- there are hundreds of clusters, not millions of
conversations, which is what keeps this a trivial, cheap batch unlike
per-record summarization/embedding.
"""

import json
import logging
import os
import re
from typing import Any, Protocol

import polars as pl
from anthropic import Anthropic, AnthropicBedrock
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import (
    attach_llm_usage,
    infer_llm_provider,
    render_prompt,
    traced,
)
from openai import OpenAI

# HDBSCAN's noise label -- never worth labeling, it's the one-off-complaint bucket
# by construction (see ml.lib.cluster).
NOISE_CLUSTER_ID = -1

CATEGORY_PROPOSAL_SCHEMA = {
    "cluster_run_id": pl.String,
    "cluster_id": pl.Int64,
    "category_slug": pl.String,
    "category_label": pl.String,
    "category_description": pl.String,
    "sample_size": pl.Int64,
    "dominant_tags": pl.String,
    "model_version": pl.String,
    "proposed_at": pl.Datetime(time_zone="UTC"),
}

# How many representative conversations to sample per cluster for the prompt.
# Not the whole cluster -- a handful is enough for an LLM to characterize the
# theme, and keeps the per-cluster call cheap regardless of cluster size.
CATEGORY_PROPOSAL_SAMPLE_SIZE = int(
    os.environ.get("CATEGORY_PROPOSAL_SAMPLE_SIZE", "10")
)

# How many of a cluster's dominant existing tags to surface to the LLM as context
# -- the seed taxonomy it should prefer to reuse/refine rather than invent from
# scratch where one already fits.
CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT = int(
    os.environ.get("CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT", "3")
)

# Same reasoning as SUMMARY_MODEL_VERSION/BEDROCK_SUMMARY_MODEL_VERSION
# (ml.lib.summarize): a model id is only valid for one vendor's API.
CATEGORY_MODEL_VERSION = os.environ.get("CATEGORY_MODEL_VERSION", "claude-haiku-4-5")
BEDROCK_CATEGORY_MODEL_VERSION = os.environ.get(
    "BEDROCK_CATEGORY_MODEL_VERSION",
    "global.anthropic.claude-haiku-4-5-20251001-v1:0",
)

CATEGORY_MAX_TOKENS = int(os.environ.get("CATEGORY_MAX_TOKENS", "512"))

CATEGORY_PROMPT = (
    "You are labeling a cluster of similar support conversations for an internal "
    "support-ticket taxonomy. Below are a sample of redacted conversations from "
    "this cluster, and the existing tags most commonly already applied to "
    "conversations in it.\n\n"
    "Existing dominant tags for this cluster: {{dominant_tags}}\n\n"
    "Sample conversations:\n{{samples}}\n\n"
    "Propose a short category label for this cluster. Prefer reusing or "
    "lightly refining one of the existing dominant tags where it already fits; "
    "only propose something new if none of them describe the cluster's actual "
    "common theme.\n\n"
    "Respond with only a JSON object, no other text, in this exact shape: "
    '{"category_label": "...", "category_description": "one sentence"}'
)

logger = logging.getLogger(__name__)


def _category_prompt(dominant_tags: list[str], samples: list[str]) -> str:
    """CATEGORY_PROMPT rendered, preferring Opik's Prompt Library entry if set up."""
    return render_prompt(
        "feedback-category-proposal",
        CATEGORY_PROMPT,
        dominant_tags=", ".join(dominant_tags) or "(none)",
        samples="\n---\n".join(samples),
    )


def new_category_slug(category_label: str) -> str:
    """Slugify a label the same way src/ol_dbt/macros/slugify.sql does.

    Independently generated, not LLM-trusted: the LLM proposes a label in
    natural language; the slug is derived deterministically from it here so it
    always matches the stable, storable form dim_feedback_category expects.
    """
    lowered = category_label.lower()
    collapsed = re.sub(r"[^a-z0-9]+", "_", lowered)
    return collapsed.strip("_")


class CategoryLabelClient(Protocol):
    model_version: str

    def propose(
        self, dominant_tags: list[str], samples: list[str]
    ) -> dict[str, str]: ...


def _parse_category_response(text: str) -> dict[str, str]:
    """Parse the model's {"category_label": ..., "category_description": ...} JSON.

    Models occasionally wrap JSON in a code fence despite the prompt's "only a
    JSON object" instruction -- stripped here rather than tightening the prompt
    further, since fence-stripping is cheap and the alternative is a silent
    parse failure on an otherwise-good response.
    """
    stripped = text.strip()
    if stripped.startswith("```"):
        stripped = stripped.strip("`")
        if stripped.startswith("json"):
            stripped = stripped[len("json") :]
        stripped = stripped.strip()
    payload = json.loads(stripped)
    return {
        "category_label": payload["category_label"],
        "category_description": payload["category_description"],
    }


class AnthropicCategoryLabelClient:
    """Adapts an Anthropic-compatible client (incl. AnthropicBedrock) to
    CategoryLabelClient.
    """

    def __init__(
        self, client: Anthropic | AnthropicBedrock, model_version: str
    ) -> None:
        self._client = client
        self.model_version = model_version

    @traced("feedback_category_propose_anthropic", tags=["feedback"])
    def propose(self, dominant_tags: list[str], samples: list[str]) -> dict[str, str]:
        message = self._client.messages.create(
            model=self.model_version,
            max_tokens=CATEGORY_MAX_TOKENS,
            messages=[
                {
                    "role": "user",
                    "content": _category_prompt(dominant_tags, samples),
                }
            ],
        )
        if message.usage is not None:
            attach_llm_usage(
                usage={
                    "prompt_tokens": message.usage.input_tokens,
                    "completion_tokens": message.usage.output_tokens,
                    "total_tokens": message.usage.input_tokens
                    + message.usage.output_tokens,
                },
                model=self.model_version,
                provider="bedrock"
                if isinstance(self._client, AnthropicBedrock)
                else "anthropic",
            )
        if not message.content:
            msg = (
                "Empty response proposing a category label "
                "(possibly all thinking tokens)."
            )
            raise ValueError(msg)
        return _parse_category_response(message.content[0].text)


class OpenAICategoryLabelClient:
    """Adapts an OpenAI-compatible client to CategoryLabelClient."""

    def __init__(self, client: OpenAI, model_version: str) -> None:
        self._client = client
        self.model_version = model_version

    @traced("feedback_category_propose_openai", tags=["feedback"])
    def propose(self, dominant_tags: list[str], samples: list[str]) -> dict[str, str]:
        response = self._client.chat.completions.create(
            model=self.model_version,
            messages=[
                {
                    "role": "user",
                    "content": _category_prompt(dominant_tags, samples),
                }
            ],
        )
        if response.usage is not None:
            attach_llm_usage(
                usage={
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens,
                },
                model=self.model_version,
                provider=infer_llm_provider(self.model_version, default="openai"),
            )
        content = response.choices[0].message.content
        if not content:
            msg = "Empty response proposing a category label."
            raise ValueError(msg)
        return _parse_category_response(content)


def build_category_label_client(
    llm: LLMClientFactory,
    model_version: str | None = None,
    bedrock_model_version: str | None = None,
) -> AnthropicCategoryLabelClient | OpenAICategoryLabelClient:
    """Build the client whose model_version comes from run config, else a default.

    Mirrors ml.lib.summarize.build_summary_client's dispatch and
    model_version/bedrock_model_version split -- same reasoning: Bedrock's model
    id namespace differs from the plain Anthropic/OpenAI one.
    """
    client = llm.get_client()
    if isinstance(client, AnthropicBedrock):
        return AnthropicCategoryLabelClient(
            client, bedrock_model_version or BEDROCK_CATEGORY_MODEL_VERSION
        )
    if isinstance(client, Anthropic):
        return AnthropicCategoryLabelClient(
            client, model_version or CATEGORY_MODEL_VERSION
        )
    return OpenAICategoryLabelClient(client, model_version or CATEGORY_MODEL_VERSION)


def build_cluster_prompt_inputs(
    conversation_df: pl.DataFrame,
    sample_size: int = CATEGORY_PROPOSAL_SAMPLE_SIZE,
    dominant_tag_count: int = CATEGORY_PROPOSAL_DOMINANT_TAG_COUNT,
    random_state: int = 42,
) -> dict[int, dict[str, Any]]:
    """Group a cluster-joined conversation frame into per-cluster prompt inputs.

    Args:
        conversation_df: one row per conversation, with (at least) cluster_id,
            conversation_text, and category_label (the conversation's seed-tag
            category, nullable) columns.
        sample_size: representative conversations to sample per cluster.
        dominant_tag_count: how many of a cluster's most common category_labels
            to surface as context.

    Returns:
        {cluster_id: {"samples": [...], "dominant_tags": [...],
        "total_conversations": int}}, excluding the noise cluster (-1).
    """
    non_noise = conversation_df.filter(pl.col("cluster_id") != NOISE_CLUSTER_ID)
    result: dict[int, dict[str, Any]] = {}
    for cluster_key, group in non_noise.group_by("cluster_id"):
        # group_by yields a 1-tuple key for a single group-by column.
        cluster_id = cluster_key[0]
        texts = group["conversation_text"].drop_nulls().to_list()
        if len(texts) > sample_size:
            texts = (
                group.filter(pl.col("conversation_text").is_not_null())
                .sample(n=sample_size, seed=random_state)["conversation_text"]
                .to_list()
            )
        tag_counts = (
            group["category_label"]
            .drop_nulls()
            .value_counts()
            .sort("count", descending=True)
        )
        dominant_tags = tag_counts["category_label"].head(dominant_tag_count).to_list()
        result[cluster_id] = {
            "samples": texts,
            "dominant_tags": dominant_tags,
            "total_conversations": group.height,
        }
    return result


def propose_categories(
    cluster_prompt_inputs: dict[int, dict[str, Any]],
    client: CategoryLabelClient,
    cluster_run_id: str,
) -> pl.DataFrame:
    """Call the LLM once per cluster, returning one proposal row per cluster.

    A cluster whose proposal call fails is skipped (logged), not fatal to the
    whole run -- a few hundred clusters means one bad call shouldn't lose every
    other cluster's proposal.
    """
    rows = []
    for cluster_id, inputs in cluster_prompt_inputs.items():
        if not inputs["samples"]:
            logger.warning(
                "Cluster %s has no non-null conversation_text to sample; skipping",
                cluster_id,
            )
            continue
        try:
            proposal = client.propose(inputs["dominant_tags"], inputs["samples"])
        except Exception:
            logger.warning(
                "Failed to propose a category for cluster %s", cluster_id, exc_info=True
            )
            continue
        rows.append(
            {
                "cluster_run_id": cluster_run_id,
                "cluster_id": cluster_id,
                "category_slug": new_category_slug(proposal["category_label"]),
                "category_label": proposal["category_label"],
                "category_description": proposal["category_description"],
                "sample_size": len(inputs["samples"]),
                "dominant_tags": ", ".join(inputs["dominant_tags"]),
                "model_version": client.model_version,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                k: v for k, v in CATEGORY_PROPOSAL_SCHEMA.items() if k != "proposed_at"
            }
        )
    return pl.DataFrame(rows)
