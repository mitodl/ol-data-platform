"""Opik span details for the courseware model calls.

Each call (one scope's extraction, one adjudication batch, one description batch)
is its own @traced span. Opik times it; these attach what it cannot see on its own:
the token usage, the identifying keys and the Prompt Library version, so a slow or
expensive call can be traced back to the rows it produced.
"""

from typing import Any

from anthropic import Anthropic, AnthropicBedrock
from anthropic.types import Usage
from ml.resources.opik_auth import (
    attach_llm_usage,
    attach_span_metadata,
    attach_span_prompt,
)

TRACE_TAGS = ["courseware"]


def attach_call(
    client: Anthropic | AnthropicBedrock,
    model_id: str,
    usages: list[Usage],
    *,
    template: str,
    metadata: dict[str, Any],
) -> None:
    """Attach one traced call's usage, metadata and prompt to its span.

    The usage is summed over every response the call received, so a retried call
    is priced for both attempts. Cache tokens are passed in the provider's own
    keys, which Opik adds to the billed input and prices separately; the cached
    system prompt is most of each call's input.

    :param client: the client the call used, which decides the pricing provider
    :param model_id: the model id the call sent
    :param usages: the usage of each response, in order
    :param template: the system prompt the call sent, to link its published version
    :param metadata: the keys that identify the call's output rows
    """
    input_tokens = sum(usage.input_tokens for usage in usages)
    output_tokens = sum(usage.output_tokens for usage in usages)
    cache_read = sum(usage.cache_read_input_tokens or 0 for usage in usages)
    cache_write = sum(usage.cache_creation_input_tokens or 0 for usage in usages)
    if isinstance(client, AnthropicBedrock):
        attach_llm_usage(
            usage={
                "inputTokens": input_tokens,
                "outputTokens": output_tokens,
                "cacheReadInputTokens": cache_read,
                "cacheWriteInputTokens": cache_write,
            },
            model=model_id,
            provider="bedrock",
        )
    else:
        attach_llm_usage(
            usage={
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cache_read_input_tokens": cache_read,
                "cache_creation_input_tokens": cache_write,
            },
            model=model_id,
            provider="anthropic",
        )
    attach_span_metadata({**metadata, "model_id": model_id, "attempts": len(usages)})
    attach_span_prompt(template)
