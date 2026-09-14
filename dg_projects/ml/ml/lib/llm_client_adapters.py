"""Shared Anthropic/OpenAI call plumbing for the feedback LLM adapters.

summarize.py and sentiment_eval.py each wrap an LLMClientFactory client in an
Anthropic*Client/OpenAI*Client pair -- only the prompt, max_tokens, and response
parsing differ between them. This module holds the shared call/usage-attach/
dispatch logic; each caller keeps its own @traced-decorated public method for
its trace name/tags and response handling.
"""

from typing import Protocol, TypeVar

from anthropic import Anthropic, AnthropicBedrock
from anthropic.types import Message
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import attach_llm_usage, infer_llm_provider
from openai import OpenAI
from openai.types.chat import ChatCompletion


def raise_if_claude_model_on_openai(
    *, client_class: str, model_version: str, config_hint: str
) -> None:
    """Guard against a Claude model id configured under client_class='openai'.

    Only real api.openai.com can never serve an Anthropic-namespaced model id --
    "openai_compatible" may legitimately proxy Claude under this same id, so it
    gets no such check. config_hint names the caller's own Config field/env var
    in the error message.
    """
    if client_class == "openai" and model_version.startswith("claude"):
        msg = (
            f"model_version={model_version!r} looks like an Anthropic model id, "
            f"but client_class='openai' is configured. Set {config_hint} to an "
            "OpenAI model id (e.g. 'gpt-4o-mini')."
        )
        raise ValueError(msg)


def call_anthropic(
    client: Anthropic | AnthropicBedrock,
    model_version: str,
    *,
    max_tokens: int,
    prompt: str,
) -> Message:
    """messages.create + Opik usage attach. Caller handles content/empty-response."""
    message = client.messages.create(
        model=model_version,
        max_tokens=max_tokens,
        messages=[{"role": "user", "content": prompt}],
    )
    if message.usage is not None:
        attach_llm_usage(
            usage={
                "prompt_tokens": message.usage.input_tokens,
                "completion_tokens": message.usage.output_tokens,
                "total_tokens": message.usage.input_tokens
                + message.usage.output_tokens,
            },
            model=model_version,
            provider="bedrock" if isinstance(client, AnthropicBedrock) else "anthropic",
        )
    return message


def call_openai(client: OpenAI, model_version: str, *, prompt: str) -> ChatCompletion:
    """chat.completions.create + Opik usage attach. Caller handles content/empty."""
    response = client.chat.completions.create(
        model=model_version,
        messages=[{"role": "user", "content": prompt}],
    )
    if response.usage is not None:
        attach_llm_usage(
            usage={
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "total_tokens": response.usage.total_tokens,
            },
            model=model_version,
            provider=infer_llm_provider(model_version, default="openai"),
        )
    return response


AnthropicClientT_co = TypeVar("AnthropicClientT_co", covariant=True)
OpenAIClientT_co = TypeVar("OpenAIClientT_co", covariant=True)


class _AnthropicClientCtor(Protocol[AnthropicClientT_co]):
    """type[X] alone can't tell mypy an arbitrary class is callable this way."""

    def __call__(
        self, client: Anthropic | AnthropicBedrock, model_version: str
    ) -> AnthropicClientT_co: ...


class _OpenAIClientCtor(Protocol[OpenAIClientT_co]):
    def __call__(
        self, client: OpenAI, model_version: str, *, client_class: str
    ) -> OpenAIClientT_co: ...


def build_llm_client(  # noqa: PLR0913, UP047
    llm: LLMClientFactory,
    *,
    anthropic_client_cls: _AnthropicClientCtor[AnthropicClientT_co],
    openai_client_cls: _OpenAIClientCtor[OpenAIClientT_co],
    model_version: str | None,
    bedrock_model_version: str | None,
    default_model_version: str,
    default_bedrock_model_version: str,
) -> AnthropicClientT_co | OpenAIClientT_co:
    """Build the client whose model_version comes from run config, else a default.

    model_version/bedrock_model_version are the caller's own per-run Config
    fields -- None means the run didn't override them, so the caller's own
    MODEL_VERSION/BEDROCK_MODEL_VERSION env-var default applies.
    """
    client = llm.get_client()
    if isinstance(client, AnthropicBedrock):
        return anthropic_client_cls(
            client, bedrock_model_version or default_bedrock_model_version
        )
    if isinstance(client, Anthropic):
        return anthropic_client_cls(client, model_version or default_model_version)
    return openai_client_cls(
        client,
        model_version or default_model_version,
        client_class=llm.client_class,
    )
