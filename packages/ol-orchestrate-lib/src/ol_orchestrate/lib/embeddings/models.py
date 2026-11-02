"""Embedding models, and the embedder that calls one through ol-llm.

The model call goes through ol-llm's ``embed_sync`` under a ``UseCase``, which
gives it the shared retry policy, error classes, cost accounting and an Opik
trace per request. Batching to a provider's request limits stays here, as
ol-llm leaves it to the caller.
"""

from collections.abc import Iterator, Sequence
from typing import Protocol

import tiktoken
from ol_llm import Attribution, UseCase, embed_sync, load_catalog
from pydantic import BaseModel, ConfigDict, Field
from pydantic_ai.embeddings.result import EmbedInputType

# OpenAI accepts 2,048 inputs and 300,000 tokens per embeddings request. The token
# budget leaves room for the provider's tokenizer counting differently.
DEFAULT_BATCH_SIZE = 256
DEFAULT_MAX_REQUEST_TOKENS = 250_000


class EmbeddingModelSpec(BaseModel):
    """A dense embedding model at a vector length.

    :param model: the ol-llm model id, ``provider:model``
    :param dimensions: the vector length to ask for, for a model that can
        shorten its vectors. ``None`` asks for nothing and takes the model's
        default length, which the ol-llm catalog must then list.
    :param batch_size: most texts per request
    :param max_request_tokens: most tokens per request, counted with
        ``token_encoding``
    :param token_encoding: the tiktoken encoding that estimates request size
    :param vector_space: a name shared by the models whose vectors are
        interchangeable, e.g. ``text-embedding-3-large@3072`` for OpenAI's model
        and an Azure OpenAI deployment of it. Stored vectors are reused within a
        space. ``None`` puts the model in a space of its own, ``model_id``, so
        that two providers' vectors are only ever mixed by declaring it.
    """

    model_config = ConfigDict(frozen=True)

    model: str
    dimensions: int | None = Field(default=None, gt=0)
    batch_size: int = Field(default=DEFAULT_BATCH_SIZE, gt=0)
    max_request_tokens: int = Field(default=DEFAULT_MAX_REQUEST_TOKENS, gt=0)
    token_encoding: str = "cl100k_base"  # noqa: S105
    vector_space: str | None = Field(default=None, min_length=1)

    def request_batches(self, texts: Sequence[str]) -> Iterator[list[str]]:
        """Split texts into requests within ``batch_size`` and ``max_request_tokens``.

        :param texts: the texts, kept in order
        :returns: the requests' texts
        :rtype: Iterator[list[str]]
        :raises ValueError: when one text alone is over ``max_request_tokens``
        """
        encoding = tiktoken.get_encoding(self.token_encoding)
        batch: list[str] = []
        batch_tokens = 0
        for text in texts:
            tokens = len(encoding.encode(text, disallowed_special=()))
            if tokens > self.max_request_tokens:
                msg = f"a text of {tokens} tokens is over max_request_tokens"
                raise ValueError(msg)
            if batch and (
                len(batch) == self.batch_size
                or batch_tokens + tokens > self.max_request_tokens
            ):
                yield batch
                batch, batch_tokens = [], 0
            batch.append(text)
            batch_tokens += tokens
        if batch:
            yield batch

    @property
    def vector_length(self) -> int:
        """The length of the vectors the model returns.

        :rtype: int
        :raises ValueError: when ``dimensions`` is unset and the ol-llm catalog
            does not give the model's default length
        """
        if self.dimensions is not None:
            return self.dimensions
        entry = load_catalog().get(self.model)
        if entry is None or entry.dimensions is None:
            msg = (
                f"{self.model} is not an embedding model in the ol-llm catalog; "
                "set dimensions"
            )
            raise ValueError(msg)
        return entry.dimensions

    @property
    def model_id(self) -> str:
        """``{model}@{vector length}``: which model produced a stored vector.

        Vectors from another model, or the same model at another length, are not
        comparable, so both are in the id.

        :rtype: str
        """
        return f"{self.model}@{self.vector_length}"

    @property
    def vector_space_id(self) -> str:
        """The space a stored vector is reused in: ``vector_space``, else ``model_id``.

        :rtype: str
        """
        return self.vector_space or self.model_id


class EmbeddingBatch(BaseModel):
    """Vectors for a list of texts, and what they cost.

    :param vectors: one vector per text, in order
    :param input_tokens: the input tokens the provider reported
    """

    model_config = ConfigDict(frozen=True)

    vectors: list[list[float]]
    input_tokens: int


class Embedder(Protocol):
    """Anything that turns texts into vectors of one model."""

    @property
    def spec(self) -> EmbeddingModelSpec:
        """The model the vectors come from."""
        ...

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        """Return one vector per text, in order."""
        ...


class OlLlmEmbedder:
    """Embeds through ol-llm, in requests sized by the spec, each one traced.

    :param spec: the model
    :param use_case: the ol-llm call site; its ``model`` must be ``spec.model``.
        Its ``app`` is the Opik project the requests are traced to.
    :param attribution: e.g. the Dagster run id, put on every trace
    """

    def __init__(
        self,
        spec: EmbeddingModelSpec,
        use_case: UseCase,
        attribution: Attribution,
    ) -> None:
        """Check that the use case calls the spec's model.

        :raises ValueError: when it calls another model
        """
        if use_case.model != spec.model:
            msg = f"use case model {use_case.model} is not {spec.model}"
            raise ValueError(msg)
        self._spec = spec
        self._use_case = use_case
        self._attribution = attribution

    @property
    def spec(self) -> EmbeddingModelSpec:
        """The model the vectors come from.

        :rtype: EmbeddingModelSpec
        """
        return self._spec

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        """Embed texts in requests from ``spec.request_batches``.

        :param texts: the texts
        :param input_type: ``document`` for what is stored, ``query`` for what is
            searched with; some models embed the two differently
        :returns: one vector per text, in order, and the total input tokens
        :rtype: EmbeddingBatch
        :raises ValueError: when a request returns vectors that are not
            ``spec.vector_length`` long. ol-llm raises ``LLMInvalidOutput`` itself
            when the vector count does not match the texts.
        """
        vectors: list[list[float]] = []
        input_tokens = 0
        for batch in self._spec.request_batches(texts):
            result = embed_sync(
                self._use_case,
                batch,
                input_type=input_type,
                dimensions=self._spec.dimensions,
                attribution=self._attribution,
            )
            if result.dimensions != self._spec.vector_length:
                msg = (
                    f"{self._spec.model} returned vectors {result.dimensions} long, "
                    f"not {self._spec.vector_length}"
                )
                raise ValueError(msg)
            vectors.extend(list(vector) for vector in result.embeddings)
            input_tokens += result.usage.input_tokens
        return EmbeddingBatch(vectors=vectors, input_tokens=input_tokens)
