"""Strategies: the named combination of choices that decides a set of vectors.

A strategy is one row of a reviewed YAML file, so trying a new model, chunker or
context template is a pull request adding a row, and its vectors are stored
under its own keys next to the others'. ``learn_v1``
(:mod:`ol_orchestrate.lib.embeddings.learn`) is MIT Learn's current configuration
written as strategies.
"""

from pathlib import Path
from string import Formatter
from typing import Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ol_orchestrate.lib.embeddings.chunking import Chunk, Chunker
from ol_orchestrate.lib.embeddings.documents import Document
from ol_orchestrate.lib.embeddings.models import EmbeddingModelSpec

StrategyRole = Literal["serving", "candidate", "retired"]
CHUNK_FIELD = "chunk"


class QdrantTarget(BaseModel):
    """Where a strategy's vectors are projected in Qdrant.

    :param collection: the collection
    :param dense_vector_name: the named dense vector the strategy writes
    :param sparse_model: the model Qdrant computes the sparse vector with, from
        the chunk text; ``None`` writes no sparse vector
    :param sparse_vector_name: the named sparse vector
    """

    model_config = ConfigDict(frozen=True)

    collection: str
    dense_vector_name: str
    sparse_model: str | None = None
    sparse_vector_name: str | None = None

    @model_validator(mode="after")
    def _check_sparse(self) -> Self:
        if (self.sparse_model is None) != (self.sparse_vector_name is None):
            msg = "sparse_model and sparse_vector_name must be set together"
            raise ValueError(msg)
        return self


class EmbeddingStrategy(BaseModel):
    """How one kind of document is chunked and embedded.

    :param strategy_id: lowercase letters, digits and underscores
    :param doc_kind: the kind of document the strategy applies to
    :param chunker: how documents are split
    :param model: the embedding model
    :param context_template: a ``str.format`` template for the text embedded for
        each chunk, with ``{chunk}`` for the chunk and the document's metadata
        keys for the rest, e.g. ``"{title}\\n\\n{chunk}"``. ``None`` embeds the
        chunk as it is. The chunk text stored and searched over stays the chunk.
    :param role: ``serving`` is what users search, ``candidate`` is under
        evaluation, ``retired`` keeps its vectors but is no longer refreshed
    :param qdrant: where the strategy is projected; ``None`` keeps it in the
        platform only
    :param notes: why the strategy exists
    """

    model_config = ConfigDict(frozen=True)

    strategy_id: str = Field(pattern=r"^[a-z0-9_]+$")
    doc_kind: str
    chunker: Chunker
    model: EmbeddingModelSpec
    context_template: str | None = None
    role: StrategyRole = "candidate"
    qdrant: QdrantTarget | None = None
    notes: str = ""

    @model_validator(mode="after")
    def _check_template(self) -> Self:
        if self.context_template is None:
            return self
        fields = {
            name
            for _, name, _, _ in Formatter().parse(self.context_template)
            if name is not None
        }
        if CHUNK_FIELD not in fields:
            msg = "context_template must contain {chunk}"
            raise ValueError(msg)
        if "" in fields or any(name.isdigit() for name in fields):
            msg = "context_template fields must be named"
            raise ValueError(msg)
        return self

    def embedding_text(self, document: Document, chunk: Chunk) -> str:
        """Render the text sent to the model for a chunk.

        :param document: the chunk's document
        :param chunk: the chunk
        :rtype: str
        :raises KeyError: when the template names a key the document's metadata
            does not have
        """
        if self.context_template is None:
            return chunk.text
        return self.context_template.format_map(
            {**document.metadata, CHUNK_FIELD: chunk.text}
        )


class StrategyFile(BaseModel):
    """The YAML file strategies are declared in: ``strategies: [...]``."""

    model_config = ConfigDict(frozen=True)

    strategies: list[EmbeddingStrategy]

    @model_validator(mode="after")
    def _check_unique(self) -> Self:
        ids = [strategy.strategy_id for strategy in self.strategies]
        if duplicated := sorted({i for i in ids if ids.count(i) > 1}):
            msg = f"strategy ids declared more than once: {duplicated}"
            raise ValueError(msg)
        return self


def load_strategies(path: Path) -> dict[str, EmbeddingStrategy]:
    """Read a strategy file.

    :param path: the YAML file
    :returns: the strategies by id
    :rtype: dict[str, EmbeddingStrategy]
    """
    declared = StrategyFile.model_validate(yaml.safe_load(path.read_text()))
    return {strategy.strategy_id: strategy for strategy in declared.strategies}
