"""Project a strategy's vector rows into a Qdrant collection.

Qdrant holds a copy of what the platform stores, so a collection can be dropped
and refilled from the rows. The point id and payload are the consumer's
contract, not the platform's, so a ``PointMapper`` supplies them.
"""

from collections.abc import Mapping, Sequence
from typing import Any, Protocol

from qdrant_client import models

from ol_orchestrate.lib.embeddings.documents import Document
from ol_orchestrate.lib.embeddings.pipeline import ChunkRow, VectorRow
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy


class PointMapper(Protocol):
    """The point id and payload a consumer's collection expects for a chunk."""

    def point_id(self, document: Document, row: ChunkRow) -> str:
        """Return the id of the chunk's point."""
        ...

    def payload(self, document: Document, row: ChunkRow) -> dict[str, Any]:
        """Return the chunk's payload."""
        ...


def build_points(
    strategy: EmbeddingStrategy,
    chunks: Sequence[tuple[Document, ChunkRow]],
    vectors: Mapping[str, VectorRow],
    mapper: PointMapper,
) -> list[models.PointStruct]:
    """Build the points of a strategy's chunks.

    The sparse vector, when the strategy's target has one, is sent as a
    ``models.Document`` of the chunk text for Qdrant to encode, so the client
    that uploads the points must have cloud inference on.

    :param strategy: the strategy; it must have a Qdrant target
    :param chunks: the chunks, with their documents
    :param vectors: the strategy's vector rows by ``chunk_key``
    :param mapper: the point ids and payloads
    :returns: one point per chunk, in order
    :rtype: list[models.PointStruct]
    :raises ValueError: when the strategy has no Qdrant target, or a vector row
        is from another strategy
    :raises KeyError: when a chunk has no vector row
    """
    target = strategy.qdrant
    if target is None:
        msg = f"{strategy.strategy_id} has no Qdrant target"
        raise ValueError(msg)
    points = []
    for document, row in chunks:
        vector_row = vectors[row.chunk_key]
        if vector_row.strategy_id != strategy.strategy_id:
            msg = f"vector for {row.chunk_key} is from {vector_row.strategy_id}"
            raise ValueError(msg)
        vector: dict[str, Any] = {target.dense_vector_name: vector_row.vector}
        if target.sparse_model is not None and target.sparse_vector_name is not None:
            vector[target.sparse_vector_name] = models.Document(
                text=row.chunk_text, model=target.sparse_model
            )
        points.append(
            models.PointStruct(
                id=mapper.point_id(document, row),
                payload=mapper.payload(document, row),
                vector=vector,
            )
        )
    return points
