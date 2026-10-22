"""Documents to chunk rows to vector rows, under a strategy.

The rows are what the platform stores: chunks keyed by ``(doc_key, chunker_id,
chunk_number)`` and vectors keyed by ``(chunk_key, strategy_id)``, each vector
with the checksum of the exact text that was embedded. A text whose checksum
already has a vector under the same model is not sent again, so re-running a
strategy after a few documents changed pays only for the changed chunks, and two
strategies that embed the same text with the same model share the cost.
"""

import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime

from pydantic import BaseModel, ConfigDict

from ol_orchestrate.lib.embeddings.chunking import Chunk, Chunker
from ol_orchestrate.lib.embeddings.documents import Document, md5_hex
from ol_orchestrate.lib.embeddings.models import Embedder, EmbeddingBatch
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy


def chunk_key(doc_key: str, chunker_id: str, chunk_number: int) -> str:
    """Return the key of a chunk: a UUID5 of its document, chunker and number.

    :param doc_key: the document's key
    :param chunker_id: the chunker's id
    :param chunk_number: the chunk's number
    :rtype: str
    """
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_key}|{chunker_id}|{chunk_number}"))


class ChunkRow(BaseModel):
    """One chunk of one document under one chunker."""

    model_config = ConfigDict(frozen=True)

    chunk_key: str
    doc_key: str
    doc_kind: str
    chunker_id: str
    chunk_number: int
    chunk_text: str
    chunk_checksum: str

    @property
    def chunk(self) -> Chunk:
        """The chunk this row stores.

        :rtype: Chunk
        """
        return Chunk(chunk_number=self.chunk_number, text=self.chunk_text)


class VectorRow(BaseModel):
    """The vector of one chunk under one strategy.

    :param input_checksum: MD5 of the text the model was sent, which is the
        chunk text after the strategy's context template
    """

    model_config = ConfigDict(frozen=True)

    chunk_key: str
    strategy_id: str
    model_id: str
    input_checksum: str
    vector: list[float]
    embedded_at: datetime


class EmbedOutcome(BaseModel):
    """The vector rows of a run and what producing them took.

    :param embedded_texts: distinct texts sent to the model
    :param reused_texts: distinct texts whose vector was passed in
    :param input_tokens: tokens the provider reported for ``embedded_texts``
    """

    model_config = ConfigDict(frozen=True)

    rows: list[VectorRow]
    embedded_texts: int
    reused_texts: int
    input_tokens: int


def chunk_documents(
    documents: Iterable[Document], chunker: Chunker
) -> list[tuple[Document, ChunkRow]]:
    """Chunk documents, keeping each chunk with its document.

    :param documents: the documents
    :param chunker: the chunker
    :returns: one pair per non-empty chunk; documents without text have none
    :rtype: list[tuple[Document, ChunkRow]]
    """
    return [
        (
            document,
            ChunkRow(
                chunk_key=chunk_key(
                    document.doc_key, chunker.chunker_id, chunk.chunk_number
                ),
                doc_key=document.doc_key,
                doc_kind=document.doc_kind,
                chunker_id=chunker.chunker_id,
                chunk_number=chunk.chunk_number,
                chunk_text=chunk.text,
                chunk_checksum=md5_hex(chunk.text),
            ),
        )
        for document in documents
        for chunk in chunker.chunk(document)
    ]


def embed_chunks(
    strategy: EmbeddingStrategy,
    chunks: Sequence[tuple[Document, ChunkRow]],
    embedder: Embedder,
    *,
    known_vectors: Mapping[str, list[float]] | None = None,
    embedded_at: datetime | None = None,
) -> EmbedOutcome:
    """Embed chunks under a strategy, sending each distinct new text once.

    :param strategy: the strategy
    :param chunks: chunk rows with their documents, from :func:`chunk_documents`
        with the strategy's chunker
    :param embedder: an embedder for the strategy's model
    :param known_vectors: vectors already computed with the strategy's model at
        its vector length, by the MD5 of the text they were computed from
    :param embedded_at: the time to stamp on new rows; now when omitted
    :returns: one vector row per chunk, in order
    :rtype: EmbedOutcome
    :raises ValueError: when the embedder, a chunk's chunker, a document's kind
        or the length of a known vector does not match the strategy
    """
    if embedder.spec != strategy.model:
        msg = f"embedder is for {embedder.spec.model_id}, not {strategy.model.model_id}"
        raise ValueError(msg)
    for document, row in chunks:
        if row.chunker_id != strategy.chunker.chunker_id:
            msg = f"chunk {row.chunk_key} is from chunker {row.chunker_id}"
            raise ValueError(msg)
        if document.doc_kind != strategy.doc_kind:
            msg = (
                f"{strategy.strategy_id} embeds {strategy.doc_kind}, "
                f"not {document.doc_kind}"
            )
            raise ValueError(msg)

    texts = [strategy.embedding_text(document, row.chunk) for document, row in chunks]
    checksums = [md5_hex(text) for text in texts]
    known = dict(known_vectors or {})
    vector_length = strategy.model.vector_length
    if any(len(vector) != vector_length for vector in known.values()):
        msg = (
            f"known_vectors must be {vector_length} long, as {strategy.model.model_id}"
        )
        raise ValueError(msg)
    reused = len(set(checksums) & set(known))
    new_texts = {
        checksum: text
        for checksum, text in zip(checksums, texts, strict=True)
        if checksum not in known
    }
    # No call at all when every text is known, so an embedder that counts or
    # traces requests records none for a run that embeds nothing.
    batch = (
        embedder.embed(list(new_texts.values()), input_type="document")
        if new_texts
        else EmbeddingBatch(vectors=[], input_tokens=0)
    )
    known.update(zip(new_texts, batch.vectors, strict=True))

    stamp = embedded_at or datetime.now(tz=UTC)
    rows = [
        VectorRow(
            chunk_key=row.chunk_key,
            strategy_id=strategy.strategy_id,
            model_id=strategy.model.model_id,
            input_checksum=checksum,
            vector=known[checksum],
            embedded_at=stamp,
        )
        for (_, row), checksum in zip(chunks, checksums, strict=True)
    ]
    return EmbedOutcome(
        rows=rows,
        embedded_texts=len(new_texts),
        reused_texts=reused,
        input_tokens=batch.input_tokens,
    )
