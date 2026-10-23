"""MIT Learn's current embedding configuration, as strategies, and its point contract.

Everything here reproduces what mit-learn's ``vector_search`` package writes, so
that points written by the platform can be compared with Learn's id by id.
``docs/learn_embedding_config.md`` records the deployed configuration this
follows. ``LEARN_V1_CONTENT_FILES`` is Learn's content-file configuration; other
strategies for content files are compared against it.
"""

import uuid
from collections.abc import Iterable
from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator
from qdrant_client import models

from ol_orchestrate.lib.embeddings.chunking import TokenChunker
from ol_orchestrate.lib.embeddings.documents import Document
from ol_orchestrate.lib.embeddings.models import Embedder, EmbeddingModelSpec
from ol_orchestrate.lib.embeddings.pipeline import (
    ChunkRow,
    chunk_documents,
    embed_chunks,
)
from ol_orchestrate.lib.embeddings.qdrant import build_points
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy, QdrantTarget

CONTENT_FILE_DOC_KIND = "content_file"
DENSE_MODEL_NAME = "text-embedding-3-large"
# Learn's vectors are the model's at its default length. Another provider's
# deployment of the same model, e.g. Azure OpenAI's, declares this space too.
DENSE_VECTOR_SPACE = f"{DENSE_MODEL_NAME}@3072"
SPARSE_MODEL_NAME = "qdrant/bm25"
MARKDOWN_FILE_TYPE = "marketing_page"
MARKDOWN_FILE_EXTENSION = ".md"
# The platform's first content-file collection, separate from Learn's live
# ``content_files`` until the cutover (docs/learn_embedding_config.md, decision 1).
PLATFORM_CONTENT_FILES_COLLECTION = "platform_content_files"

# The payload keys of a content-file chunk, from QDRANT_CONTENT_FILE_PARAM_MAP in
# mit-learn vector_search/constants.py.
CONTENT_FILE_PAYLOAD_FIELDS = frozenset(
    {
        "key",
        "course_number",
        "platform",
        "offered_by",
        "file_extension",
        "content_feature_type",
        "run_readable_id",
        "resource_readable_id",
        "run_title",
        "edx_module_id",
        "content_type",
        "description",
        "title",
        "url",
        "file_type",
        "summary",
        "flashcards",
        "checksum",
    }
)
_CONTENT_FILE_ID_FIELDS = frozenset(
    {"key", "resource_readable_id", "run_readable_id", "file_type", "file_extension"}
)


def vector_name(model_name: str) -> str:
    """Name a Qdrant vector the way Learn does: the second ``/`` segment, if any.

    ``text-embedding-3-large`` stays as it is and ``qdrant/bm25`` becomes ``bm25``.

    :param model_name: the embedding model name
    :returns: the vector name Learn's collections use for the model
    :rtype: str
    """
    segments = model_name.split("/")
    return segments[1] if len(segments) > 1 else model_name


LEARN_V1_CONTENT_FILES = EmbeddingStrategy(
    strategy_id="learn_v1_content_files",
    doc_kind=CONTENT_FILE_DOC_KIND,
    chunker=TokenChunker(
        encoding="cl100k_base",
        chunk_size=512,
        chunk_overlap=51,
        markdown_header_levels=4,
    ),
    model=EmbeddingModelSpec(
        model=f"openai:{DENSE_MODEL_NAME}", vector_space=DENSE_VECTOR_SPACE
    ),
    role="serving",
    qdrant=QdrantTarget(
        collection=PLATFORM_CONTENT_FILES_COLLECTION,
        dense_vector_name=vector_name(DENSE_MODEL_NAME),
        sparse_model=SPARSE_MODEL_NAME,
        sparse_vector_name=vector_name(SPARSE_MODEL_NAME),
    ),
    notes=(
        "MIT Learn's content-file configuration at mit-learn 2662d01e4: "
        "LiteLLMEncoder with text-embedding-3-large at its default length, "
        "RecursiveCharacterTextSplitter.from_tiktoken_encoder(cl100k_base, 512, 51), "
        "markdown header splitting to level 4, BM25 by Qdrant cloud inference."
    ),
)


def point_id(key: str) -> str:
    """Return the Qdrant point id Learn derives from a point key.

    :param key: one of the keys built by the ``*_point_key`` functions
    :returns: ``uuid5(NAMESPACE_DNS, key)`` as a string
    :rtype: str
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


def resource_point_key(platform_code: str | None, readable_id: str) -> str:
    """Key of a learning resource's point in the resources collection.

    :param platform_code: the resource's ``platform.code``, ``None`` without one
    :param readable_id: the resource's ``readable_id``
    :returns: ``{platform}.{readable_id}``
    :rtype: str
    """
    return f"{platform_code or ''}.{readable_id}"


def content_file_point_key(
    platform_code: str | None,
    resource_readable_id: str,
    run_readable_id: str | None,
    key: str,
    chunk_number: int,
) -> str:
    """Key of one chunk of a content file in the content files collection.

    :param platform_code: the parent resource's ``platform.code``
    :param resource_readable_id: the parent resource's ``readable_id``
    :param run_readable_id: the run's ``run_id``, ``None`` for a run-less file
    :param key: the content file's ``key``
    :param chunk_number: the chunk's index among all chunks of the file
    :returns: ``{platform}.{resource_readable_id}.{run_readable_id}.{key}.{chunk}``
    :rtype: str
    """
    doc_key = content_file_doc_key(
        platform_code, resource_readable_id, run_readable_id, key
    )
    return f"{doc_key}.{chunk_number}"


def content_file_doc_key(
    platform_code: str | None,
    resource_readable_id: str,
    run_readable_id: str | None,
    key: str,
) -> str:
    """Key of a content file as a document: its point keys without the chunk.

    :param platform_code: the parent resource's ``platform.code``
    :param resource_readable_id: the parent resource's ``readable_id``
    :param run_readable_id: the run's ``run_id``, ``None`` for a run-less file
    :param key: the content file's ``key``
    :returns: ``{platform}.{resource_readable_id}.{run_readable_id}.{key}``
    :rtype: str
    """
    return f"{platform_code or ''}.{resource_readable_id}.{run_readable_id or ''}.{key}"


def resource_metadata_point_key(
    platform_code: str | None, readable_id: str, chunk_number: int
) -> str:
    """Key of one chunk of a resource's metadata document.

    These chunks go in the content files collection, next to the file chunks.

    :param platform_code: the resource's ``platform.code``
    :param readable_id: the resource's ``readable_id``
    :param chunk_number: the chunk's index
    :returns: ``{platform}.{readable_id}.course_information.{chunk_number}``
    :rtype: str
    """
    return f"{platform_code or ''}.{readable_id}.course_information.{chunk_number}"


def is_markdown(file_type: str | None, file_extension: str | None) -> bool:
    """Whether Learn splits a content file on markdown headers before chunking it.

    :param file_type: the content file's ``file_type``
    :param file_extension: the content file's ``file_extension``
    :rtype: bool
    """
    return file_type == MARKDOWN_FILE_TYPE or file_extension == MARKDOWN_FILE_EXTENSION


class ContentFileDocument(BaseModel):
    """A content file as Learn's chunking and payload see it.

    The fields other than ``content`` and ``payload`` go into the point id or
    pick the splitter. ``payload`` carries the rest of the chunk payload, keyed as
    Learn's ``ContentFileSerializer`` keys it (e.g. ``platform`` and
    ``offered_by`` are objects with a ``code``).
    """

    model_config = ConfigDict(frozen=True)

    key: str
    content: str | None
    resource_readable_id: str
    run_readable_id: str | None
    platform_code: str | None
    file_type: str | None
    file_extension: str | None
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_payload(self) -> Self:
        unknown = set(self.payload) - CONTENT_FILE_PAYLOAD_FIELDS
        if unknown:
            msg = f"payload keys Learn does not store on a chunk: {sorted(unknown)}"
            raise ValueError(msg)
        duplicated = set(self.payload) & _CONTENT_FILE_ID_FIELDS
        if duplicated:
            msg = (
                f"payload keys that are set from the model fields: {sorted(duplicated)}"
            )
            raise ValueError(msg)
        platform = self.payload.get("platform", {"code": self.platform_code or ""})
        if not isinstance(platform, dict) or platform.get("code") != (
            self.platform_code or ""
        ):
            msg = "payload platform.code does not match platform_code"
            raise ValueError(msg)
        return self

    def to_document(self) -> Document:
        """Return the generic document, with every field in its metadata.

        :rtype: Document
        """
        return Document(
            doc_key=content_file_doc_key(
                self.platform_code,
                self.resource_readable_id,
                self.run_readable_id,
                self.key,
            ),
            doc_kind=CONTENT_FILE_DOC_KIND,
            text=self.content,
            format="markdown"
            if is_markdown(self.file_type, self.file_extension)
            else "text",
            metadata={
                **self.payload,
                "key": self.key,
                "resource_readable_id": self.resource_readable_id,
                "run_readable_id": self.run_readable_id,
                "platform_code": self.platform_code,
                "file_type": self.file_type,
                "file_extension": self.file_extension,
            },
        )


class ContentFilePointMapper:
    """Learn's point ids and payloads for content-file chunks.

    Reads the metadata :meth:`ContentFileDocument.to_document` writes.
    """

    def point_id(self, document: Document, row: ChunkRow) -> str:
        """Return Learn's id of the chunk's point.

        :param document: the content file
        :param row: the chunk
        :rtype: str
        """
        metadata = document.metadata
        return point_id(
            content_file_point_key(
                metadata["platform_code"],
                metadata["resource_readable_id"],
                metadata["run_readable_id"],
                metadata["key"],
                row.chunk_number,
            )
        )

    def payload(self, document: Document, row: ChunkRow) -> dict[str, Any]:
        """Return the payload Learn stores on the chunk's point.

        A run-less file stores its resource's readable_id as run_readable_id, but
        its point key keeps the empty run (mit-learn vector_search/utils.py,
        ``_with_run_readable_id_fallback``).

        :param document: the content file
        :param row: the chunk
        :rtype: dict[str, Any]
        """
        metadata = document.metadata
        return {
            **{
                name: value
                for name, value in metadata.items()
                if name in CONTENT_FILE_PAYLOAD_FIELDS
            },
            "resource_point_id": point_id(
                resource_point_key(
                    metadata["platform_code"], metadata["resource_readable_id"]
                )
            ),
            "chunk_number": row.chunk_number,
            "chunk_content": row.chunk_text,
            "run_readable_id": metadata["run_readable_id"]
            or metadata["resource_readable_id"],
        }


def chunk_and_embed(
    documents: Iterable[ContentFileDocument],
    embedder: Embedder,
    strategy: EmbeddingStrategy = LEARN_V1_CONTENT_FILES,
) -> list[models.PointStruct]:
    """Chunk and embed content files into the points Learn's collection holds.

    Files with no content get no points, as in Learn.

    :param documents: the content files
    :param embedder: computes the dense vectors for the strategy's model
    :param strategy: a content-file strategy with a Qdrant target
    :returns: one point per non-empty chunk
    :rtype: list[models.PointStruct]
    """
    chunks = chunk_documents(
        [document.to_document() for document in documents], strategy.chunker
    )
    outcome = embed_chunks(strategy, chunks, embedder)
    return build_points(
        strategy,
        chunks,
        {row.chunk_key: row for row in outcome.rows},
        ContentFilePointMapper(),
    )
