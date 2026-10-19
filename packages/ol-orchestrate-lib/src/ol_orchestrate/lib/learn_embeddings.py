"""Point ids, chunks and points for MIT Learn's Qdrant collections.

Everything here reproduces what mit-learn's ``vector_search`` package writes, so
that points written by the platform can be compared with Learn's id by id.
``docs/learn_embedding_config.md`` records the deployed configuration this
follows. Requires the ``embeddings`` extra.
"""

import uuid
from collections.abc import Iterable, Sequence
from functools import cache
from typing import Any, Protocol

from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator
from qdrant_client import models

DENSE_MODEL_NAME = "text-embedding-3-large"
SPARSE_MODEL_NAME = "qdrant/bm25"
TOKEN_ENCODING_NAME = "cl100k_base"  # noqa: S105
CHUNK_SIZE_TOKENS = 512
CHUNK_OVERLAP_TOKENS = 51
MARKDOWN_HEADERS_TO_SPLIT_ON = [
    ("#", "Header 1"),
    ("##", "Header 2"),
    ("###", "Header 3"),
    ("####", "Header 4"),
]
MARKDOWN_FILE_TYPE = "marketing_page"
MARKDOWN_FILE_EXTENSION = ".md"

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


SPARSE_VECTOR_NAME = vector_name(SPARSE_MODEL_NAME)


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
    return (
        f"{platform_code or ''}.{resource_readable_id}."
        f"{run_readable_id or ''}.{key}.{chunk_number}"
    )


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


@cache
def _recursive_splitter() -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        encoding_name=TOKEN_ENCODING_NAME,
        chunk_size=CHUNK_SIZE_TOKENS,
        chunk_overlap=CHUNK_OVERLAP_TOKENS,
    )


def _split_markdown(text: str) -> list[str]:
    header_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=MARKDOWN_HEADERS_TO_SPLIT_ON,
        strip_headers=False,
    )
    split_docs = _recursive_splitter().split_documents(header_splitter.split_text(text))
    header_prefixes = {name: prefix for prefix, name in MARKDOWN_HEADERS_TO_SPLIT_ON}
    chunks = []
    for doc in split_docs:
        lost_headers = [
            doc.metadata[name]
            for name, prefix in header_prefixes.items()
            if name in doc.metadata
            and not doc.page_content.startswith(f"{prefix} {doc.metadata[name]}")
        ]
        if lost_headers:
            chunks.append(f"{' > '.join(lost_headers)}\n\n{doc.page_content}")
        else:
            chunks.append(doc.page_content)
    return chunks


def is_markdown(file_type: str | None, file_extension: str | None) -> bool:
    """Whether Learn splits a content file on markdown headers before chunking it.

    :param file_type: the content file's ``file_type``
    :param file_extension: the content file's ``file_extension``
    :rtype: bool
    """
    return file_type == MARKDOWN_FILE_TYPE or file_extension == MARKDOWN_FILE_EXTENSION


def chunk_text(text: str, *, markdown: bool) -> list[tuple[int, str]]:
    """Split a content file's text into the chunks Learn embeds.

    Markdown is split on headers (levels 1 to 4, headers kept) first, and a
    chunk that lost its heading to the token split gets the header path
    prepended. Empty chunks are dropped, but every chunk keeps the index it had
    before they were, because Learn numbers chunks that way and the index is
    part of the point id.

    :param text: the content file's ``content``
    :param markdown: whether to split on markdown headers first
    :returns: ``(chunk_number, chunk_text)`` for every non-empty chunk
    :rtype: list[tuple[int, str]]
    """
    chunks = (
        _split_markdown(text) if markdown else _recursive_splitter().split_text(text)
    )
    return [(number, chunk) for number, chunk in enumerate(chunks) if chunk]


class ContentFileDocument(BaseModel):
    """A content file to chunk and embed.

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
    def _check_payload(self) -> "ContentFileDocument":
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


class DenseEmbedder(Protocol):
    """Anything that turns texts into dense vectors for one named Qdrant vector."""

    @property
    def vector_name(self) -> str:
        """The name of the dense vector in the collection."""
        ...

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        """Return one vector per text, in order."""
        ...


def build_point(
    point_id: str,
    payload: dict[str, Any],
    dense_vector_name: str,
    dense_vector: list[float],
    text: str,
) -> models.PointStruct:
    """Build a point with the dense vector and a BM25 document for Qdrant to encode.

    The sparse vector is computed by Qdrant Cloud inference, so the client that
    uploads the point must be built with ``cloud_inference=True``.

    :param point_id: the point id
    :param payload: the point payload
    :param dense_vector_name: the name of the dense vector
    :param dense_vector: the dense vector
    :param text: the text the dense vector was computed from
    :rtype: models.PointStruct
    """
    return models.PointStruct(
        id=point_id,
        payload=payload,
        vector={
            dense_vector_name: dense_vector,
            SPARSE_VECTOR_NAME: models.Document(text=text, model=SPARSE_MODEL_NAME),
        },
    )


def chunk_and_embed(
    documents: Iterable[ContentFileDocument], embedder: DenseEmbedder
) -> list[models.PointStruct]:
    """Chunk content files and build their content-file collection points.

    Files with no content get no points, as in Learn.

    :param documents: the content files
    :param embedder: computes the dense vectors
    :returns: one point per non-empty chunk
    :rtype: list[models.PointStruct]
    """
    chunked: list[tuple[ContentFileDocument, int, str]] = [
        (document, chunk_number, chunk)
        for document in documents
        if document.content
        for chunk_number, chunk in chunk_text(
            document.content,
            markdown=is_markdown(document.file_type, document.file_extension),
        )
    ]
    vectors = embedder.embed_documents([chunk for _, _, chunk in chunked])
    return [
        build_point(
            point_id(
                content_file_point_key(
                    document.platform_code,
                    document.resource_readable_id,
                    document.run_readable_id,
                    document.key,
                    chunk_number,
                )
            ),
            _content_file_payload(document, chunk_number, chunk),
            embedder.vector_name,
            vector,
            chunk,
        )
        for (document, chunk_number, chunk), vector in zip(
            chunked, vectors, strict=True
        )
    ]


def _content_file_payload(
    document: ContentFileDocument, chunk_number: int, chunk: str
) -> dict[str, Any]:
    # A run-less file stores its resource's readable_id as run_readable_id, but
    # its point key keeps the empty run (mit-learn vector_search/utils.py,
    # _with_run_readable_id_fallback).
    return {
        **document.payload,
        "resource_point_id": point_id(
            resource_point_key(document.platform_code, document.resource_readable_id)
        ),
        "chunk_number": chunk_number,
        "chunk_content": chunk,
        "key": document.key,
        "resource_readable_id": document.resource_readable_id,
        "run_readable_id": document.run_readable_id or document.resource_readable_id,
        "file_type": document.file_type,
        "file_extension": document.file_extension,
    }
