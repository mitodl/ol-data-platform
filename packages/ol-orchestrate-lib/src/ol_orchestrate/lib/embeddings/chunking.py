"""Chunkers: deterministic, parameter-pinned ways to split a document into chunks.

A chunker is configuration, not code: its parameters are its identity, and
``chunker_id`` is built from them, so two strategies that chunk the same way
share chunk rows and a change of parameters is a new id rather than a silent
change. ``Chunker`` is the union a strategy file names a chunker with.
"""

from functools import cache
from typing import Annotated, Literal, Self

import tiktoken
from langchain_text_splitters import (
    MarkdownHeaderTextSplitter,
    RecursiveCharacterTextSplitter,
)
from pydantic import BaseModel, ConfigDict, Field, model_validator

from ol_orchestrate.lib.embeddings.documents import Document

MAX_MARKDOWN_HEADER_LEVEL = 6


class Chunk(BaseModel):
    """One piece of a document.

    :param chunk_number: the chunk's index among all the pieces the splitter
        produced, counting empty ones that were dropped
    :param text: the chunk's text
    """

    model_config = ConfigDict(frozen=True)

    chunk_number: int
    text: str


@cache
def _token_splitter(
    encoding: str, chunk_size: int, chunk_overlap: int
) -> RecursiveCharacterTextSplitter:
    return RecursiveCharacterTextSplitter.from_tiktoken_encoder(
        encoding_name=encoding, chunk_size=chunk_size, chunk_overlap=chunk_overlap
    )


class TokenChunker(BaseModel):
    """Recursive character splitting measured in tokens of a tiktoken encoding.

    With ``markdown_header_levels`` set, a markdown document is first split on
    headers of levels 1 to ``markdown_header_levels`` (headers kept in the text),
    and a chunk that lost its heading to the token split gets the header path
    prepended. Other documents, and markdown with the levels at 0, are split on
    tokens only.

    :param encoding: the tiktoken encoding that measures chunk size
    :param chunk_size: tokens per chunk
    :param chunk_overlap: tokens shared by consecutive chunks
    :param markdown_header_levels: deepest header level to split markdown on
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["token"] = "token"
    encoding: str
    chunk_size: int = Field(gt=0)
    chunk_overlap: int = Field(ge=0)
    markdown_header_levels: int = Field(default=0, ge=0, le=MAX_MARKDOWN_HEADER_LEVEL)

    @model_validator(mode="after")
    def _check_overlap(self) -> Self:
        if self.chunk_overlap >= self.chunk_size:
            msg = "chunk_overlap must be smaller than chunk_size"
            raise ValueError(msg)
        return self

    @property
    def chunker_id(self) -> str:
        """``token:{encoding}:{size}:{overlap}``, plus ``:md{levels}`` if set.

        :rtype: str
        """
        base = f"token:{self.encoding}:{self.chunk_size}:{self.chunk_overlap}"
        return (
            f"{base}:md{self.markdown_header_levels}"
            if self.markdown_header_levels
            else base
        )

    def chunk(self, document: Document) -> list[Chunk]:
        """Split a document's text, dropping empty chunks but not their numbers.

        :param document: the document
        :returns: the non-empty chunks; none for a document without text
        :rtype: list[Chunk]
        """
        if not document.text:
            return []
        splitter = _token_splitter(self.encoding, self.chunk_size, self.chunk_overlap)
        if document.format == "markdown" and self.markdown_header_levels:
            pieces = self._split_markdown(document.text, splitter)
        else:
            pieces = splitter.split_text(document.text)
        return [
            Chunk(chunk_number=number, text=piece)
            for number, piece in enumerate(pieces)
            if piece
        ]

    def _split_markdown(
        self, text: str, splitter: RecursiveCharacterTextSplitter
    ) -> list[str]:
        headers = [
            ("#" * level, f"Header {level}")
            for level in range(1, self.markdown_header_levels + 1)
        ]
        header_splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=headers, strip_headers=False
        )
        pieces = []
        for doc in splitter.split_documents(header_splitter.split_text(text)):
            lost_headers = [
                doc.metadata[name]
                for prefix, name in headers
                if name in doc.metadata
                and not doc.page_content.startswith(f"{prefix} {doc.metadata[name]}")
            ]
            if lost_headers:
                pieces.append(f"{' > '.join(lost_headers)}\n\n{doc.page_content}")
            else:
                pieces.append(doc.page_content)
        return pieces


class WholeDocumentChunker(BaseModel):
    """The whole document as one chunk, cut to a token limit.

    The limit is required because a model rejects an input over its own limit
    (8,191 tokens for OpenAI's), which would fail the whole request. Learn cuts
    a resource's text to the model limit the same way.

    :param encoding: the tiktoken encoding that counts ``max_tokens``
    :param max_tokens: keep only the first this many tokens
    """

    model_config = ConfigDict(frozen=True)

    kind: Literal["whole"] = "whole"
    encoding: str
    max_tokens: int = Field(gt=0)

    @property
    def chunker_id(self) -> str:
        """``whole:{encoding}:{max_tokens}``.

        :rtype: str
        """
        return f"whole:{self.encoding}:{self.max_tokens}"

    def chunk(self, document: Document) -> list[Chunk]:
        """Return the document's text, cut to ``max_tokens``, as chunk 0.

        Special-token strings such as ``<|endoftext|>`` are counted as text.

        :param document: the document
        :returns: one chunk, or none for a document without text
        :rtype: list[Chunk]
        """
        if not document.text:
            return []
        text = document.text
        encoding = tiktoken.get_encoding(self.encoding)
        tokens = encoding.encode(text, disallowed_special=())
        if len(tokens) > self.max_tokens:
            text = encoding.decode(tokens[: self.max_tokens])
        return [Chunk(chunk_number=0, text=text)]


Chunker = Annotated[TokenChunker | WholeDocumentChunker, Field(discriminator="kind")]
