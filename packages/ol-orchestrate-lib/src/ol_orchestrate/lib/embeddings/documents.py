"""The unit of text a strategy chunks and embeds."""

import hashlib
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

DocumentFormat = Literal["text", "markdown"]


def md5_hex(text: str) -> str:
    """Return the hex MD5 of a text's UTF-8 bytes.

    :param text: the text
    :rtype: str
    """
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


class Document(BaseModel):
    """One thing to embed: a content file, a learning resource, a topic.

    ``doc_key`` identifies the document across strategies, so vectors of the same
    document from different strategies can be compared. ``metadata`` carries
    whatever a context template or a Qdrant payload needs; nothing else reads it.

    :param doc_key: the document's stable key, e.g. Learn's point key
    :param doc_kind: what the document is, e.g. ``content_file``
    :param text: the text to embed; a document without text gets no chunks
    :param format: ``markdown`` lets a chunker split on headers
    :param metadata: values for context templates and payloads
    """

    model_config = ConfigDict(frozen=True)

    doc_key: str
    doc_kind: str
    text: str | None
    format: DocumentFormat = "text"
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def text_checksum(self) -> str | None:
        """The MD5 of ``text``, ``None`` without text.

        :rtype: str | None
        """
        return md5_hex(self.text) if self.text is not None else None
