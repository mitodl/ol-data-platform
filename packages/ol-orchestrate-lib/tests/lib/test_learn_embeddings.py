"""Parity of the Learn embedding helpers with MIT Learn's own code.

The expected values come from ``mit_learn_vector_search``, a copy of Learn's
functions, run on the documents Learn's serializers would produce.
"""

from collections.abc import Sequence
from typing import Any
from unittest.mock import patch

import pytest
from ol_orchestrate.lib import learn_embeddings
from ol_orchestrate.lib.learn_embeddings import (
    ContentFileDocument,
    chunk_and_embed,
    chunk_text,
    content_file_point_key,
    point_id,
    resource_metadata_point_key,
    resource_point_key,
    vector_name,
)
from qdrant_client import models

from tests.lib import mit_learn_vector_search as learn

PLATFORM = {"code": "mitxonline", "name": "MITx Online"}
OFFERED_BY = {"code": "mitx", "name": "MITx"}
RUN = "course-v1:MITxT+18.01.1x+3T2026"

SENTENCES = [
    "The derivative measures how a function changes as its input changes.",
    "Integration accumulates quantities such as area under a curve.",
    "Limits make both ideas precise; they describe values approached, not reached.",
    "Problem sets ask learners to apply the chain rule, product rule and quotient rule.",  # noqa: E501
    "Office hours are held weekly, and recordings are posted to the course site.",
]


def _paragraphs(count: int) -> str:
    return "\n\n".join(
        " ".join(SENTENCES[(i + j) % len(SENTENCES)] for j in range(7))
        for i in range(count)
    )


PLAIN_TEXT = _paragraphs(40)
ONE_LINE_TEXT = " ".join(SENTENCES * 40)
MARKDOWN_TEXT = "\n\n".join(
    [
        "# Calculus 1A: Differentiation",
        "Short overview of the course.",
        "## Course format",
        _paragraphs(25),
        "### Grading",
        "Homework 40%, exams 60%.",
        "#### Late policy",
        _paragraphs(3),
        "## Instructors",
        "Prof. A and Prof. B.",
    ]
)


class FakeEmbedder:
    """Returns the text's length as a one-element vector."""

    vector_name = "text-embedding-3-large"

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def embed_documents(self, texts: Sequence[str]) -> list[list[float]]:
        self.calls.append(list(texts))
        return [[float(len(text))] for text in texts]


def expected_chunks(document: ContentFileDocument) -> list[tuple[int, str]]:
    return [
        (payload["chunk_number"], payload["chunk_content"])
        for _, payload in learn.content_file_chunks(_learn_document(document))
    ]


def _learn_document(document: ContentFileDocument) -> dict[str, Any]:
    """Build the dict ContentFileSerializer would produce for ``document``.

    A run-less file has no ``run_readable_id`` key at all, because the serializer
    field is ``CharField(source="run.run_id", required=False)``.
    """
    serialized = {
        "id": 1,
        "content": document.content,
        "key": document.key,
        "resource_readable_id": document.resource_readable_id,
        "file_type": document.file_type,
        "file_extension": document.file_extension,
        **document.payload,
    }
    if document.run_readable_id is not None:
        serialized["run_readable_id"] = document.run_readable_id
    return serialized


def _content_file(**overrides: Any) -> ContentFileDocument:
    fields: dict[str, Any] = {
        "key": f"{RUN}/block-v1:MITxT+18.01.1x+3T2026+type@html+block@intro",
        "content": PLAIN_TEXT,
        "resource_readable_id": "course-v1:MITxT+18.01.1x",
        "run_readable_id": RUN,
        "platform_code": "mitxonline",
        "file_type": "html",
        "file_extension": ".html",
        "payload": {
            "platform": PLATFORM,
            "offered_by": OFFERED_BY,
            "title": "Introduction",
            "url": "https://courses.learn.mit.edu/x",
            "edx_module_id": "block-v1:MITxT+18.01.1x+3T2026+type@html+block@intro",
            "checksum": "0123abcd",
            "description": None,
        },
    }
    fields.update(overrides)
    return ContentFileDocument(**fields)


@pytest.mark.parametrize(
    "model_name", ["text-embedding-3-large", "qdrant/bm25", "a/b/c", "x/"]
)
def test_vector_name_matches_learn(model_name: str) -> None:
    assert vector_name(model_name) == learn.model_short_name(model_name)


def test_point_id_is_pinned() -> None:
    """Pins the namespace, so a change to both copies still fails."""
    assert (
        point_id(resource_point_key("mitxonline", "course-v1:MITxT+18.01x"))
        == "396a391a-aea8-551b-bcdd-32574d1bcb23"
    )


@pytest.mark.parametrize(
    "platform",
    [PLATFORM, None],
)
def test_resource_keys_match_learn(platform: dict[str, str] | None) -> None:
    code = platform["code"] if platform else None
    document = {"platform": platform, "readable_id": "PROG-1"}
    assert resource_point_key(code, "PROG-1") == learn.vector_point_key(document)
    for chunk_number in (0, 7):
        assert resource_metadata_point_key(
            code, "PROG-1", chunk_number
        ) == learn.vector_point_key(
            document, chunk_number=chunk_number, document_type="course_information"
        )


@pytest.mark.parametrize("run_readable_id", ["course-v1:MITxT+18.01.1x+3T2026", None])
def test_content_file_key_matches_learn(run_readable_id: str | None) -> None:
    document = _content_file(run_readable_id=run_readable_id)
    serialized = _learn_document(document)
    serialized["platform"] = PLATFORM
    assert content_file_point_key(
        "mitxonline",
        document.resource_readable_id,
        run_readable_id,
        document.key,
        3,
    ) == learn.vector_point_key(
        serialized, chunk_number=3, document_type="content_file"
    )


@pytest.mark.parametrize(
    "document",
    [
        _content_file(),
        _content_file(run_readable_id=None),
        _content_file(
            content=MARKDOWN_TEXT,
            file_type="marketing_page",
            file_extension=".md",
            key="https://learn.mit.edu/courses/calculus",
            run_readable_id=None,
        ),
        _content_file(content=ONE_LINE_TEXT),
        _content_file(content=MARKDOWN_TEXT, file_extension=".md"),
        _content_file(
            platform_code=None,
            payload={"platform": {"code": "", "name": ""}, "title": "No platform"},
        ),
    ],
    ids=["run", "run-less", "marketing-page", "one-line", "md-file", "no-platform"],
)
def test_chunk_and_embed_matches_learn(document: ContentFileDocument) -> None:
    expected = learn.content_file_chunks(_learn_document(document))
    embedder = FakeEmbedder()

    points = chunk_and_embed([document], embedder)

    assert len(expected) > 1
    assert [(point.id, point.payload) for point in points] == expected
    for point in points:
        assert isinstance(point.vector, dict)
        assert point.payload
        chunk = point.payload["chunk_content"]
        assert point.vector["text-embedding-3-large"] == [float(len(chunk))]
        assert point.vector["bm25"] == models.Document(text=chunk, model="qdrant/bm25")
    assert embedder.calls == [[chunk for _, chunk in expected_chunks(document)]]


def test_markdown_chunks_keep_their_headers() -> None:
    """Each chunk is prefixed with the headers its own text does not start with."""
    chunks = [chunk for _, chunk in chunk_text(MARKDOWN_TEXT, markdown=True)]

    assert (
        chunks[0] == "# Calculus 1A: Differentiation  \nShort overview of the course."
    )
    assert chunks[1].startswith("Calculus 1A: Differentiation\n\n## Course format")
    assert chunks[2].startswith("Calculus 1A: Differentiation > Course format\n\n")


def test_empty_chunks_keep_their_numbers() -> None:
    class Splitter:
        def split_text(self, _text: str) -> list[str]:
            return ["a", "", "b"]

    with patch.object(learn_embeddings, "_recursive_splitter", Splitter):
        assert chunk_text("ignored", markdown=False) == [(0, "a"), (2, "b")]


def test_files_without_content_get_no_points() -> None:
    embedder = FakeEmbedder()
    assert chunk_and_embed([_content_file(content="")], embedder) == []
    assert learn.content_file_chunks(_learn_document(_content_file(content=""))) == []


@pytest.mark.parametrize(
    ("payload", "message"),
    [
        ({"views": 3}, "does not store"),
        ({"key": "x"}, "set from the model fields"),
        ({"platform": {"code": "xpro", "name": "xPRO"}}, "does not match"),
    ],
)
def test_bad_payloads_are_rejected(payload: dict[str, Any], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _content_file(payload=payload)
