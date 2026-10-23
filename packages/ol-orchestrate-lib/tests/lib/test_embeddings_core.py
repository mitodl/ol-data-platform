"""Chunkers, strategies, the embedding pipeline and the ol-llm embedder."""

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest
from ol_llm import (
    Attribution,
    Settings,
    TracingConfig,
    UseCase,
    configure,
    configure_tracing,
)
from ol_llm.embeddings import override_embedding_models
from ol_orchestrate.lib.embeddings.chunking import TokenChunker, WholeDocumentChunker
from ol_orchestrate.lib.embeddings.documents import Document, md5_hex
from ol_orchestrate.lib.embeddings.models import (
    EmbeddingBatch,
    EmbeddingModelSpec,
    OlLlmEmbedder,
)
from ol_orchestrate.lib.embeddings.pipeline import (
    chunk_documents,
    chunk_key,
    embed_chunks,
)
from ol_orchestrate.lib.embeddings.strategy import (
    EmbeddingStrategy,
    load_strategies,
)
from pydantic import ValidationError
from pydantic_ai.embeddings import TestEmbeddingModel
from pydantic_ai.embeddings.result import EmbedInputType

MODEL = EmbeddingModelSpec(model="openai:text-embedding-3-large", dimensions=4)
WHOLE = WholeDocumentChunker(encoding="cl100k_base", max_tokens=100)


class FakeEmbedder:
    """Returns ``[len(text), 1, 0, 0]`` and records each call's texts."""

    def __init__(self, spec: EmbeddingModelSpec = MODEL) -> None:
        self.spec = spec
        self.calls: list[list[str]] = []
        self.input_types: list[EmbedInputType] = []

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        self.calls.append(list(texts))
        self.input_types.append(input_type)
        return EmbeddingBatch(
            vectors=[[float(len(text)), 1.0, 0.0, 0.0] for text in texts],
            input_tokens=sum(len(text) for text in texts),
        )


def _strategy(**overrides: object) -> EmbeddingStrategy:
    fields: dict[str, object] = {
        "strategy_id": "exp_whole",
        "doc_kind": "page",
        "chunker": WHOLE,
        "model": MODEL,
    }
    fields.update(overrides)
    return EmbeddingStrategy.model_validate(fields)


def _document(doc_key: str, text: str | None, **metadata: object) -> Document:
    return Document(doc_key=doc_key, doc_kind="page", text=text, metadata=metadata)


def test_chunker_ids_carry_their_parameters() -> None:
    assert (
        TokenChunker(
            encoding="cl100k_base", chunk_size=512, chunk_overlap=51
        ).chunker_id
        == "token:cl100k_base:512:51"
    )
    assert (
        TokenChunker(
            encoding="cl100k_base",
            chunk_size=1024,
            chunk_overlap=100,
            markdown_header_levels=3,
        ).chunker_id
        == "token:cl100k_base:1024:100:md3"
    )
    assert WHOLE.chunker_id == "whole:cl100k_base:100"


@pytest.mark.parametrize(
    "chunker",
    [
        {
            "kind": "token",
            "encoding": "cl100k_base",
            "chunk_size": 10,
            "chunk_overlap": 10,
        },
        {"kind": "whole", "encoding": "cl100k_base"},
        {"kind": "whole", "max_tokens": 5},
        {"kind": "whole"},
    ],
)
def test_bad_chunkers_are_rejected(chunker: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        _strategy(chunker=chunker)


def test_whole_document_chunker_cuts_to_the_token_limit() -> None:
    chunker = WholeDocumentChunker(encoding="cl100k_base", max_tokens=3)
    [chunk] = chunker.chunk(_document("a", "one two three four five"))
    assert chunk.text == "one two three"
    assert WHOLE.chunk(_document("b", "")) == []
    [chunk] = WHOLE.chunk(_document("c", "ends <|endoftext|>"))
    assert chunk.text == "ends <|endoftext|>"


def test_markdown_split_only_applies_to_markdown_documents() -> None:
    chunker = TokenChunker(
        encoding="cl100k_base",
        chunk_size=512,
        chunk_overlap=0,
        markdown_header_levels=2,
    )
    text = "# Title\n\nIntro.\n\n## Part\n\nBody."
    markdown = Document(doc_key="m", doc_kind="page", text=text, format="markdown")
    assert [chunk.text for chunk in chunker.chunk(markdown)] == [
        "# Title  \nIntro.",
        "Title\n\n## Part  \nBody.",
    ]
    assert len(chunker.chunk(_document("t", text))) == 1


def test_context_template_decorates_only_the_embedded_text() -> None:
    strategy = _strategy(context_template="{title}\n\n{chunk}")
    [(document, row)] = chunk_documents(
        [_document("a", "body", title="Course")], strategy.chunker
    )
    embedder = FakeEmbedder()

    outcome = embed_chunks(strategy, [(document, row)], embedder)

    assert embedder.calls == [["Course\n\nbody"]]
    assert row.chunk_text == "body"
    assert outcome.rows[0].input_checksum == md5_hex("Course\n\nbody")


@pytest.mark.parametrize(
    ("template", "message"),
    [("{title}", "must contain"), ("{chunk} {}", "must be named")],
)
def test_bad_templates_are_rejected(template: str, message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        _strategy(context_template=template)


def test_a_missing_template_key_fails() -> None:
    strategy = _strategy(context_template="{title}: {chunk}")
    chunks = chunk_documents([_document("a", "body")], strategy.chunker)
    with pytest.raises(KeyError, match="title"):
        embed_chunks(strategy, chunks, FakeEmbedder())


def test_chunk_keys_are_stable_and_distinct_per_chunker() -> None:
    assert chunk_key("doc", "whole", 0) == chunk_key("doc", "whole", 0)
    assert chunk_key("doc", "whole", 0) != chunk_key("doc", "token:x:1:0", 0)
    assert chunk_key("doc", "whole", 0) != chunk_key("doc", "whole", 1)


def test_each_distinct_new_text_is_embedded_once() -> None:
    strategy = _strategy()
    chunks = chunk_documents(
        [
            _document("a", "same"),
            _document("b", "same"),
            _document("c", "known"),
            _document("d", None),
        ],
        strategy.chunker,
    )
    embedder = FakeEmbedder()
    stamp = datetime(2026, 10, 20, tzinfo=UTC)

    outcome = embed_chunks(
        strategy,
        chunks,
        embedder,
        known_vectors={md5_hex("known"): [9.0, 9.0, 9.0, 9.0]},
        embedded_at=stamp,
    )

    assert embedder.calls == [["same"]]
    assert embedder.input_types == ["document"]
    assert [row.vector for row in outcome.rows] == [
        [4.0, 1.0, 0.0, 0.0],
        [4.0, 1.0, 0.0, 0.0],
        [9.0, 9.0, 9.0, 9.0],
    ]
    assert {(row.model_id, row.vector_space) for row in outcome.rows} == {
        ("openai:text-embedding-3-large@4", "openai:text-embedding-3-large@4")
    }
    assert {row.embedded_at for row in outcome.rows} == {stamp}
    assert (outcome.embedded_texts, outcome.reused_texts, outcome.input_tokens) == (
        1,
        1,
        4,
    )


def test_embed_chunks_makes_no_call_when_every_text_is_known() -> None:
    strategy = _strategy()
    chunks = chunk_documents([_document("a", "known")], strategy.chunker)
    embedder = FakeEmbedder()

    outcome = embed_chunks(
        strategy,
        chunks,
        embedder,
        known_vectors={md5_hex("known"): [9.0, 9.0, 9.0, 9.0]},
    )

    assert embedder.calls == []
    assert (outcome.embedded_texts, outcome.input_tokens) == (0, 0)


def test_known_vectors_must_have_the_model_length() -> None:
    strategy = _strategy()
    chunks = chunk_documents([_document("a", "text")], strategy.chunker)
    with pytest.raises(ValueError, match="must be 4 long"):
        embed_chunks(
            strategy,
            chunks,
            FakeEmbedder(),
            known_vectors={md5_hex("other"): [1.0, 2.0]},
        )


def test_requests_are_cut_by_count_and_tokens() -> None:
    spec = EmbeddingModelSpec(
        model="openai:text-embedding-3-large", batch_size=3, max_request_tokens=4
    )
    # "one two" is two cl100k tokens, "one two three four" four.
    assert list(spec.request_batches(["one two"] * 4)) == [
        ["one two", "one two"],
        ["one two", "one two"],
    ]
    assert list(spec.request_batches(["a", "b", "c", "d"])) == [
        ["a", "b", "c"],
        ["d"],
    ]
    assert list(spec.request_batches(["one two three four", "a"])) == [
        ["one two three four"],
        ["a"],
    ]
    with pytest.raises(ValueError, match="over max_request_tokens"):
        list(spec.request_batches(["one two three four five"]))


def test_mismatched_inputs_are_rejected() -> None:
    strategy = _strategy()
    chunks = chunk_documents([_document("a", "text")], strategy.chunker)
    with pytest.raises(ValueError, match="embedder is for"):
        embed_chunks(
            strategy,
            chunks,
            FakeEmbedder(EmbeddingModelSpec(model="openai:x", dimensions=4)),
        )
    other_chunker = _strategy(
        chunker={
            "kind": "token",
            "encoding": "cl100k_base",
            "chunk_size": 8,
            "chunk_overlap": 0,
        }
    )
    with pytest.raises(ValueError, match="from chunker"):
        embed_chunks(other_chunker, chunks, FakeEmbedder())
    with pytest.raises(ValueError, match="embeds topic"):
        embed_chunks(_strategy(doc_kind="topic"), chunks, FakeEmbedder())


def test_strategies_load_from_yaml(tmp_path: Path) -> None:
    path = tmp_path / "strategies.yaml"
    path.write_text(
        """
strategies:
  - strategy_id: exp_small_chunks
    doc_kind: content_file
    chunker: {kind: token, encoding: cl100k_base, chunk_size: 256, chunk_overlap: 25}
    model: {model: "openai:text-embedding-3-large", dimensions: 1024}
    context_template: "{title}\\n\\n{chunk}"
    qdrant:
      collection: platform_content_files_exp
      dense_vector_name: te3l-1024
"""
    )
    [strategy] = load_strategies(path).values()
    assert strategy.chunker.chunker_id == "token:cl100k_base:256:25"
    assert strategy.model.model_id == "openai:text-embedding-3-large@1024"
    assert strategy.role == "candidate"

    path.write_text(
        path.read_text()
        + """  - strategy_id: exp_small_chunks
    doc_kind: content_file
    chunker: {kind: whole, encoding: cl100k_base, max_tokens: 8191}
    model: {model: "openai:text-embedding-3-large"}
"""
    )
    with pytest.raises(ValidationError, match="more than once"):
        load_strategies(path)


def test_model_id_takes_the_catalog_length_by_default() -> None:
    assert (
        EmbeddingModelSpec(model="openai:text-embedding-3-large").model_id
        == "openai:text-embedding-3-large@3072"
    )
    with pytest.raises(ValueError, match="set dimensions"):
        _ = EmbeddingModelSpec(model="google:gemini-embedding-001").model_id


def test_vector_space_is_the_model_unless_declared() -> None:
    openai = EmbeddingModelSpec(model="openai:text-embedding-3-large")
    azure = EmbeddingModelSpec(model="azure:text-embedding-3-large")
    assert openai.vector_space_id == "openai:text-embedding-3-large@3072"
    assert azure.vector_space_id != openai.vector_space_id

    shared = "text-embedding-3-large@3072"
    declared = [
        spec.model_copy(update={"vector_space": shared}) for spec in (openai, azure)
    ]
    assert {spec.vector_space_id for spec in declared} == {shared}
    assert {spec.model_id for spec in declared} == {
        "openai:text-embedding-3-large@3072",
        "azure:text-embedding-3-large@3072",
    }
    with pytest.raises(ValidationError):
        EmbeddingModelSpec(model="openai:text-embedding-3-large", vector_space="")


@pytest.fixture
def ol_llm_settings() -> None:
    configure(Settings(environment="ci"))
    configure_tracing(TracingConfig(enabled=False))


def _ol_llm_embedder(spec: EmbeddingModelSpec) -> OlLlmEmbedder:
    return OlLlmEmbedder(
        spec,
        UseCase(
            app="ol-data-platform", name="embed-test", profile="batch", model=spec.model
        ),
        Attribution(run_id="run-1"),
    )


@pytest.mark.usefixtures("ol_llm_settings")
def test_ol_llm_embedder_batches_requests() -> None:
    spec = EmbeddingModelSpec(model="openai:text-embedding-3-large", batch_size=2)
    texts = ["a", "b", "c", "d", "e"]
    with override_embedding_models(TestEmbeddingModel(dimensions=3072)):
        batch = _ol_llm_embedder(spec).embed(texts, input_type="document")
    assert len(batch.vectors) == len(texts)
    assert {len(vector) for vector in batch.vectors} == {3072}


@pytest.mark.usefixtures("ol_llm_settings")
def test_ol_llm_embedder_rejects_vectors_of_the_wrong_length() -> None:
    spec = EmbeddingModelSpec(model="openai:text-embedding-3-large")
    with (
        override_embedding_models(TestEmbeddingModel(dimensions=8)),
        pytest.raises(ValueError, match="not 3072"),
    ):
        _ol_llm_embedder(spec).embed(["a"], input_type="document")


def test_ol_llm_embedder_needs_the_use_case_to_call_its_model() -> None:
    with pytest.raises(ValueError, match="is not"):
        OlLlmEmbedder(
            MODEL,
            UseCase(
                app="ol-data-platform",
                name="embed-test",
                profile="batch",
                model="openai:other",
            ),
            Attribution(),
        )
