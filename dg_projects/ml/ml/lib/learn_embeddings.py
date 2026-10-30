"""MIT Learn's content files and topics as embedding chunks and vectors.

The platform's own copy of what Learn writes to its ``content_files`` and
``topics`` Qdrant collections, under the strategies in
``ml/embedding_strategies.yaml``. Chunks and vectors are built with
:mod:`ol_orchestrate.lib.embeddings`; this module turns the warehouse rows into
its documents and its rows into frames, and carries the stored vectors of a
vector space into the next run so only changed texts are sent to a model.
``docs/design/embedding_substrate_and_search_engine.md`` (section 2) is the design.
"""

import logging
import uuid
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from pathlib import Path

import polars as pl
from ol_llm import BatchFailed, BatchReport, Chunk, CircuitBreaker, process_batch
from ol_orchestrate.lib.embeddings.chunking import Chunker
from ol_orchestrate.lib.embeddings.documents import Document, md5_hex
from ol_orchestrate.lib.embeddings.learn import (
    CONTENT_FILE_DOC_KIND,
    ContentFileDocument,
    ContentFilePointMapper,
)
from ol_orchestrate.lib.embeddings.models import Embedder, EmbeddingBatch
from ol_orchestrate.lib.embeddings.pipeline import (
    ChunkRow,
    StoredVector,
    chunk_documents,
    embed_chunks,
)
from ol_orchestrate.lib.embeddings.strategy import (
    EmbeddingStrategy,
    load_strategies,
)

STRATEGIES_PATH = Path(__file__).parents[1] / "embedding_strategies.yaml"
TOPIC_DOC_KIND = "topic"
TOPICS_RELATION = (
    "stg__mitlearn__app__postgres__learning_resources_learningresourcetopic"
)


@dataclass(frozen=True)
class ContentFileSource:
    """Where an etl_source's content files and their runs' resources are.

    :param platform_code: Learn's ``platform.code`` for every resource of the
        source, or ``None`` when it varies by course and the runs model's
        ``platform_code`` column has each run's
    :param files_relation: the integrations model with the source's content files
    :param runs_relation: the integrations model whose ``run_id`` and
        ``readable_id`` give each run's resource
    """

    platform_code: str | None
    files_relation: str
    runs_relation: str

    def run_resources(self, runs: pl.LazyFrame) -> pl.LazyFrame:
        """Return each run's resource and Learn platform code from the runs model.

        :param runs: rows of ``runs_relation``
        :returns: ``run_id``, ``readable_id`` and ``platform_code`` rows
        :rtype: pl.LazyFrame
        """
        platform_code = (
            pl.col("platform_code")
            if self.platform_code is None
            else pl.lit(self.platform_code)
        )
        return runs.select(
            "run_id", "readable_id", platform_code.alias("platform_code")
        )


# The etl_sources the content-file models carry. Learn's platform code is the
# PlatformType name its ETL sets (learning_resources/constants.py): one per source,
# except xPRO, whose ETL maps each course's platform (XPRO_PLATFORM_TRANSFORM).
CONTENT_FILE_SOURCES = {
    "mitxonline": ContentFileSource(
        platform_code="mitxonline",
        files_relation="integrations__learn__content_files",
        runs_relation="integrations__learn__mitxonline_runs",
    ),
    "ocw": ContentFileSource(
        platform_code="ocw",
        files_relation="integrations__learn__ocw_content_files",
        runs_relation="integrations__learn__ocw_courses",
    ),
    "xpro": ContentFileSource(
        platform_code=None,
        files_relation="integrations__learn__content_files",
        runs_relation="integrations__learn__xpro_runs",
    ),
}
CONTENT_FILE_COLUMNS = (
    "run_readable_id",
    "key",
    "content",
    "file_type",
    "file_extension",
)

CHUNK_SCHEMA = pl.Schema(
    {
        "chunk_key": pl.String,
        "doc_key": pl.String,
        "doc_kind": pl.String,
        "chunker_id": pl.String,
        "chunk_number": pl.Int64,
        "chunk_text": pl.String,
        "chunk_checksum": pl.String,
        "text_checksum": pl.String,
        "point_id": pl.String,
        "source_relation": pl.String,
    }
)
# A vector row as embedding produces it, before the run is stamped on.
EMBEDDED_SCHEMA = pl.Schema(
    {
        "chunk_key": pl.String(),
        "strategy_id": pl.String(),
        "model_id": pl.String(),
        "vector_space": pl.String(),
        "input_checksum": pl.String(),
        "vector": pl.List(pl.Float32()),
        "embedded_at": pl.Datetime("us", "UTC"),
    }
)
VECTOR_SCHEMA = pl.Schema({**EMBEDDED_SCHEMA, "run_id": pl.String()})


@cache
def strategies() -> dict[str, EmbeddingStrategy]:
    """Return the strategies in ``embedding_strategies.yaml`` by id.

    :rtype: dict[str, EmbeddingStrategy]
    """
    return load_strategies(STRATEGIES_PATH)


def strategies_version() -> str:
    """Return the MD5 of the strategy file, for a code version.

    :rtype: str
    """
    return md5_hex(STRATEGIES_PATH.read_text())


def active_strategies(doc_kind: str) -> list[EmbeddingStrategy]:
    """Return the serving and candidate strategies for a kind of document.

    :param doc_kind: e.g. ``content_file``
    :rtype: list[EmbeddingStrategy]
    """
    return [
        strategy
        for strategy in strategies().values()
        if strategy.doc_kind == doc_kind and strategy.role != "retired"
    ]


def active_chunkers(doc_kind: str) -> list[Chunker]:
    """Return the distinct chunkers of a kind's serving and candidate strategies.

    :param doc_kind: e.g. ``content_file``
    :returns: one chunker per ``chunker_id``, sorted by it
    :rtype: list[Chunker]
    """
    chunkers = {
        strategy.chunker.chunker_id: strategy.chunker
        for strategy in active_strategies(doc_kind)
    }
    return [chunkers[chunker_id] for chunker_id in sorted(chunkers)]


def chunkers_version(doc_kind: str) -> str:
    """Return a code version that moves when a kind's chunkers change.

    :param doc_kind: e.g. ``content_file``
    :rtype: str
    """
    return md5_hex("\n".join(c.chunker_id for c in active_chunkers(doc_kind)))


def strategies_for_run(
    doc_kind: str, strategy_ids: Sequence[str] | None
) -> list[EmbeddingStrategy]:
    """Return the strategies a vector run embeds.

    :param doc_kind: the kind of document the run embeds
    :param strategy_ids: the run's choice; ``None`` takes the serving strategies
    :rtype: list[EmbeddingStrategy]
    :raises ValueError: when a named strategy is unknown, retired, for another
        kind of document, or has a context template (the chunk tables do not
        carry the document metadata a template renders)
    """
    if strategy_ids is None:
        chosen = [s for s in active_strategies(doc_kind) if s.role == "serving"]
    else:
        unknown = sorted(set(strategy_ids) - set(strategies()))
        if unknown:
            msg = f"unknown strategies: {unknown}"
            raise ValueError(msg)
        chosen = [strategies()[strategy_id] for strategy_id in strategy_ids]
    for strategy in chosen:
        if strategy.doc_kind != doc_kind:
            msg = f"{strategy.strategy_id} embeds {strategy.doc_kind}, not {doc_kind}"
            raise ValueError(msg)
        if strategy.role == "retired":
            msg = f"{strategy.strategy_id} is retired"
            raise ValueError(msg)
        if strategy.context_template is not None:
            msg = (
                f"{strategy.strategy_id} has a context template, which needs "
                "document metadata the chunk tables do not store"
            )
            raise ValueError(msg)
    return chosen


def content_file_documents(
    files: pl.DataFrame, runs: pl.DataFrame
) -> tuple[list[Document], int]:
    """Return content files as documents keyed as Learn keys their points.

    A file's point key needs its run's resource and platform code, which come from
    the runs model. Files whose run is not there, or has no platform code, get no
    document: Learn loads no resource for a course whose platform it cannot map.

    :param files: rows of a content-file model, with ``CONTENT_FILE_COLUMNS``
    :param runs: ``run_id``, ``readable_id`` (the run's resource) and
        ``platform_code`` rows, from :meth:`ContentFileSource.run_resources`
    :returns: the documents, and how many files had no resource
    :rtype: tuple[list[Document], int]
    :raises ValueError: when a run has more than one resource
    """
    resources = runs.select("run_id", "readable_id", "platform_code").unique()
    if resources["run_id"].is_duplicated().any():
        msg = "a run_id has more than one resource in the runs model"
        raise ValueError(msg)
    joined = files.join(
        resources, left_on="run_readable_id", right_on="run_id", how="left"
    )
    matched = joined.filter(
        pl.col("readable_id").is_not_null() & pl.col("platform_code").is_not_null()
    )
    documents = [
        ContentFileDocument(
            key=row["key"],
            content=row["content"],
            resource_readable_id=row["readable_id"],
            run_readable_id=row["run_readable_id"],
            platform_code=row["platform_code"],
            file_type=row["file_type"],
            file_extension=row["file_extension"],
        ).to_document()
        for row in matched.iter_rows(named=True)
    ]
    return documents, joined.height - matched.height


def topic_documents(topics: pl.DataFrame) -> list[Document]:
    """Return Learn's topics as documents keyed by their point id.

    Learn's topic point id is the topic's ``topic_uuid`` and its text the name
    (``embed_topics`` in mit-learn ``vector_search/utils.py``).

    :param topics: rows of the topic staging model
    :rtype: list[Document]
    """
    return [
        Document(
            doc_key=str(uuid.UUID(row["learningresourcetopic_uuid"])),
            doc_kind=TOPIC_DOC_KIND,
            text=row["learningresourcetopic_name"],
            metadata={"name": row["learningresourcetopic_name"]},
        )
        for row in topics.iter_rows(named=True)
    ]


def point_id(document: Document, row: ChunkRow) -> str:
    """Return the id of the Qdrant point Learn writes for a chunk.

    :param document: the chunk's document
    :param row: the chunk
    :rtype: str
    :raises ValueError: for a kind of document Learn has no points for here, or a
        topic chunk other than the first (Learn embeds a topic whole)
    """
    if document.doc_kind == CONTENT_FILE_DOC_KIND:
        return ContentFilePointMapper().point_id(document, row)
    if document.doc_kind == TOPIC_DOC_KIND:
        if row.chunk_number != 0:
            msg = f"topic {document.doc_key} has chunk {row.chunk_number}"
            raise ValueError(msg)
        return document.doc_key
    msg = f"no point id for {document.doc_kind}"
    raise ValueError(msg)


def chunk_frame(
    documents: Sequence[Document], chunkers: Iterable[Chunker], source_relation: str
) -> pl.DataFrame:
    """Chunk documents with every chunker, as rows of a chunk table.

    :param documents: the documents
    :param chunkers: the chunkers
    :param source_relation: the model the documents were read from
    :returns: one row per non-empty chunk per chunker
    :rtype: pl.DataFrame
    """
    rows = [
        {
            **row.model_dump(),
            "text_checksum": document.text_checksum,
            "point_id": point_id(document, row),
            "source_relation": source_relation,
        }
        for chunker in chunkers
        for document, row in chunk_documents(documents, chunker)
    ]
    return pl.DataFrame(rows, schema=CHUNK_SCHEMA)


def stored_chunks(chunks: pl.DataFrame) -> list[tuple[Document, ChunkRow]]:
    """Rebuild the chunk pairs :func:`embed_chunks` takes from a chunk table's rows.

    The document is rebuilt from the row's key and kind only, which is all
    ``embed_chunks`` reads for a strategy without a context template.

    :param chunks: rows of a chunk table
    :rtype: list[tuple[Document, ChunkRow]]
    """
    return [
        (
            Document(doc_key=row["doc_key"], doc_kind=row["doc_kind"], text=None),
            ChunkRow.model_validate(
                {name: row[name] for name in ChunkRow.model_fields}
            ),
        )
        for row in chunks.iter_rows(named=True)
    ]


def known_vectors(
    vectors: pl.LazyFrame | None, vector_space: str, checksums: Iterable[str]
) -> dict[str, StoredVector]:
    """Look up stored vectors of a vector space by the checksum of the text embedded.

    :param vectors: a vector table, ``None`` before it exists
    :param vector_space: a model's ``vector_space_id``, e.g.
        ``text-embedding-3-large@3072``
    :param checksums: the input checksums wanted
    :returns: per checksum found, the vector of its earliest stored row and the
        model that produced it
    :rtype: dict[str, StoredVector]
    """
    wanted = sorted(set(checksums))
    if vectors is None or not wanted:
        return {}
    found = (
        vectors.filter(
            (pl.col("vector_space") == vector_space)
            & pl.col("input_checksum").is_in(wanted)
        )
        # The earliest row of a checksum, so the pick does not vary between runs
        # when two models of the space stored one.
        .sort("input_checksum", "embedded_at", "model_id", "chunk_key")
        .unique("input_checksum", keep="first", maintain_order=True)
        .select("input_checksum", "vector", "model_id")
        .collect()
    )
    return {
        checksum: StoredVector([float(value) for value in vector], model_id)
        for checksum, vector, model_id in found.iter_rows()
    }


@dataclass(frozen=True)
class RequestBatch:
    """One embeddings request: texts, with the checksums they are stored under."""

    checksums: tuple[str, ...]
    texts: tuple[str, ...]


@dataclass(frozen=True)
class StrategyOutcome:
    """What one strategy's run did.

    :param chunks: chunks of the strategy's chunker in the partition
    :param embedded_texts: distinct texts a model returned a vector for
    :param reused_texts: distinct texts whose stored vector was reused
    :param failed_chunks: chunks left without a vector, as their request failed
    :param input_tokens: tokens the provider reported
    :param failure: the batches' descriptions when a chunk was left without a
        vector
    """

    chunks: int
    embedded_texts: int
    reused_texts: int
    failed_chunks: int
    input_tokens: int
    failure: str | None


def _request_batches(
    strategy: EmbeddingStrategy, new_texts: Mapping[str, str]
) -> list[RequestBatch]:
    checksums = list(new_texts)
    batches = []
    start = 0
    for texts in strategy.model.request_batches(list(new_texts.values())):
        end = start + len(texts)
        batches.append(RequestBatch(tuple(checksums[start:end]), tuple(texts)))
        start = end
    return batches


def embed_strategy(  # noqa: PLR0913
    strategy: EmbeddingStrategy,
    chunks: Sequence[tuple[Document, ChunkRow]],
    embedder: Embedder,
    lookup: Callable[[str, Iterable[str]], dict[str, StoredVector]],
    *,
    embedded_at: datetime,
    max_workers: int = 1,
    logger: logging.Logger | None = None,
) -> tuple[pl.DataFrame, StrategyOutcome]:
    """Embed a partition's chunks under a strategy, paying only for new texts.

    The texts whose checksum has a stored vector in the strategy model's vector
    space are not sent. The rest go through ol-llm's ``process_batch`` with each row one
    request of up to the model's ``batch_size`` texts, and an outage stops the
    batch. A request that fails for its input (a rejection or invalid output,
    which the same texts would repeat) is sent again one text per request, so a
    bad text costs only its own chunks; a rate limit or outage is not split, as
    it says nothing about the texts. The chunks of a failed request get no row,
    and the next run of the partition sends them again. Automation re-requests a
    failed partition once; after that the partition waits for its text or
    chunker to change, or a run by hand.

    :param strategy: the strategy
    :param chunks: the partition's chunks of the strategy's chunker
    :param embedder: an embedder for the strategy's model
    :param lookup: stored vectors and their models by input checksum, for a
        vector space
    :param embedded_at: the time put on new rows
    :param max_workers: requests in flight at once
    :param logger: where ``process_batch`` logs, e.g. ``context.log``
    :returns: the vector rows, without ``run_id``, and what the run did
    :rtype: tuple[pl.DataFrame, StrategyOutcome]
    """
    texts = [strategy.embedding_text(document, row.chunk) for document, row in chunks]
    checksums = [md5_hex(text) for text in texts]
    known = lookup(strategy.model.vector_space_id, checksums)
    reused = len(set(checksums) & set(known))
    new_texts = {
        checksum: text
        for checksum, text in zip(checksums, texts, strict=True)
        if checksum not in known
    }

    embedded: dict[str, list[float]] = {}
    input_tokens = 0
    failure = None

    def persist(chunk: Chunk[RequestBatch, EmbeddingBatch]) -> None:
        nonlocal input_tokens
        for batch, result in chunk.succeeded:
            embedded.update(zip(batch.checksums, result.vectors, strict=True))
            input_tokens += result.input_tokens

    def send(batches: list[RequestBatch]) -> BatchReport[RequestBatch]:
        try:
            return process_batch(
                batches,
                lambda batch: embedder.embed(batch.texts, input_type="document"),
                persist,
                max_workers=max_workers,
                breaker=CircuitBreaker(),
                logger=logger,
            )
        except BatchFailed as error:
            return error.report

    reports = []
    if new_texts:
        reports.append(send(_request_batches(strategy, new_texts)))
        if reports[0].circuit_opened_by is None:
            singles = [
                RequestBatch((checksum,), (text,))
                for failed in reports[0].failures
                if not failed.retry_next_run and len(failed.row.texts) > 1
                for checksum, text in zip(
                    failed.row.checksums, failed.row.texts, strict=True
                )
            ]
            if singles:
                reports.append(send(singles))

    known.update(
        (checksum, StoredVector(vector, strategy.model.model_id))
        for checksum, vector in embedded.items()
    )
    vectored = [
        pair
        for pair, checksum in zip(chunks, checksums, strict=True)
        if checksum in known
    ]
    if len(vectored) < len(chunks):
        failure = " Then one text per request: ".join(
            report.describe() for report in reports
        )
    outcome = embed_chunks(
        strategy, vectored, embedder, known_vectors=known, embedded_at=embedded_at
    )
    frame = pl.DataFrame(
        [row.model_dump() for row in outcome.rows],
        schema=EMBEDDED_SCHEMA,
    )
    return frame, StrategyOutcome(
        chunks=len(chunks),
        embedded_texts=len(embedded),
        reused_texts=reused,
        failed_chunks=len(chunks) - len(vectored),
        input_tokens=input_tokens,
        failure=failure,
    )


def merge_vectors(
    new: pl.DataFrame,
    stored: pl.DataFrame | None,
    strategy_ids: Iterable[str],
    run_id: str,
) -> pl.DataFrame:
    """Combine a run's vector rows with the partition's stored ones.

    Stored rows of strategies the run did not embed are kept as they are. A new
    row whose chunk, strategy, vector space and input checksum are already stored
    is replaced by the stored row whole, so a run that embedded nothing changes
    nothing but the write, whichever stored vector of the space the run picked.

    :param new: the run's rows, without ``run_id``
    :param stored: the partition's stored rows, ``None`` when there are none
    :param strategy_ids: the strategies the run embedded
    :param run_id: the Dagster run
    :returns: the partition's rows to write, in ``VECTOR_SCHEMA``
    :rtype: pl.DataFrame
    """
    new = new.with_columns(pl.lit(run_id).alias("run_id")).select(VECTOR_SCHEMA.names())
    if stored is None or stored.is_empty():
        return new
    stored = stored.select(VECTOR_SCHEMA.names())
    untouched = stored.filter(~pl.col("strategy_id").is_in(list(strategy_ids)))
    key = ["chunk_key", "strategy_id", "vector_space", "input_checksum"]
    kept = stored.join(new.select(key), on=key, how="semi")
    added = new.join(stored.select(key), on=key, how="anti")
    return pl.concat([untouched, kept, added])
