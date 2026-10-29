"""The platform's copy of MIT Learn's content-file and topic embeddings, in Iceberg.

Content files are partitioned by (etl_source, course run), so a run whose files
changed is chunked and embedded alone. learn_content_file_text stands for a run's
files and is observed, not materialized: learn_content_file_text_sensor registers
each run and reports a digest of its files' keys and checksums as the data
version, and the chunk and vector assets run for the partitions whose version
moved. Topics are one small table, rebuilt when the topic model is.

The vector assets send a model only the texts with no stored vector in the
strategy model's vector space, so a re-run over unchanged chunks embeds nothing.
Nothing here writes to Qdrant; projecting these tables into Learn's collections is
a separate step.
"""

from collections.abc import Collection, Iterable
from datetime import UTC, datetime

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetSpec,
    Config,
    DataVersion,
    DynamicPartitionsDefinition,
    Failure,
    MetadataValue,
    MultiPartitionKey,
    MultiPartitionsDefinition,
    ObserveResult,
    Output,
    StaticPartitionsDefinition,
    asset,
    multi_observable_source_asset,
)
from ml.lib.learn_embeddings import (
    CONTENT_FILE_COLUMNS,
    CONTENT_FILE_SOURCES,
    EMBEDDED_SCHEMA,
    TOPIC_DOC_KIND,
    TOPICS_RELATION,
    active_chunkers,
    chunk_frame,
    chunkers_version,
    content_file_documents,
    embed_strategy,
    known_vectors,
    merge_vectors,
    stored_chunks,
    strategies,
    strategies_for_run,
    strategies_version,
    topic_documents,
)
from ml.lib.warehouse import (
    read_table,
    require_rows,
    require_table,
    warehouse_database,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.embeddings.documents import md5_hex
from ol_orchestrate.lib.embeddings.learn import CONTENT_FILE_DOC_KIND
from ol_orchestrate.lib.embeddings.pipeline import StoredVector
from ol_orchestrate.resources.embeddings import EmbeddingsResource
from pydantic import Field

GROUP_NAME = "learn_embeddings"
LAYER = "integrations"
CONTENT_FILE_RUNS = DynamicPartitionsDefinition(name="learn_content_file_run")
# etl_source is static and the run dynamic, so the key space is their cross
# product. A combination with no files is never observed, which keeps it from
# being requested.
CONTENT_FILE_PARTITIONS = MultiPartitionsDefinition(
    {
        "etl_source": StaticPartitionsDefinition(sorted(CONTENT_FILE_SOURCES)),
        "run": CONTENT_FILE_RUNS,
    }
)
CONTENT_FILE_PARTITION_EXPR = {"etl_source": "etl_source", "run": "run_readable_id"}
# The version of a run with no files, which the digest of no rows would be.
EMPTY_TEXT_VERSION = md5_hex("")

CONTENT_FILE_TEXT_KEY = AssetKey([GROUP_NAME, "learn_content_file_text"])
CONTENT_FILE_CHUNKS_KEY = AssetKey([LAYER, "embedding_chunks__content_file"])
CONTENT_FILE_VECTORS_KEY = AssetKey([LAYER, "embedding_vectors__content_file"])
TOPIC_CHUNKS_KEY = AssetKey([LAYER, "embedding_chunks__topic"])
TOPIC_VECTORS_KEY = AssetKey([LAYER, "embedding_vectors__topic"])
STRATEGIES_KEY = AssetKey([LAYER, "embedding_strategies"])
TOPICS_SOURCE_KEY = AssetKey(["staging", TOPICS_RELATION])
CONTENT_FILE_SOURCE_KEYS = sorted(
    {
        AssetKey([LAYER, relation])
        for source in CONTENT_FILE_SOURCES.values()
        for relation in (source.files_relation, source.runs_relation)
    },
    key=lambda key: key.to_user_string(),
)

_TABLE_METADATA = {"schema": warehouse_database(LAYER)}
_PARTITIONED_METADATA = {
    **_TABLE_METADATA,
    "partition_expr": CONTENT_FILE_PARTITION_EXPR,
    "schema_update_mode": "update",
}


def partition_run(key: MultiPartitionKey) -> tuple[str, str]:
    """Return (etl_source, run_readable_id) for a content-file partition."""
    dimensions = key.keys_by_dimension
    return dimensions["etl_source"], dimensions["run"]


def content_file_text_versions(
    etl_source: str,
    run_readable_id: str | None = None,
    *,
    registered: Collection[str] = (),
) -> pl.DataFrame:
    """Return a digest of each run's files, for the runs with a known resource.

    The digest covers what a chunk or its point id depends on: the run's
    resource and platform code, each file's key and checksum, and the two fields
    that pick the markdown splitter. A registered run that is still in the runs
    model but has no files left, or no platform code, gets ``EMPTY_TEXT_VERSION``,
    so its partition is rebuilt with no rows rather than keeping the chunks of
    files that are gone or that Learn has no resource for.

    :param etl_source: the source
    :param run_readable_id: one run, or every run of the source when omitted
    :param registered: the runs that have a partition
    :returns: ``etl_source``, ``run_readable_id`` and ``text_version`` rows
    :rtype: pl.DataFrame
    """
    source = CONTENT_FILE_SOURCES[etl_source]
    files = require_table(LAYER, source.files_relation).filter(
        pl.col("etl_source") == etl_source
    )
    runs = source.run_resources(require_table(LAYER, source.runs_relation)).rename(
        {"run_id": "run_readable_id"}
    )
    if run_readable_id is not None:
        files = files.filter(pl.col("run_readable_id") == run_readable_id)
        runs = runs.filter(pl.col("run_readable_id") == run_readable_id)
    resource_fields = ["readable_id", "platform_code"]
    file_fields = ["key", "checksum", "file_type", "file_extension"]
    versions = (
        files.select("run_readable_id", *file_fields)
        .join(
            runs.filter(pl.col("platform_code").is_not_null()),
            on="run_readable_id",
            how="inner",
        )
        .group_by("run_readable_id")
        .agg(
            pl.concat_str(
                [
                    pl.col(name).fill_null("")
                    for name in (*resource_fields, *file_fields)
                ],
                separator="|",
            )
            .sort()
            .str.join("\n")
            .alias("rows")
        )
        .collect()
        .select(
            "run_readable_id",
            pl.col("rows")
            .map_elements(md5_hex, return_dtype=pl.String)
            .alias("text_version"),
        )
    )
    emptied = (
        runs.filter(pl.col("run_readable_id").is_in(list(registered)))
        .select("run_readable_id")
        .unique()
        .collect()
        .join(versions, on="run_readable_id", how="anti")
        .with_columns(pl.lit(EMPTY_TEXT_VERSION).alias("text_version"))
    )
    return (
        pl.concat([versions, emptied])
        .select(
            pl.lit(etl_source).alias("etl_source"), "run_readable_id", "text_version"
        )
        .sort("run_readable_id")
    )


@multi_observable_source_asset(
    specs=[
        AssetSpec(
            CONTENT_FILE_TEXT_KEY,
            partitions_def=CONTENT_FILE_PARTITIONS,
            deps=CONTENT_FILE_SOURCE_KEYS,
            group_name=GROUP_NAME,
            description=(
                "The content files of one MIT Learn course run, versioned by a "
                "digest of their keys and checksums and the run's resource and "
                "platform code."
            ),
        )
    ],
)
def learn_content_file_text(context: AssetExecutionContext) -> ObserveResult:
    """Report one run's text version.

    Routine observation comes from learn_content_file_text_sensor. This is kept so
    the asset is observable (only observable upstreams take part in the data
    version comparison) and so a single run can be observed on demand.
    """
    etl_source, run = partition_run(context.partition_key)
    versions = content_file_text_versions(etl_source, run, registered=[run])
    if versions.is_empty():
        runs_relation = CONTENT_FILE_SOURCES[etl_source].runs_relation
        msg = f"{etl_source} {run} is not in {runs_relation}"
        raise Failure(msg)
    return ObserveResult(
        asset_key=CONTENT_FILE_TEXT_KEY,
        data_version=DataVersion(versions["text_version"][0]),
    )


@asset(
    key=CONTENT_FILE_CHUNKS_KEY,
    partitions_def=CONTENT_FILE_PARTITIONS,
    deps=[CONTENT_FILE_TEXT_KEY],
    group_name=GROUP_NAME,
    io_manager_key="learn_embeddings_io_manager",
    metadata=_PARTITIONED_METADATA,
    code_version=chunkers_version(CONTENT_FILE_DOC_KIND),
    automation_condition=upstream_or_code_changes(),
    description=(
        "One row per chunk of a content file per chunker of the serving and "
        "candidate content-file strategies, with Learn's point id for the chunk."
    ),
)
def content_file_embedding_chunks(
    context: AssetExecutionContext,
) -> Output[pl.DataFrame]:
    """Chunk one course run's content files.

    The file's point key, and so its document key, needs the run's platform code
    and resource readable_id, which come from the runs model of the source.
    """
    etl_source, run = partition_run(context.partition_key)
    source = CONTENT_FILE_SOURCES[etl_source]
    files = require_rows(
        LAYER,
        source.files_relation,
        CONTENT_FILE_COLUMNS,
        etl_source=etl_source,
        run_readable_id=run,
    )
    runs = (
        source.run_resources(require_table(LAYER, source.runs_relation))
        .filter(pl.col("run_id") == run)
        .collect()
    )
    documents, unmatched = content_file_documents(files, runs)
    if unmatched:
        context.log.warning(
            "%d content files of %s %s have no resource or platform code in %s "
            "and are not chunked",
            unmatched,
            etl_source,
            run,
            source.runs_relation,
        )
    chunks = chunk_frame(
        documents, active_chunkers(CONTENT_FILE_DOC_KIND), source.files_relation
    ).with_columns(
        pl.lit(etl_source).alias("etl_source"),
        pl.lit(run).alias("run_readable_id"),
    )
    return Output(
        chunks,
        metadata={
            "row_count": MetadataValue.int(chunks.height),
            "document_count": MetadataValue.int(len(documents)),
            "unmatched_file_count": MetadataValue.int(unmatched),
        },
    )


@asset(
    key=TOPIC_CHUNKS_KEY,
    deps=[TOPICS_SOURCE_KEY],
    group_name=GROUP_NAME,
    metadata=_TABLE_METADATA,
    code_version=chunkers_version(TOPIC_DOC_KIND),
    automation_condition=upstream_or_code_changes(),
    description=(
        "One row per MIT Learn topic per chunker of the topic strategies, with the "
        "topic's point id (its topic_uuid)."
    ),
)
def topic_embedding_chunks() -> Output[pl.DataFrame]:
    """Chunk every topic name; Learn embeds each one whole."""
    topics = require_table("staging", TOPICS_RELATION).collect()
    chunks = chunk_frame(
        topic_documents(topics), active_chunkers(TOPIC_DOC_KIND), TOPICS_RELATION
    )
    return Output(chunks, metadata={"row_count": MetadataValue.int(chunks.height)})


class EmbeddingVectorsConfig(Config):
    strategy_ids: list[str] | None = Field(
        default=None,
        description=(
            "Strategies to embed. Unset embeds the serving strategies; name a "
            "candidate here to embed it. Strategies not named keep their stored "
            "vectors."
        ),
    )
    max_workers: int = Field(default=1, ge=1, description="Requests in flight")


def embed_vectors(  # noqa: PLR0913
    context: AssetExecutionContext,
    config: EmbeddingVectorsConfig,
    embeddings: EmbeddingsResource,
    *,
    doc_kind: str,
    chunks: pl.DataFrame,
    vectors: pl.LazyFrame | None,
    stored: pl.DataFrame | None,
) -> tuple[pl.DataFrame, dict[str, MetadataValue], list[str]]:
    """Embed a partition's chunks under each strategy of the run.

    :param context: the asset context, for logs and the run id
    :param config: the run's strategies and concurrency
    :param embeddings: the embedder resource
    :param doc_kind: the kind of document the asset embeds
    :param chunks: the partition's chunk rows
    :param vectors: the whole vector table, for stored vectors of a vector space
    :param stored: the partition's stored vector rows
    :returns: the rows to write, their metadata, and one message per strategy
        with failed requests
    :rtype: tuple[pl.DataFrame, dict[str, MetadataValue], list[str]]
    """
    chosen = strategies_for_run(doc_kind, config.strategy_ids)
    embedded_at = datetime.now(tz=UTC)
    frames = []
    metadata: dict[str, MetadataValue] = {}
    failures = []
    # This run's vectors by vector space and input checksum, so a later strategy
    # in the same space reuses what an earlier one embedded.
    this_run: dict[str, dict[str, StoredVector]] = {}

    def lookup(vector_space: str, checksums: Iterable[str]) -> dict[str, StoredVector]:
        wanted = set(checksums)
        found = {
            checksum: stored
            for checksum, stored in this_run.get(vector_space, {}).items()
            if checksum in wanted
        }
        return found | known_vectors(vectors, vector_space, wanted - found.keys())

    for strategy in chosen:
        pairs = stored_chunks(
            chunks.filter(pl.col("chunker_id") == strategy.chunker.chunker_id)
        )
        frame, outcome = embed_strategy(
            strategy,
            pairs,
            embeddings.embedder(strategy),
            lookup,
            embedded_at=embedded_at,
            max_workers=config.max_workers,
            logger=context.log,
        )
        this_run.setdefault(strategy.model.vector_space_id, {}).update(
            (checksum, StoredVector(vector, model_id))
            for checksum, vector, model_id in frame.select(
                "input_checksum", "vector", "model_id"
            ).iter_rows()
        )
        frames.append(frame)
        context.log.info(
            "%s: %d chunks, %d texts embedded, %d reused, %d chunks failed",
            strategy.strategy_id,
            outcome.chunks,
            outcome.embedded_texts,
            outcome.reused_texts,
            outcome.failed_chunks,
        )
        prefix = strategy.strategy_id
        metadata |= {
            f"{prefix}/embedded_texts": MetadataValue.int(outcome.embedded_texts),
            f"{prefix}/reused_texts": MetadataValue.int(outcome.reused_texts),
            f"{prefix}/failed_chunks": MetadataValue.int(outcome.failed_chunks),
            f"{prefix}/input_tokens": MetadataValue.int(outcome.input_tokens),
        }
        if outcome.failure is not None:
            failures.append(f"{strategy.strategy_id}: {outcome.failure}")
    rows = merge_vectors(
        pl.concat(frames) if frames else pl.DataFrame(schema=EMBEDDED_SCHEMA),
        stored,
        [strategy.strategy_id for strategy in chosen],
        context.run.run_id,
    )
    metadata["row_count"] = MetadataValue.int(rows.height)
    return rows, metadata, failures


def _fail_on_request_failures(failures: list[str]) -> None:
    # After the output, so what succeeded is stored and the next run sends only
    # the chunks that failed.
    if failures:
        raise Failure(
            description=f"Some embedding requests failed. {' '.join(failures)}",
            metadata={"failed_strategies": len(failures)},
        )


@asset(
    key=CONTENT_FILE_VECTORS_KEY,
    partitions_def=CONTENT_FILE_PARTITIONS,
    deps=[CONTENT_FILE_CHUNKS_KEY],
    group_name=GROUP_NAME,
    io_manager_key="learn_embeddings_io_manager",
    metadata=_PARTITIONED_METADATA,
    # No code version: a new strategy is embedded when a run names it, never by
    # a deploy re-running every partition.
    automation_condition=upstream_or_code_changes(),
    description=(
        "One row per content-file chunk per strategy, keyed (chunk_key, "
        "strategy_id), with the vector, its vector space and the checksum of the "
        "text embedded."
    ),
)
def content_file_embedding_vectors(
    context: AssetExecutionContext,
    config: EmbeddingVectorsConfig,
    embeddings: EmbeddingsResource,
):
    """Embed one course run's chunks, reusing every stored vector of the space."""
    etl_source, run = partition_run(context.partition_key)
    in_partition = (pl.col("etl_source") == etl_source) & (
        pl.col("run_readable_id") == run
    )
    chunks = (
        require_table(LAYER, CONTENT_FILE_CHUNKS_KEY.path[-1])
        .filter(in_partition)
        .collect()
    )
    vectors = read_table(LAYER, CONTENT_FILE_VECTORS_KEY.path[-1])
    stored = vectors.filter(in_partition).collect() if vectors is not None else None
    rows, metadata, failures = embed_vectors(
        context,
        config,
        embeddings,
        doc_kind=CONTENT_FILE_DOC_KIND,
        chunks=chunks,
        vectors=vectors,
        stored=stored,
    )
    yield Output(
        rows.with_columns(
            pl.lit(etl_source).alias("etl_source"),
            pl.lit(run).alias("run_readable_id"),
        ),
        metadata=metadata,
    )
    _fail_on_request_failures(failures)


@asset(
    key=TOPIC_VECTORS_KEY,
    deps=[TOPIC_CHUNKS_KEY],
    group_name=GROUP_NAME,
    metadata=_TABLE_METADATA,
    automation_condition=upstream_or_code_changes(),
    description=(
        "One row per topic chunk per strategy, keyed (chunk_key, strategy_id), "
        "with the vector, its vector space and the checksum of the text embedded."
    ),
)
def topic_embedding_vectors(
    context: AssetExecutionContext,
    config: EmbeddingVectorsConfig,
    embeddings: EmbeddingsResource,
):
    """Embed the topic chunks, reusing every stored vector of the space."""
    chunks = require_table(LAYER, TOPIC_CHUNKS_KEY.path[-1]).collect()
    vectors = read_table(LAYER, TOPIC_VECTORS_KEY.path[-1])
    # The table is one row per topic and strategy, so it is read once and the
    # stored-vector lookups search it in memory.
    stored = vectors.collect() if vectors is not None else None
    rows, metadata, failures = embed_vectors(
        context,
        config,
        embeddings,
        doc_kind=TOPIC_DOC_KIND,
        chunks=chunks,
        vectors=stored.lazy() if stored is not None else None,
        stored=stored,
    )
    yield Output(rows, metadata=metadata)
    _fail_on_request_failures(failures)


@asset(
    key=STRATEGIES_KEY,
    group_name=GROUP_NAME,
    metadata=_TABLE_METADATA,
    code_version=strategies_version(),
    automation_condition=upstream_or_code_changes(),
    description=(
        "A copy of ml/embedding_strategies.yaml, one row per strategy, for joins "
        "with the chunk and vector tables."
    ),
)
def embedding_strategies() -> Output[pl.DataFrame]:
    """Write the strategy file as a table."""
    rows = pl.DataFrame(
        [
            {
                "strategy_id": strategy.strategy_id,
                "doc_kind": strategy.doc_kind,
                "role": strategy.role,
                "chunker_id": strategy.chunker.chunker_id,
                "chunker": strategy.chunker.model_dump_json(),
                "model_id": strategy.model.model_id,
                "vector_space": strategy.model.vector_space_id,
                "model": strategy.model.model_dump_json(),
                "context_template": strategy.context_template,
                "qdrant": strategy.qdrant.model_dump_json()
                if strategy.qdrant is not None
                else None,
                "notes": strategy.notes,
            }
            for strategy in strategies().values()
        ],
        schema={
            "strategy_id": pl.String,
            "doc_kind": pl.String,
            "role": pl.String,
            "chunker_id": pl.String,
            "chunker": pl.String,
            "model_id": pl.String,
            "vector_space": pl.String,
            "model": pl.String,
            "context_template": pl.String,
            "qdrant": pl.String,
            "notes": pl.String,
        },
    )
    return Output(rows, metadata={"row_count": MetadataValue.int(rows.height)})
