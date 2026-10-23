"""Smokechecks of the learn_v1 vectors against the points in Learn's collections.

The platform's point ids are Learn's, so a partition's chunks are read back from
Learn's ``content_files`` collection by id, and the run's points there are listed
to find the ones the platform lacks. Text and checksum mismatches are expected for
mitxonline and xpro OLX files, whose text the platform approximates
(docs/learn_embedding_config.md), so they only warn. A chunk whose text is
Learn's but whose vector is not is an error: both sides embed the same text with
text-embedding-3-large, so the Azure OpenAI and OpenAI deployments would be
returning different vectors, or the two sides embed differently.

The checks have no automation condition, so automation never runs them; launch
them by hand for the partitions to compare. They read Learn's cluster through
the ``learn_qdrant`` resource.
"""

import polars as pl
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    MetadataValue,
    asset_check,
)
from ml.assets.learn_embeddings import (
    CONTENT_FILE_CHUNKS_KEY,
    CONTENT_FILE_PARTITIONS,
    CONTENT_FILE_VECTORS_KEY,
    LAYER,
    TOPIC_CHUNKS_KEY,
    TOPIC_VECTORS_KEY,
    partition_run,
)
from ml.lib.learn_embeddings import strategies
from ml.lib.learn_parity import (
    MIN_COSINE_SIMILARITY,
    ParityReport,
    compare_with_learn,
    content_file_run_filter,
    retrieve_points,
    scroll_point_ids,
)
from ml.lib.warehouse import require_table
from ol_orchestrate.lib.embeddings.learn import (
    LEARN_CONTENT_FILES_COLLECTION,
    LEARN_TOPICS_COLLECTION,
)
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy
from ol_orchestrate.resources.embeddings import QdrantResource

LEARN_V1_CONTENT_FILES_ID = "learn_v1_content_files"
LEARN_V1_TOPICS_ID = "learn_v1_topics"


def platform_chunks(
    chunks: pl.DataFrame, vectors: pl.DataFrame, strategy: EmbeddingStrategy
) -> pl.DataFrame:
    """Return a strategy's chunks with their vectors, null where there is none.

    :param chunks: chunk rows
    :param vectors: vector rows of the same chunks
    :param strategy: the strategy
    :returns: ``point_id``, ``chunk_text``, ``text_checksum`` and ``vector`` rows
    :rtype: pl.DataFrame
    """
    return (
        chunks.filter(pl.col("chunker_id") == strategy.chunker.chunker_id)
        .select("chunk_key", "point_id", "chunk_text", "text_checksum")
        .join(
            vectors.filter(pl.col("strategy_id") == strategy.strategy_id).select(
                "chunk_key", "vector"
            ),
            on="chunk_key",
            how="left",
        )
    )


def check_result(report: ParityReport) -> AssetCheckResult:
    """Fail with ERROR for unlike vectors of the same text, WARN for other gaps.

    :param report: the comparison
    :rtype: AssetCheckResult
    """
    metadata: dict[str, MetadataValue] = {
        "platform_chunks": MetadataValue.int(report.platform_chunks),
        "platform_chunks_without_vector": MetadataValue.int(
            report.platform_chunks_without_vector
        ),
        "learn_points": MetadataValue.int(report.learn_points),
        "matched_ids": MetadataValue.int(report.matched_ids),
        "missing_in_learn": MetadataValue.int(report.missing_in_learn),
        "missing_on_platform": MetadataValue.int(report.missing_on_platform),
        "text_matches": MetadataValue.int(report.text_matches),
        "learn_points_without_vector": MetadataValue.int(
            report.learn_points_without_vector
        ),
        "compared_vectors": MetadataValue.int(report.compared_vectors),
        "below_min_cosine": MetadataValue.int(report.below_threshold),
        "min_cosine_threshold": MetadataValue.float(MIN_COSINE_SIMILARITY),
    }
    if report.checksum_matches is not None:
        metadata["checksum_matches"] = MetadataValue.int(report.checksum_matches)
    if report.cosine_min is not None and report.cosine_p50 is not None:
        metadata["cosine_min"] = MetadataValue.float(report.cosine_min)
        metadata["cosine_p50"] = MetadataValue.float(report.cosine_p50)
    if report.vectors_differ:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=(
                f"{report.below_threshold} chunks with Learn's text have a vector "
                f"under {MIN_COSINE_SIMILARITY} cosine similarity to Learn's"
            ),
            metadata=metadata,
        )
    if report.points_differ:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Some points or texts differ from Learn's",
            metadata=metadata,
        )
    return AssetCheckResult(passed=True, metadata=metadata)


@asset_check(
    asset=CONTENT_FILE_VECTORS_KEY,
    name="matches_learn_qdrant",
    partitions_def=CONTENT_FILE_PARTITIONS,
    additional_deps=[CONTENT_FILE_CHUNKS_KEY],
    description=(
        "Compares one course run's learn_v1_content_files chunks and vectors with "
        "the points in Learn's content_files collection: ids, texts, checksums and "
        "the cosine similarity of the vectors of chunks whose text matches."
    ),
)
def content_file_vectors_match_learn(
    context: AssetCheckExecutionContext, learn_qdrant: QdrantResource
) -> AssetCheckResult:
    """Compare one course run's chunks with Learn's points for the run."""
    etl_source, run = partition_run(context.multi_partition_key)
    in_partition = (pl.col("etl_source") == etl_source) & (
        pl.col("run_readable_id") == run
    )
    platform = platform_chunks(
        require_table(LAYER, CONTENT_FILE_CHUNKS_KEY.path[-1])
        .filter(in_partition)
        .collect(),
        require_table(LAYER, CONTENT_FILE_VECTORS_KEY.path[-1])
        .filter(in_partition)
        .collect(),
        strategies()[LEARN_V1_CONTENT_FILES_ID],
    )
    client = learn_qdrant.get_client()
    collection = learn_qdrant.collection_name(LEARN_CONTENT_FILES_COLLECTION)
    learn = retrieve_points(
        client,
        collection,
        platform["point_id"].to_list(),
        text_field="chunk_content",
        checksum_field="checksum",
        vector_name=LEARN_CONTENT_FILES_COLLECTION.dense_vector_name,
    )
    learn_ids = scroll_point_ids(client, collection, content_file_run_filter(run))
    return check_result(
        compare_with_learn(platform, learn, learn_ids, compare_checksums=True)
    )


@asset_check(
    asset=TOPIC_VECTORS_KEY,
    name="matches_learn_qdrant",
    additional_deps=[TOPIC_CHUNKS_KEY],
    description=(
        "Compares the learn_v1_topics chunks and vectors with the points in "
        "Learn's topics collection: ids, names and the cosine similarity of the "
        "vectors of topics whose name matches."
    ),
)
def topic_vectors_match_learn(learn_qdrant: QdrantResource) -> AssetCheckResult:
    """Compare every topic with Learn's topic points."""
    platform = platform_chunks(
        require_table(LAYER, TOPIC_CHUNKS_KEY.path[-1]).collect(),
        require_table(LAYER, TOPIC_VECTORS_KEY.path[-1]).collect(),
        strategies()[LEARN_V1_TOPICS_ID],
    )
    client = learn_qdrant.get_client()
    collection = learn_qdrant.collection_name(LEARN_TOPICS_COLLECTION)
    learn = retrieve_points(
        client,
        collection,
        platform["point_id"].to_list(),
        text_field="name",
        checksum_field=None,
        vector_name=LEARN_TOPICS_COLLECTION.dense_vector_name,
    )
    learn_ids = scroll_point_ids(client, collection, None)
    return check_result(
        compare_with_learn(platform, learn, learn_ids, compare_checksums=False)
    )
