"""Smokechecks of the learn_v1 vectors against the points in Learn's collections.

The platform's point ids are Learn's, so a partition's chunks are read back from
Learn's ``content_files`` collection by id, and the run's points there are listed
to find the ones the platform lacks. Text and checksum mismatches are expected for
mitxonline and xpro OLX files, whose text the platform approximates
(docs/learn_embedding_config.md), so they only warn. A chunk whose text is
Learn's but whose vector is not is an error: both sides embed the same text with
text-embedding-3-large, so the Azure OpenAI and OpenAI deployments would be
returning different vectors, or the two sides embed differently.

The checks have no automation condition, so they run with every materialization
of their asset: automation's, backfills' and the UI's. They read Learn's cluster
through the ``learn_qdrant`` resource, which dev has no access to, so
:func:`learn_parity_definitions` defines both only outside dev.
"""

import os
from functools import partial

import polars as pl
from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetChecksDefinition,
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
from ml.lib.learn_embeddings import CONTENT_FILE_SOURCES, strategies
from ml.lib.learn_parity import (
    MIN_COSINE_SIMILARITY,
    ParityReport,
    compare_with_learn,
    content_file_run_filter,
    dense_vector_size,
    retrieve_points,
    scroll_point_ids,
)
from ml.lib.warehouse import require_table
from ol_orchestrate.lib.embeddings.learn import (
    LEARN_CONTENT_FILES_COLLECTION,
    LEARN_TOPICS_COLLECTION,
    LEARN_V1_CONTENT_FILES,
)
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy
from ol_orchestrate.resources.embeddings import QdrantResource
from ol_orchestrate.resources.secrets.vault import Vault

LEARN_V1_CONTENT_FILES_ID = "learn_v1_content_files"
LEARN_V1_TOPICS_ID = "learn_v1_topics"
# The model Learn's own vectors come from; a platform row of it was imported.
LEARN_MODEL_ID = LEARN_V1_CONTENT_FILES.model.model_id


def platform_chunks(
    chunks: pl.DataFrame, vectors: pl.DataFrame, strategy: EmbeddingStrategy
) -> pl.DataFrame:
    """Return a strategy's chunks with their vectors, null where there is none.

    :param chunks: chunk rows
    :param vectors: vector rows of the same chunks
    :param strategy: the strategy
    :returns: ``point_id``, ``chunk_text``, ``text_checksum``, ``vector`` and
        ``model_id`` rows
    :rtype: pl.DataFrame
    """
    return (
        chunks.filter(pl.col("chunker_id") == strategy.chunker.chunker_id)
        .select("chunk_key", "point_id", "chunk_text", "text_checksum")
        .join(
            vectors.filter(pl.col("strategy_id") == strategy.strategy_id).select(
                "chunk_key", "vector", "model_id"
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
        "platform_vector_size": MetadataValue.int(report.platform_vector_size),
        "vector_length_mismatches": MetadataValue.int(report.vector_length_mismatches),
    }
    if report.learn_vector_size is not None:
        metadata["learn_vector_size"] = MetadataValue.int(report.learn_vector_size)
    if report.checksum_matches is not None:
        metadata["checksum_matches"] = MetadataValue.int(report.checksum_matches)
    # By the model that produced the platform's vector, so vectors imported from
    # Learn, which match Learn's by construction, do not hide the others.
    for model_id, stats in report.cosine_by_model.items():
        metadata[f"{model_id}/compared_vectors"] = MetadataValue.int(stats.compared)
        metadata[f"{model_id}/below_min_cosine"] = MetadataValue.int(
            stats.below_threshold
        )
        if stats.minimum is not None and stats.p50 is not None:
            metadata[f"{model_id}/cosine_min"] = MetadataValue.float(stats.minimum)
            metadata[f"{model_id}/cosine_p50"] = MetadataValue.float(stats.p50)
    if report.vectors_differ:
        description = (
            f"{report.below_threshold} chunks with Learn's text have a vector, "
            f"not from {report.reference_model_id}, under "
            f"{MIN_COSINE_SIMILARITY} cosine similarity to Learn's"
        )
        if report.vector_length_mismatches:
            description += (
                f"; {report.vector_length_mismatches} have a vector of another "
                "length than Learn's"
            )
        if report.size_differs:
            description = (
                f"Learn's dense vectors are {report.learn_vector_size} long and "
                f"the platform's {report.platform_vector_size}. {description}"
            )
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            description=description,
            metadata=metadata,
        )
    if report.points_differ:
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.WARN,
            description="Some points, texts or checksums differ from Learn's",
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
    strategy = strategies()[LEARN_V1_CONTENT_FILES_ID]
    platform = platform_chunks(
        require_table(LAYER, CONTENT_FILE_CHUNKS_KEY.path[-1])
        .filter(in_partition)
        .collect(),
        require_table(LAYER, CONTENT_FILE_VECTORS_KEY.path[-1])
        .filter(in_partition)
        .collect(),
        strategy,
    )
    client = learn_qdrant.get_client()
    collection = learn_qdrant.collection_name(LEARN_CONTENT_FILES_COLLECTION)
    vector_name = LEARN_CONTENT_FILES_COLLECTION.dense_vector_name
    fetch = partial(
        retrieve_points,
        client,
        collection,
        text_field="chunk_content",
        checksum_field="checksum",
        vector_name=vector_name,
    )
    source = CONTENT_FILE_SOURCES[etl_source]
    platform_code = source.platform_code
    if platform_code is None:
        # An xPRO run's code is its course's. A run with none, or no longer in
        # the runs model, gets "", the code Learn gives a resource with no
        # platform, so no xPRO point matches it.
        platform_code = (
            source.run_resources(require_table(LAYER, source.runs_relation))
            .filter(pl.col("run_id") == run)
            .collect()["platform_code"]
            .first()
        ) or ""
    learn_ids = scroll_point_ids(
        client, collection, content_file_run_filter(platform_code, run)
    )
    return check_result(
        compare_with_learn(
            platform,
            fetch,
            learn_ids,
            compare_checksums=True,
            reference_model_id=LEARN_MODEL_ID,
            platform_vector_size=strategy.model.vector_length,
            learn_vector_size=dense_vector_size(client, collection, vector_name),
        )
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
    strategy = strategies()[LEARN_V1_TOPICS_ID]
    platform = platform_chunks(
        require_table(LAYER, TOPIC_CHUNKS_KEY.path[-1]).collect(),
        require_table(LAYER, TOPIC_VECTORS_KEY.path[-1]).collect(),
        strategy,
    )
    client = learn_qdrant.get_client()
    collection = learn_qdrant.collection_name(LEARN_TOPICS_COLLECTION)
    vector_name = LEARN_TOPICS_COLLECTION.dense_vector_name
    fetch = partial(
        retrieve_points,
        client,
        collection,
        text_field="name",
        checksum_field=None,
        vector_name=vector_name,
    )
    learn_ids = scroll_point_ids(client, collection, None)
    return check_result(
        compare_with_learn(
            platform,
            fetch,
            learn_ids,
            compare_checksums=False,
            reference_model_id=LEARN_MODEL_ID,
            platform_vector_size=strategy.model.vector_length,
            learn_vector_size=dense_vector_size(client, collection, vector_name),
        )
    )


def learn_parity_definitions(
    vault: Vault, dagster_env: str
) -> tuple[dict[str, QdrantResource], list[AssetChecksDefinition]]:
    """Return the ``learn_qdrant`` resource and the checks, for deployed envs.

    The checks run with every build of the vector assets, so where they are
    defined their resource must resolve. Dev has no Learn cluster, so it gets
    neither, and its builds run without the checks. A deployed env without
    ``LEARN_QDRANT_COLLECTION_BASE`` fails here, when the code location loads.

    :param vault: the Vault resource the Qdrant credentials are read with
    :param dagster_env: ``dev``, ``qa`` or ``production``
    :returns: the resources by key, and the checks
    :rtype: tuple[dict[str, QdrantResource], list[AssetChecksDefinition]]
    :raises KeyError: outside dev, when LEARN_QDRANT_COLLECTION_BASE is unset
    """
    if dagster_env == "dev":
        return {}, []
    learn_qdrant = QdrantResource(
        vault=vault,
        vault_path="pipelines/qdrant",
        # e.g. mitlearn-production
        base_collection_name=os.environ["LEARN_QDRANT_COLLECTION_BASE"],
    )
    return {"learn_qdrant": learn_qdrant}, [
        content_file_vectors_match_learn,
        topic_vectors_match_learn,
    ]
