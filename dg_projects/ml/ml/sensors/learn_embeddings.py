"""Register content-file run partitions and report each run's text version."""

import polars as pl
from dagster import (
    AssetObservation,
    DefaultSensorStatus,
    MultiPartitionKey,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

# Not exported from the top-level dagster namespace; see openedx/sensors/openedx.py.
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from ml.assets.learn_embeddings import (
    CONTENT_FILE_RUNS,
    CONTENT_FILE_TEXT_KEY,
    content_file_text_versions,
)
from ml.lib.learn_embeddings import CONTENT_FILE_SOURCES
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.lib.embeddings.documents import md5_hex


@sensor(
    name="learn_content_file_text_sensor",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Registers a learn_content_file_run partition for every course run with "
        "content files and a known resource, and reports a digest of each run's "
        "files as learn_embeddings/learn_content_file_text's data version. "
        "learn_embeddings_automation_sensor then chunks and embeds the runs "
        "whose version moved."
    ),
)
def learn_content_file_text_sensor(context: SensorEvaluationContext):
    """Report every run's text version, once per change.

    The cursor is a digest of every run's version, so a tick where none changed
    reports nothing. Observations go out only for runs already registered: the
    daemon records asset events before it adds partitions, so a run registered on
    this tick is observed on the next, and the cursor is held back until then.
    """
    versions = pl.concat(
        [content_file_text_versions(source) for source in sorted(CONTENT_FILE_SOURCES)]
    )
    digest = md5_hex("\n".join("|".join(row) for row in versions.iter_rows()))
    if digest == context.cursor:
        return SensorResult()

    valid = versions.filter(
        ~pl.col("run_readable_id").map_elements(
            contains_invalid_partition_strings, return_dtype=pl.Boolean
        )
    )
    if valid.height < versions.height:
        context.log.warning(
            "Skipping %d course runs whose id is not a valid partition key",
            versions.height - valid.height,
        )
    registered = set(
        CONTENT_FILE_RUNS.get_partition_keys(dynamic_partitions_store=context.instance)
    )
    new_runs = sorted(set(valid["run_readable_id"]) - registered)
    observations = [
        AssetObservation(
            asset_key=CONTENT_FILE_TEXT_KEY,
            partition=MultiPartitionKey(
                {"etl_source": row["etl_source"], "run": row["run_readable_id"]}
            ),
            tags={DATA_VERSION_TAG: row["text_version"]},
        )
        for row in valid.iter_rows(named=True)
        if row["run_readable_id"] in registered
    ]
    context.log.info(
        "Observed %d course runs, registered %d new ones",
        len(observations),
        len(new_runs),
    )
    return SensorResult(
        asset_events=observations,
        dynamic_partitions_requests=(
            [CONTENT_FILE_RUNS.build_add_request(new_runs)] if new_runs else []
        ),
        cursor=context.cursor if new_runs else digest,
    )
