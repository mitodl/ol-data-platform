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

# Until the smokecheck against Learn's collections passes, only a sample of runs is
# registered, so automation and backfills embed only these. To lift it, delete the
# two constants and in_sample, and register every run in new_runs below.
#
# The sample is the first SAMPLE_RUNS_PER_SOURCE runs of each etl_source in md5
# order of their run_readable_id, which is stable and spread over the courses,
# plus runs picked for the smokecheck.
SAMPLE_RUNS_PER_SOURCE = 5
SAMPLE_NAMED_RUNS = frozenset(
    {
        # Learn's text for an OCW file is the platform's, so these should match
        # Learn's points exactly: 18.06 spring 2010, 6.0001 fall 2016 and 18.01
        # fall 2006.
        ("ocw", "aabc44bdb2e45374d62f30f2a6d4c63e"),  # pragma: allowlist secret
        ("ocw", "9833b95b8cbb8e0e7928063857ea2761"),  # pragma: allowlist secret
        ("ocw", "f5d4bf19ee14590de94d9e2b266614ba"),  # pragma: allowlist secret
        # OLX runs, whose text only approximates Learn's
        # (docs/learn_embedding_config.md).
        ("mitxonline", "course-v1:MITxT+7.05x+2T2026"),
        ("xpro", "course-v1:xPRO+SysEngx2+R28"),
    }
)


def in_sample(versions: pl.DataFrame) -> pl.DataFrame:
    """Return the runs of the sample among a sensor tick's run versions.

    :param versions: ``etl_source``, ``run_readable_id`` and ``text_version`` rows
    :rtype: pl.DataFrame
    """
    named = pl.concat_str("etl_source", "run_readable_id", separator="|").is_in(
        [f"{etl_source}|{run}" for etl_source, run in SAMPLE_NAMED_RUNS]
    )
    first = (
        pl.col("run_readable_id")
        .map_elements(md5_hex, return_dtype=pl.String)
        .rank("ordinal")
        .over("etl_source")
        <= SAMPLE_RUNS_PER_SOURCE
    )
    return versions.filter(named | first)


@sensor(
    name="learn_content_file_text_sensor",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Registers a learn_content_file_run partition for a sample of the course "
        "runs with content files and a known resource, and reports a digest of "
        "each run's files as learn_embeddings/learn_content_file_text's data version. "
        "learn_embeddings_automation_sensor then chunks and embeds the runs "
        "whose version moved."
    ),
)
def learn_content_file_text_sensor(context: SensorEvaluationContext):
    """Report every run's text version, once per change.

    The cursor is a digest of every run's version, so a tick where none changed
    reports nothing. Observations go out only for runs already registered: the
    daemon records asset events before it adds partitions, so a run registered on
    this tick is observed on the next, and the cursor is held back until then. A
    registered run that lost its last file is observed with the empty version, so
    its partition is rebuilt with no rows.
    """
    registered = set(
        CONTENT_FILE_RUNS.get_partition_keys(dynamic_partitions_store=context.instance)
    )
    versions = pl.concat(
        [
            content_file_text_versions(source, registered=registered)
            for source in sorted(CONTENT_FILE_SOURCES)
        ]
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
    new_runs = sorted(set(in_sample(valid)["run_readable_id"]) - registered)
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
