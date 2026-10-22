"""Register course run partitions and report each run's text checksum."""

import hashlib

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
from ml.assets.courseware_extraction import (
    EXTRACTION_TEXT_KEY,
    EXTRACTOR_VERSION,
    approved,
    extraction_text_version,
    text_checksums,
)
from ml.lib.courseware.warehouse import (
    COURSERUN_PARTITIONS,
    COURSEWARE_PLATFORMS,
)
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings


@sensor(
    name="courseware_extraction_text_sensor",
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    description=(
        "Registers a courseware_courserun partition for every run "
        "courseware_extraction_allowlist approves for the deployed extractor "
        "version, and reports each one's text checksum as "
        "courseware/extraction_text's data version. courseware_automation_sensor "
        "then extracts the runs whose version moved."
    ),
)
def courseware_extraction_text_sensor(context: SensorEvaluationContext):
    """Report every approved course run's text checksum, once per change.

    int__courseware__extraction_source_checksums holds only allowlisted runs, each
    with the extractor version it is approved for. A run approved for any other
    version than this code's is neither registered nor observed, so its extraction
    waits until its allowlist row is promoted.

    The cursor is a digest of the whole checksum table and this code's extractor
    version, so a tick where neither changed reports nothing. Observations go out
    only for runs already registered: the daemon records asset events before it
    adds partitions, so a run registered on this tick is observed on the next, and
    the cursor is held back until then.
    """
    rows = text_checksums().sort("platform", "courserun_readable_id")
    unknown = set(rows["platform"]) - set(COURSEWARE_PLATFORMS)
    if unknown:
        msg = f"int__courseware__extraction_sources has unknown platforms {unknown}"
        raise ValueError(msg)
    digest = hashlib.sha256(
        "\n".join(
            ["|".join(EXTRACTOR_VERSION.values())]
            + ["|".join(row) for row in rows.iter_rows()]
        ).encode()
    ).hexdigest()
    if digest == context.cursor:
        return SensorResult()

    ready = approved(rows)
    if ready.height < rows.height:
        context.log.warning(
            "%d allowlisted course runs are approved for another extractor version "
            "than %s and wait for courseware_extraction_allowlist to promote them",
            rows.height - ready.height,
            "-".join(EXTRACTOR_VERSION.values()),
        )
    valid = [
        row
        for row in ready.iter_rows(named=True)
        if not contains_invalid_partition_strings(row["courserun_readable_id"])
    ]
    if len(valid) < ready.height:
        context.log.warning(
            "Skipping %d course runs whose id is not a valid partition key",
            ready.height - len(valid),
        )
    registered = set(
        COURSERUN_PARTITIONS.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_courseruns = sorted(
        {row["courserun_readable_id"] for row in valid} - registered
    )
    observations = [
        AssetObservation(
            asset_key=EXTRACTION_TEXT_KEY,
            partition=MultiPartitionKey(
                {"platform": row["platform"], "courserun": row["courserun_readable_id"]}
            ),
            tags={DATA_VERSION_TAG: extraction_text_version(row["text_checksum"])},
        )
        for row in valid
        if row["courserun_readable_id"] in registered
    ]
    context.log.info(
        "Observed %d course runs, registered %d new ones",
        len(observations),
        len(new_courseruns),
    )
    return SensorResult(
        asset_events=observations,
        dynamic_partitions_requests=(
            [COURSERUN_PARTITIONS.build_add_request(new_courseruns)]
            if new_courseruns
            else []
        ),
        cursor=context.cursor if new_courseruns else digest,
    )
