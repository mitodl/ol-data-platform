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
from ml.assets.courseware_extraction import EXTRACTION_TEXT_KEY, text_checksums
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
        "Registers a courseware_courserun partition for every run in "
        "int__courseware__extraction_source_checksums and reports each run's text "
        "checksum as courseware/extraction_text's data version. "
        "courseware_automation_sensor then extracts the runs whose version moved."
    ),
)
def courseware_extraction_text_sensor(context: SensorEvaluationContext):
    """Report every course run's text checksum, once per change to the table.

    The cursor is a digest of the whole checksum table, so a tick where nothing
    changed reports nothing. Observations go out only for runs already registered:
    the daemon records asset events before it adds partitions, so a run registered
    on this tick is observed on the next, and the cursor is held back until then.
    """
    rows = text_checksums().sort("platform", "courserun_readable_id")
    unknown = set(rows["platform"]) - set(COURSEWARE_PLATFORMS)
    if unknown:
        msg = f"int__courseware__extraction_sources has unknown platforms {unknown}"
        raise ValueError(msg)
    digest = hashlib.sha256(
        "\n".join("|".join(row) for row in rows.iter_rows()).encode()
    ).hexdigest()
    if digest == context.cursor:
        return SensorResult()

    valid = [
        row
        for row in rows.iter_rows(named=True)
        if not contains_invalid_partition_strings(row["courserun_readable_id"])
    ]
    if len(valid) < rows.height:
        context.log.warning(
            "Skipping %d course runs whose id is not a valid partition key",
            rows.height - len(valid),
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
            tags={DATA_VERSION_TAG: row["text_checksum"]},
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
