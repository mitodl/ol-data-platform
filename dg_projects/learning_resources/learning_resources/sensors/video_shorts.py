"""Sensors for monitoring Google Sheets and triggering video processing."""

from dagster import (
    AddDynamicPartitionsRequest,
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    RunRequest,
    SensorResult,
    SkipReason,
    define_asset_job,
    run_status_sensor,
    sensor,
)

from learning_resources.assets.video_shorts import (
    sheets_api_key,
)

video_shorts_delete_job = define_asset_job(
    name="video_shorts_delete_job",
    description="Send stale video deletion webhook for specific partitions",
    selection=AssetSelection.keys(
        ["video_shorts", "video_delete_webhook"],
    ),
)

# Safety threshold: skip deletions if more than this many partitions would be removed.
# Protects against mass deletion if the sheet API returns partial/empty data.
MAX_STALE_DELETIONS = 6


@sensor(
    name="video_shorts_discovery_sensor",
    description=(
        "Manages video partitions and triggers runs for new videos from Google Sheets."
    ),
    minimum_interval_seconds=3600,  # Check every hour
    default_status=DefaultSensorStatus.STOPPED,
    job_name="video_shorts_video_job",
)
def video_shorts_discovery_sensor(context):
    """
    Discovery sensor: manages partition creation for new videos.
    """
    # Check if we have a recent sheets_api materialization
    api_materialization = context.instance.get_latest_materialization_event(
        sheets_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial sheets_api materialization")

    # Extract metadata from the sheets_api materialization
    metadata = api_materialization.asset_materialization.metadata

    partition_keys = (
        metadata.get("processing_partition_keys").value
        if metadata.get("processing_partition_keys")
        else (
            metadata.get("partition_keys").value
            if metadata.get("partition_keys")
            else []
        )
    )

    if not partition_keys:
        return SkipReason("No videos to process found in Google Sheets")

    # Get existing video partitions
    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("video_short_ids")
    )
    current_partition_keys = set(partition_keys)

    new_partition_keys = current_partition_keys - existing_video_partitions

    if not new_partition_keys:
        return SkipReason("No new videos found")

    sorted_new_partition_keys = sorted(new_partition_keys)
    context.log.info(
        "Adding %d new video partitions",
        len(sorted_new_partition_keys),
    )

    run_requests = [
        RunRequest(
            partition_key=partition_key,
            run_key=(f"video_shorts_{partition_key}_{api_materialization.run_id}"),
        )
        for partition_key in sorted_new_partition_keys
    ]

    return SensorResult(
        dynamic_partitions_requests=[
            AddDynamicPartitionsRequest(
                partitions_def_name="video_short_ids",
                partition_keys=sorted_new_partition_keys,
            )
        ],
        run_requests=run_requests,
    )


@sensor(
    name="video_shorts_stale_cleanup_sensor",
    description=(
        "Detects stale video partitions and triggers the deletion webhook workflow."
    ),
    minimum_interval_seconds=3600,
    default_status=DefaultSensorStatus.STOPPED,
    job_name="video_shorts_delete_job",
)
def video_shorts_stale_cleanup_sensor(context):
    """Sensor that triggers delete workflow runs for stale video partitions."""
    api_materialization = context.instance.get_latest_materialization_event(
        sheets_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial sheets_api materialization")

    metadata = api_materialization.asset_materialization.metadata
    partition_keys = (
        metadata.get("sheet_partition_keys").value
        if metadata.get("sheet_partition_keys")
        else (
            metadata.get("partition_keys").value
            if metadata.get("partition_keys")
            else []
        )
    )

    if not partition_keys:
        return SkipReason("No videos to process found in Google Sheets")

    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("video_short_ids")
    )
    stale_partition_keys = existing_video_partitions - set(partition_keys)

    if not stale_partition_keys:
        return SkipReason("No stale videos found")

    if len(stale_partition_keys) > MAX_STALE_DELETIONS:
        sample = sorted(stale_partition_keys)[:5]
        context.log.warning(
            "Skipping stale cleanup run launch: %d stale partitions exceeds "
            "safety threshold of %d. First %d stale keys: %s",
            len(stale_partition_keys),
            MAX_STALE_DELETIONS,
            len(sample),
            sample,
        )
        return SkipReason("Stale partition count exceeds safety threshold")

    sorted_stale_partition_keys = sorted(stale_partition_keys)
    sample = sorted_stale_partition_keys[:5]
    context.log.info(
        "Launching stale cleanup runs for %d partitions. First %d keys: %s",
        len(sorted_stale_partition_keys),
        len(sample),
        sample,
    )

    run_requests = [
        RunRequest(
            partition_key=partition_key,
            run_key=(
                f"video_shorts_delete_{partition_key}_{api_materialization.run_id}"
            ),
        )
        for partition_key in sorted_stale_partition_keys
    ]

    return SensorResult(run_requests=run_requests)


@run_status_sensor(
    name="video_shorts_delete_partition_cleanup_sensor",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[video_shorts_delete_job],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
)
def video_shorts_delete_partition_cleanup_sensor(context):
    """Delete dynamic partitions only after stale-delete runs succeed."""
    partition_key = context.dagster_run.tags.get("dagster/partition")
    if not partition_key:
        return SkipReason("Delete workflow run missing partition tag")

    if not context.instance.has_dynamic_partition("video_short_ids", partition_key):
        return SkipReason(f"Partition already removed or not found: {partition_key}")

    context.instance.delete_dynamic_partition("video_short_ids", partition_key)
    context.log.info(
        "Removed stale video partition after successful delete run: %s",
        partition_key,
    )

    return SensorResult()
