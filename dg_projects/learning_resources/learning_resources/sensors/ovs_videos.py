"""Sensors for managing OVS video partitions and triggering webhook runs."""

import time

from dagster import (
    AddDynamicPartitionsRequest,
    AssetSelection,
    DagsterRunStatus,
    DefaultSensorStatus,
    DeleteDynamicPartitionsRequest,
    RunRequest,
    SensorResult,
    SkipReason,
    define_asset_job,
    run_status_sensor,
    sensor,
)

from learning_resources.assets.ovs_videos import video_api_key

ovs_videos_delete_job = define_asset_job(
    name="ovs_videos_delete_job",
    description="Send stale OVS video deletion webhook for specific partitions",
    selection=AssetSelection.keys(
        ["ovs_videos", "video_delete_webhook"],
    ),
)


@sensor(
    name="ovs_videos_discovery_sensor",
    description=(
        "Adds new OVS video partitions and triggers webhook runs for new or "
        "content-changed include_in_learn videos."
    ),
    minimum_interval_seconds=600,
    default_status=DefaultSensorStatus.STOPPED,
    job_name="ovs_videos_webhook_job",
)
def ovs_videos_discovery_sensor(context):
    """Trigger webhook runs for new or content-changed OVS videos."""
    api_materialization = context.instance.get_latest_materialization_event(
        video_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial OVS video_api materialization")

    metadata = api_materialization.asset_materialization.metadata
    hashes_meta = metadata.get("video_content_hashes")
    if not hashes_meta:
        return SkipReason(
            "Waiting for materialization with video_content_hashes metadata"
        )
    video_content_hashes: dict[str, str] = hashes_meta.value or {}

    if not video_content_hashes:
        return SkipReason("No videos returned by OVS API")

    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("ovs_video_ids")
    )
    current_partition_keys = set(video_content_hashes)
    new_partition_keys = sorted(current_partition_keys - existing_video_partitions)

    if new_partition_keys:
        context.log.info(
            "Adding %d new OVS video partitions",
            len(new_partition_keys),
        )

    # One run request per current partition, keyed by content hash. Dagster's
    # run_key dedup means only new or content-changed videos actually launch.
    run_requests = [
        RunRequest(
            partition_key=partition_key,
            run_key=f"ovs_videos_{partition_key}_{content_hash}",
        )
        for partition_key, content_hash in sorted(video_content_hashes.items())
    ]

    dynamic_partitions_requests = (
        [
            AddDynamicPartitionsRequest(
                partitions_def_name="ovs_video_ids",
                partition_keys=new_partition_keys,
            )
        ]
        if new_partition_keys
        else []
    )

    return SensorResult(
        dynamic_partitions_requests=dynamic_partitions_requests,
        run_requests=run_requests,
    )


@sensor(
    name="ovs_videos_stale_cleanup_sensor",
    description=(
        "Detects stale OVS video partitions (no longer include_in_learn) and "
        "triggers the deletion webhook workflow."
    ),
    minimum_interval_seconds=3600 * 24,
    default_status=DefaultSensorStatus.STOPPED,
    job_name="ovs_videos_delete_job",
)
def ovs_videos_stale_cleanup_sensor(context):
    """Trigger delete webhook runs for OVS video partitions no longer in the API."""
    api_materialization = context.instance.get_latest_materialization_event(
        video_api_key
    )

    if not api_materialization:
        return SkipReason("Waiting for initial OVS video_api materialization")

    metadata = api_materialization.asset_materialization.metadata
    api_keys_meta = metadata.get("api_partition_keys")
    if not api_keys_meta:
        return SkipReason(
            "Waiting for materialization with complete api_partition_keys metadata"
        )
    partition_keys = api_keys_meta.value

    if not partition_keys:
        return SkipReason("OVS API returned zero videos; refusing to compute stale set")

    existing_video_partitions = set(
        context.instance.get_dynamic_partitions("ovs_video_ids")
    )
    stale_partition_keys = existing_video_partitions - set(partition_keys)

    if not stale_partition_keys:
        return SkipReason("No stale OVS videos found")

    sorted_stale_partition_keys = sorted(stale_partition_keys)
    sample = sorted_stale_partition_keys[:5]
    context.log.info(
        "Launching OVS stale cleanup runs for %d partitions. First %d keys: %s",
        len(sorted_stale_partition_keys),
        len(sample),
        sample,
    )

    tick_ts = int(time.time())
    run_requests = [
        RunRequest(
            partition_key=partition_key,
            run_key=f"ovs_videos_delete_{partition_key}_{tick_ts}",
            tags={"ovs_videos_stale_cleanup": "true"},
        )
        for partition_key in sorted_stale_partition_keys
    ]

    return SensorResult(run_requests=run_requests)


@run_status_sensor(
    name="ovs_videos_delete_partition_cleanup_sensor",
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[ovs_videos_delete_job],
    minimum_interval_seconds=60,
    default_status=DefaultSensorStatus.STOPPED,
)
def ovs_videos_delete_partition_cleanup_sensor(context):
    """Remove dynamic partitions after sensor-triggered OVS deletes succeed."""
    if not context.dagster_run.tags.get("ovs_videos_stale_cleanup"):
        return SkipReason("Run was not triggered by stale cleanup sensor")

    partition_key = context.dagster_run.tags.get("dagster/partition")
    if not partition_key:
        return SkipReason("Delete workflow run missing partition tag")

    if not context.instance.has_dynamic_partition("ovs_video_ids", partition_key):
        return SkipReason(f"Partition already removed or not found: {partition_key}")

    context.log.info(
        "Removing stale OVS video partition after successful delete run: %s",
        partition_key,
    )

    return SensorResult(
        dynamic_partitions_requests=[
            DeleteDynamicPartitionsRequest(
                partitions_def_name="ovs_video_ids",
                partition_keys=[partition_key],
            )
        ],
    )
