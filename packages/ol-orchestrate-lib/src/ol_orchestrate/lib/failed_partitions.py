"""Surface partitions that are sitting in a failed state.

``upstream_or_code_changes()`` bounds the failure retry to one attempt per
failure edge, and ``stop_run_retries`` keeps ``run_retries`` off a permanent
failure. Both are deliberate, and together they mean a partition that cannot
succeed goes quiet after two runs rather than being re-requested forever.

Quiet is the point, and quiet is also the danger. Nothing then re-requests that
partition until its upstream changes, its code version changes, or a human
re-materializes it -- and the Sentry issue that reported the original failure
falls silent, which in a list of issues is indistinguishable from being fixed.
Silent staleness is the failure mode that let one broken asset run undetected
for six days; bounding the retry removed the noise without replacing the signal.

These checks are that replacement. They answer "what is broken *right now*"
rather than "what broke at 03:14", which is the question a standing inventory
has to answer to be worth reading.

Wire them into a code location alongside the assets they watch::

    failed_partition_checks = build_failed_partition_checks(
        [edxorg_course_xml, edxorg_course_structure]
    )

    defs = Definitions(
        assets=with_failure_hooks([...]),
        asset_checks=failed_partition_checks,
        jobs=[failed_partition_check_job(failed_partition_checks)],
        schedules=[failed_partition_check_schedule(failed_partition_checks)],
    )

An asset check rather than a bespoke sensor, for three reasons: the Dagster UI
renders it against the asset it concerns, ``asset_check_failure_sensor`` in the
data_platform code location already announces ERROR-severity check failures to
Slack, and a check is evaluated on a schedule independent of materialization --
which is what makes it a standing signal rather than another event.
"""

from collections.abc import Sequence

from dagster import (
    AssetCheckResult,
    AssetChecksDefinition,
    AssetCheckSeverity,
    AssetKey,
    AssetsDefinition,
    AssetSelection,
    DefaultScheduleStatus,
    JobDefinition,
    MetadataValue,
    ScheduleDefinition,
    asset_check,
    define_asset_job,
)

# Failed partition keys carried in the check metadata. Enough to start work from
# the notification alone; the count is always exact, and the UI has the rest.
MAX_REPORTED_PARTITION_KEYS = 20

FAILED_PARTITION_CHECK_NAME = "no_partitions_left_failed"
FAILED_PARTITION_JOB_NAME = "failed_partition_inventory"

# Daily rather than hourly. This reports a standing condition that a human has
# to act on, so re-announcing it every hour would recreate the noise problem
# that bounding the retry was meant to solve.
FAILED_PARTITION_CRON = "0 13 * * *"


def failed_partition_subset(instance, asset_key: AssetKey, partitions_def):
    """Return the partitions of ``asset_key`` whose latest run failed.

    Read from Dagster's partition status cache, which stores the failed set as
    a serialized subset. Deliberately not ``instance.get_status_by_partition``,
    which expands to a status-per-key mapping -- for the edxorg archives that is
    a dict of several hundred thousand entries built to answer a question about
    its length.
    """
    from dagster._core.storage.partition_status_cache import (  # noqa: PLC0415
        get_and_update_asset_status_cache_value,
    )

    cache_value = get_and_update_asset_status_cache_value(
        instance, asset_key, partitions_def
    )
    if cache_value is None:
        # No materialization recorded for the asset at all, so nothing has had
        # the chance to fail yet.
        return None
    return cache_value.deserialize_failed_partition_subsets(partitions_def)


def build_failed_partition_checks(
    assets: Sequence[AssetsDefinition],
) -> list[AssetChecksDefinition]:
    """Build one "no partitions left failed" check per partitioned asset.

    Unpartitioned assets are skipped rather than raising: a run-level failure on
    one of those is already visible as a failed run, and the check would have
    nothing to count.
    """
    checks: list[AssetChecksDefinition] = []
    for asset in assets:
        partitions_def = asset.partitions_def
        if partitions_def is None:
            continue
        checks.extend(_check_for(asset_key, partitions_def) for asset_key in asset.keys)
    return checks


def _check_for(asset_key: AssetKey, partitions_def) -> AssetChecksDefinition:
    @asset_check(
        asset=asset_key,
        name=FAILED_PARTITION_CHECK_NAME,
        blocking=False,
        description=(
            "Fails while any partition of this asset is sitting in a failed "
            "state. The automation condition retries a failure once and then "
            "goes quiet, so without this nothing reports that the partition is "
            "still broken."
        ),
    )
    def _check(context) -> AssetCheckResult:
        failed = failed_partition_subset(context.instance, asset_key, partitions_def)
        count = len(failed) if failed is not None else 0
        if not count:
            return AssetCheckResult(
                passed=True, metadata={"failed_partitions": MetadataValue.int(0)}
            )

        keys = sorted(failed.get_partition_keys())
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "failed_partitions": MetadataValue.int(count),
                "sample": MetadataValue.json(keys[:MAX_REPORTED_PARTITION_KEYS]),
                "sample_truncated": MetadataValue.bool(
                    count > MAX_REPORTED_PARTITION_KEYS
                ),
                "recovery": MetadataValue.md(
                    "Nothing retries these automatically -- the automation "
                    "condition spent its one retry when they first failed, and "
                    "only an upstream change, a code version change or a manual "
                    "re-materialization will ask for them again. Fix the cause, "
                    "then re-materialize the listed partitions."
                ),
            },
        )

    return _check


def failed_partition_check_job(
    checks: Sequence[AssetChecksDefinition],
) -> JobDefinition:
    """Build a job that evaluates the inventory checks and nothing else."""
    return define_asset_job(
        name=FAILED_PARTITION_JOB_NAME,
        selection=AssetSelection.checks(*checks),
        description=(
            "Evaluates the failed-partition inventory checks. Runs on a schedule "
            "rather than with each materialization, because the question is what "
            "is still broken rather than what just broke."
        ),
    )


def failed_partition_check_schedule(
    checks: Sequence[AssetChecksDefinition],
    cron_schedule: str = FAILED_PARTITION_CRON,
) -> ScheduleDefinition:
    """Evaluate the inventory once a day."""
    return ScheduleDefinition(
        name=f"{FAILED_PARTITION_JOB_NAME}_schedule",
        job=failed_partition_check_job(checks),
        cron_schedule=cron_schedule,
        default_status=DefaultScheduleStatus.STOPPED,
    )
