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

The Slack half of that is a notification, not a report: the existing formatter
carries the asset, the check and a run link, so the failed keys live in the
check metadata and the UI rather than in the message.
"""

import heapq
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

# Failed partition keys carried in the check metadata, where the Dagster UI
# renders them against the asset. The count is always exact; this only caps the
# sample.
#
# Note that the Slack notification does NOT carry them: data_platform's
# asset_check_failure_message renders the asset name, the check name and a link
# to the run, and nothing from the evaluation's metadata. Slack tells you which
# check went red; the keys are one click away, not in the message.
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


def _recovery_text(count: int, truncated: bool) -> str:  # noqa: FBT001
    """Say what has to happen next, and over how many partitions.

    The count matters as much as the procedure. Told to "re-materialize the
    listed partitions" against a truncated sample, an operator fixes twenty of
    them, sees the list they were given go green, and leaves the rest failed --
    which is the same silent staleness these checks exist to end.
    """
    scope = (
        f"all {count} failed partitions -- the sample below is the first "
        f"{MAX_REPORTED_PARTITION_KEYS}, and the asset's partition view in the "
        "Dagster UI has the rest"
        if truncated
        else "the partitions listed below"
    )
    return (
        "Nothing retries these automatically -- the automation condition spent "
        "its one retry when they first failed, and only an upstream change, a "
        "code version change or a manual re-materialization will ask for them "
        f"again. Fix the cause, then re-materialize {scope}."
    )


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

        # nsmallest rather than sorted()[:n]: sorting the whole set to keep
        # twenty of it would rebuild the very structure this function reads a
        # subset to avoid, and at O(F log F) rather than O(F log 20).
        truncated = count > MAX_REPORTED_PARTITION_KEYS
        sample = heapq.nsmallest(
            MAX_REPORTED_PARTITION_KEYS, failed.get_partition_keys()
        )
        return AssetCheckResult(
            passed=False,
            severity=AssetCheckSeverity.ERROR,
            metadata={
                "failed_partitions": MetadataValue.int(count),
                "sample": MetadataValue.json(sample),
                "sample_truncated": MetadataValue.bool(truncated),
                "recovery": MetadataValue.md(_recovery_text(count, truncated)),
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
