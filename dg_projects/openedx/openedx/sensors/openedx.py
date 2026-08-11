from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from dagster import (
    AssetKey,
    AssetObservation,
    SensorEvaluationContext,
    SensorResult,
)

# Not exported from the top-level dagster namespace. Importing the constant
# rather than hard-coding "dagster/data_version" keeps a version bump that moves
# it a loud import error instead of observations that silently carry no version.
from dagster._core.definitions.data_version import (
    DATA_VERSION_TAG,
)
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory

from openedx.assets.openedx import COURSEWARE_ASSET_KEY, sweep_course_versions
from openedx.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)

# The gRPC tick timeout is 300s. 180s leaves the sweep room to finish while
# keeping enough margin for the partition and cursor work around it -- timing
# only the fetch phase is what let a slow event-log query eat the margin and
# reproduce the killed-tick-with-no-output failure the first time around.
COURSEWARE_SWEEP_BUDGET = timedelta(seconds=180)


def course_run_sensor(
    context: SensorEvaluationContext,
    openedx: OpenEdxApiClientFactory,
):
    """Register a dynamic partition for every course run the LMS reports.

    Partition discovery only. Exports are left entirely to the asset graph: the
    openedx/courseware observable source asset picks up the new partition on its
    next observation, and ``upstream_or_code_changes()`` on course_xml treats a
    partition with no materialization as needing one.

    Requesting runs here as well used to double-export every new course, and it
    bypassed the throttling that kept a bulk course creation from flooding the
    run queue. Leaving every export to the automation condition is what keeps
    the export path single and observable.
    """
    # Enumerate the course-run IDs from edX via the API
    course_id_generator = openedx.client.get_edx_course_ids()
    course_run_ids = []
    for result_set in course_id_generator:
        course_run_ids.extend(
            [
                course["id"]
                for course in result_set
                if not contains_invalid_partition_strings(course["id"])
            ]
        )
    existing_keys = set(
        OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )
    new_course_run_ids = set(course_run_ids) - existing_keys
    context.log.info(
        "Registering %s new %s course run partitions.",
        len(new_course_run_ids),
        openedx.deployment,
    )
    return SensorResult(
        dynamic_partitions_requests=[
            OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].build_add_request(
                # Sorted because the difference above is a set: a stable order
                # keeps tick logs and any downstream diff readable.
                partition_keys=sorted(new_course_run_ids)
            )
        ],
    )


def cursor_offset(cursor: str | None) -> int:
    """Read the sweep offset out of a sensor cursor.

    Anything unparseable restarts from the top rather than failing the tick: the
    cursor is a fairness hint, and losing it costs one pass over the head of the
    list, while raising here would stop observation for the whole deployment.
    """
    try:
        return max(0, int(cursor)) if cursor else 0
    except ValueError:
        return 0


def resume_order(course_run_ids: Sequence[str], offset: int) -> list[str]:
    """Order a sweep so it starts where the last one stopped.

    A sweep that runs out of budget always abandons its tail. Starting the next
    one at the same place would sweep the head of the list forever and never
    look at the courses behind it, so the list is rotated by the offset the
    previous tick left behind.
    """
    ordered = sorted(course_run_ids)
    if not ordered:
        return ordered
    start = offset % len(ordered)
    return ordered[start:] + ordered[:start]


def courseware_observation_sensor(
    context: SensorEvaluationContext,
    openedx: OpenEdxApiClientFactory,
):
    """Report the published version of every course run as an observation.

    This is the whole trigger for the export graph. Every downstream carries
    ``upstream_or_code_changes()``, whose ``data_version_changed()`` term fires
    against the versions reported here; a course whose version is unchanged
    reports the same value and asks for nothing, which is what keeps a steady
    state quiet.

    It is a sensor rather than the source asset's own automation condition
    because an AutomationCondition is evaluated per partition. Hanging an hourly
    cron on a 3,500-partition asset asked for 3,500 observation runs an hour,
    each of which swept the entire deployment anyway -- millions of LMS calls an
    hour, a permanently saturated run queue, and no exports. One tick, one
    sweep, no runs.
    """
    deployment = openedx.deployment
    course_run_ids = OPENEDX_COURSE_RUN_PARTITIONS[deployment].get_partition_keys(
        dynamic_partitions_store=context.instance
    )
    if not course_run_ids:
        context.log.info("No %s course run partitions to observe.", deployment)
        return SensorResult()

    sorted_ids = sorted(course_run_ids)
    ordered = resume_order(sorted_ids, cursor_offset(context.cursor))
    sweep = sweep_course_versions(
        openedx.client,
        ordered,
        context.log,
        deadline=datetime.now(tz=UTC) + COURSEWARE_SWEEP_BUDGET,
    )
    context.log.info(
        "Observed %s of %s %s course runs, %s failed, %s left for the next tick.",
        len(sweep.versions),
        len(ordered),
        deployment,
        sweep.failures,
        len(sweep.unswept),
    )

    # A pass where every lookup failed is a bad token or a 500-ing LMS, not a
    # deployment with nothing to say. Failing the tick surfaces it instead of
    # leaving every downstream quiet, hourly, forever.
    attempted = len(sweep.versions) + sweep.failures
    if attempted and sweep.failures == attempted:
        msg = (
            f"Course outline sweep failed for all {sweep.failures} attempted "
            f"{deployment} courses"
        )
        raise RuntimeError(msg)

    courseware_key = AssetKey([deployment, *COURSEWARE_ASSET_KEY.path])
    return SensorResult(
        asset_events=[
            AssetObservation(
                asset_key=courseware_key,
                partition=course_run_id,
                tags={DATA_VERSION_TAG: version},
            )
            for course_run_id, version in sweep.versions.items()
        ],
        # Resume at the first course this pass did not get to. ``unswept`` is
        # built in submission order, so its head is the earliest thing missed --
        # which is not the same as advancing by the number consumed, because
        # completion order is not submission order and the course that blocked
        # can sit anywhere in the list. A pass that finished goes back to the
        # top, since there is nothing outstanding to be fair to.
        cursor=str(sorted_ids.index(sweep.unswept[0])) if sweep.unswept else "0",
    )
