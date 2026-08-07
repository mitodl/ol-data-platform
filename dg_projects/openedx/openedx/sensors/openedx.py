from dagster import (
    SensorEvaluationContext,
    SensorResult,
)
from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory

from openedx.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)


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
