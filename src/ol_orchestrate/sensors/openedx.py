from dagster import (
    SensorEvaluationContext,
    SensorResult,
    sensor,
)

from ol_orchestrate.lib.dagster_helpers import contains_invalid_partition_strings
from ol_orchestrate.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITIONS,
)
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory


@sensor(
    name="openedx_courseware_sensor",
    minimum_interval_seconds=60 * 60,
    description="Query a running Open edX system for courseware definitions "
    "and version changes.",
)
def course_run_sensor(
    context: SensorEvaluationContext,
    openedx: OpenEdxApiClientFactory,
):
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
    return SensorResult(
        dynamic_partitions_requests=[
            OPENEDX_COURSE_RUN_PARTITIONS[openedx.deployment].build_add_request(
                partition_keys=list(new_course_run_ids)
            )
        ],
    )
