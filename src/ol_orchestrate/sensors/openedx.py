import httpx
from dagster import (
    AssetKey,
    AssetMaterialization,
    MultiPartitionKey,
    SensorEvaluationContext,
    SensorResult,
    sensor,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG

from ol_orchestrate.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITION,
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
        course_run_ids.extend([course["id"] for course in result_set])
    existing_keys = set(
        OPENEDX_COURSE_RUN_PARTITION.get_partition_keys(
            dynamic_partitions_store=context.instance
        )
    )

    # For each courserun ID retrieve the last published timestamp from
    # /learning_sequences/v1/course_outline/{course_key_str}. Yield an
    # AssetMaterialization event for each course key, using the last published
    # information as the data version
    assets = []
    for courserun_id in set(course_run_ids).difference(existing_keys):
        try:
            course_outline = openedx.client.get_course_outline(courserun_id)
        except httpx.HTTPStatusError:
            context.log.info(
                "Unable to retrieve the course outline for %s", courserun_id
            )
            continue
        course_asset = AssetMaterialization(
            asset_key=AssetKey(["openedx", "courseware"]),
            description=(
                f"An instance of courseware running in an Open edX environment located at {openedx.client.lms_url}"  # noqa: E501
            ),
            partition=MultiPartitionKey(
                {"source_system": openedx.deployment, "course_key": courserun_id}
            ),
            tags={
                DATA_VERSION_TAG: course_outline["published_version"],
            },
            metadata={
                "course_key": courserun_id,
                "course_title": course_outline["title"],
                "courseware_published_version": course_outline["published_version"],
                "courseware_published_at": course_outline["published_at"],
            },
        )
        assets.append(course_asset)

    return SensorResult(
        assets=assets,
        dynamic_partitions_requests=OPENEDX_COURSE_RUN_PARTITION.build_add_request(
            partition_keys=course_run_ids
        ),
    )
