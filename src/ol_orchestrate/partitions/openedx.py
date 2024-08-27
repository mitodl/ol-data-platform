from dagster import (
    DynamicPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

OPENEDX_DEPLOYMENT_PARTITION = StaticPartitionsDefinition(
    ["mitxonline", "xpro", "mitx"]
)
OPENEDX_COURSE_RUN_PARTITION = DynamicPartitionsDefinition(name="openedx_course_run")
OPENEDX_COURSE_AND_SOURCE_PARTITION = MultiPartitionsDefinition(
    {
        "source_system": OPENEDX_DEPLOYMENT_PARTITION,
        "course_key": OPENEDX_COURSE_RUN_PARTITION,
    }
)
