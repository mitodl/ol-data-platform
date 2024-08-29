from dagster import (
    DynamicPartitionsDefinition,
    StaticPartitionsDefinition,
)

from ol_orchestrate.lib.constants import OPENEDX_DEPLOYMENTS

OPENEDX_DEPLOYMENT_PARTITION = StaticPartitionsDefinition(OPENEDX_DEPLOYMENTS)
OPENEDX_COURSE_RUN_PARTITIONS = {
    deployment: DynamicPartitionsDefinition(name=f"{deployment}_openedx_course_run")
    for deployment in OPENEDX_DEPLOYMENTS
}
