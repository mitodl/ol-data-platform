import yaml
from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from metadata.workflow.metadata import MetadataWorkflow

from ol_orchestrate.partitions.openmetadta import TRINO_DAILY_PARTITIONS
from ol_orchestrate.resources.openmetadata.trino import trino_config

# Specify your Trino configuration
# yaml str -> dict
trino_dict = yaml.safe_load(trino_config)


@asset(
    description=("An instance of metadata from our Trino database."),
    group_name="openmetadata",
    key=AssetKey(["openmetadata", "trino"]),
    partitions_def=TRINO_DAILY_PARTITIONS,
)
def trino_metadata(context: AssetExecutionContext):
    date = context.partition_key
    workflow_config = yaml.safe_load(trino_dict)
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()
    return Output(
        workflow,
        data_version=DataVersion(workflow["published_version"]),
        metadata={"source": "Trino", "date": date},
    )
