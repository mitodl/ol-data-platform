from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from metadata.workflow.metadata import MetadataWorkflow

from ol_orchestrate.partitions.openmetadata import TRINO_DAILY_PARTITIONS
from ol_orchestrate.resources.openmetadata.trino import trino_config


@asset(
    description=("An instance of metadata from our Trino database."),
    group_name="platform",
    key=AssetKey(["platform", "database", "trino", "metadata"]),
    partitions_def=TRINO_DAILY_PARTITIONS,
)
def trino_metadata(context: AssetExecutionContext):
    date = context.partition_key
    workflow = MetadataWorkflow.create(trino_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()
    return Output(
        workflow,
        data_version=DataVersion(workflow["published_version"]),
        metadata={"source": "Trino", "date": date},
    )
