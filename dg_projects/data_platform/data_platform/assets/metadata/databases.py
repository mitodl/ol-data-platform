from metadata.ingestion.source.database.trino.metadata import TrinoSource
from metadata.workflow.ingestion import OpenMetadataWorkflowConfig
from metadata.workflow.metadata import MetadataWorkflow

wf = MetadataWorkflow(config=OpenMetadataWorkflowConfig(source=TrinoSource.create()))
