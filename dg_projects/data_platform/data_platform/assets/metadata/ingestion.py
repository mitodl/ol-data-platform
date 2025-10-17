"""OpenMetadata ingestion workflows for various data sources.

This module provides Dagster assets that run OpenMetadata workflows to ingest
metadata, lineage, and profiling information from various data sources.
"""

from typing import Any

from dagster import AssetExecutionContext, Output, asset
from metadata.workflow.metadata import MetadataWorkflow
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from pydantic import BaseModel, Field

from data_platform.resources.openmetadata import OpenMetadataClient


class WorkflowConfig(BaseModel):
    """Base configuration for OpenMetadata workflows."""

    source_config: dict[str, Any] = Field(
        description="Source configuration for the workflow"
    )
    sink_config: dict[str, Any] = Field(
        default_factory=lambda: {"type": "metadata-rest"},
        description="Sink configuration for the workflow",
    )
    workflow_config: dict[str, Any] = Field(
        default_factory=lambda: {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
        description="Workflow configuration",
    )


def run_metadata_workflow(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
    workflow_config: dict[str, Any],
) -> Output:
    """Run an OpenMetadata ingestion workflow.

    Args:
        context: Dagster execution context
        openmetadata_client: OpenMetadata client resource
        workflow_config: Configuration for the workflow

    Returns:
        Output with workflow status and metadata
    """
    # Update workflow config with OpenMetadata connection
    workflow_config["workflowConfig"]["openMetadataServerConfig"] = {
        "hostPort": openmetadata_client.base_url,
        "authProvider": "openmetadata",
        "securityConfig": workflow_config["workflowConfig"].get(
            "openMetadataServerConfig", {}
        ).get("securityConfig", {}),
    }

    try:
        # Create and run the workflow
        workflow = MetadataWorkflow.create(workflow_config)
        workflow.execute()
        workflow.raise_from_status()

        status = workflow.get_status()

        context.log.info(
            "Workflow completed successfully. "
            "Records: %s, "
            "Warnings: %s, "
            "Errors: %s",
            status.records,
            status.warnings.failures if status.warnings else 0,
            status.failures.failures if status.failures else 0,
        )

        return Output(
            value=status,
            metadata={
                "records": status.records,
                "warnings": status.warnings.failures if status.warnings else 0,
                "errors": status.failures.failures if status.failures else 0,
                "success": True,
            },
        )
    except Exception:
        context.log.exception("Workflow failed")
        return Output(
            value=None,
            metadata={
                "success": False,
                "error": "Workflow execution failed",
            },
        )
    finally:
        workflow.stop()


@asset(
    key=["openmetadata", "trino", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def trino_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Trino (Starburst Galaxy).

    This asset ingests table schemas, column information, and database structure
    from Trino into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "trino",
            "serviceName": "starburst_galaxy",
            "serviceConnection": {
                "config": {
                    "type": "Trino",
                    "hostPort": (
                        "ol-data-platform-cluster.starburstdata.net:443"
                    ),
                    "catalog": "ol_warehouse",
                    "databaseSchema": "ol_warehouse_qa_staging",
                    "connectionOptions": {},
                    "supportsMetadataExtraction": True,
                    "supportsProfiler": True,
                    "supportsQueryComment": True,
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "schemaFilterPattern": {
                        "includes": ["ol_warehouse_.*"],
                    },
                    "includeViews": True,
                    "includeTables": True,
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "trino", "lineage"],
    group_name="openmetadata",
    deps=["openmetadata__trino__metadata"],
    automation_condition=upstream_or_code_changes(),
)
def trino_lineage(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest lineage information from Trino query logs.

    This asset analyzes Trino query logs to extract data lineage information
    showing how tables are derived from other tables.
    """
    workflow_config = {
        "source": {
            "type": "trino",
            "serviceName": "starburst_galaxy",
            "serviceConnection": {
                "config": {
                    "type": "Trino",
                    "hostPort": (
                        "ol-data-platform-cluster.starburstdata.net:443"
                    ),
                    "catalog": "ol_warehouse",
                    "supportsLineageExtraction": True,
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseLineage",
                    "queryLogDuration": 7,  # days
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "dbt", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def dbt_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from dbt models.

    This asset ingests dbt model definitions, documentation, and tests
    into OpenMetadata, creating a comprehensive view of transformed data.
    """
    workflow_config = {
        "source": {
            "type": "dbt",
            "serviceName": "dbt_ol_warehouse",
            "sourceConfig": {
                "config": {
                    "type": "DBT",
                    "dbtConfigSource": {
                        "dbtCatalogFilePath": "/path/to/dbt/target/catalog.json",
                        "dbtManifestFilePath": "/path/to/dbt/target/manifest.json",
                        "dbtRunResultsFilePath": "/path/to/dbt/target/run_results.json",
                    },
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "dagster", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def dagster_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Dagster pipelines.

    This asset ingests Dagster pipeline definitions, assets, and jobs
    into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "dagster",
            "serviceName": "dagster_pipelines",
            "serviceConnection": {
                "config": {
                    "type": "Dagster",
                    "host": "pipelines.odl.mit.edu",
                    "port": 443,
                }
            },
            "sourceConfig": {"config": {"type": "PipelineMetadata"}},
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "superset", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def superset_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Apache Superset.

    This asset ingests Superset dashboard, chart, and dataset definitions
    into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "superset",
            "serviceName": "superset_analytics",
            "serviceConnection": {
                "config": {
                    "type": "Superset",
                    "hostPort": "https://superset.odl.mit.edu",
                    "connection": {
                        "provider": "db",
                    },
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DashboardMetadata",
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "airbyte", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def airbyte_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Airbyte connections.

    This asset ingests Airbyte connection and sync information into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "airbyte",
            "serviceName": "airbyte_elt",
            "serviceConnection": {
                "config": {
                    "type": "Airbyte",
                    "hostPort": "http://airbyte:8001",
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "PipelineMetadata",
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "s3", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def s3_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from S3 buckets.

    This asset ingests S3 bucket and object structure information into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "s3",
            "serviceName": "s3_datalake",
            "serviceConnection": {
                "config": {
                    "type": "S3",
                    "awsConfig": {
                        "awsRegion": "us-east-1",
                    },
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "StorageMetadata",
                    "containerFilterPattern": {
                        "includes": ["ol-data-platform-.*"],
                    },
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "iceberg", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def iceberg_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Iceberg tables.

    This asset ingests Apache Iceberg table metadata including schemas,
    partitioning, and table history.
    """
    workflow_config = {
        "source": {
            "type": "iceberg",
            "serviceName": "iceberg_catalog",
            "serviceConnection": {
                "config": {
                    "type": "Iceberg",
                    "catalog": {
                        "name": "ol_warehouse",
                        "connection": {
                            "type": "Glue",
                            "awsConfig": {
                                "awsRegion": "us-east-1",
                            },
                        },
                    },
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DatabaseMetadata",
                    "schemaFilterPattern": {
                        "includes": ["ol_warehouse_.*"],
                    },
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "iceberg", "profiling"],
    group_name="openmetadata",
    deps=["openmetadata__iceberg__metadata"],
    automation_condition=upstream_or_code_changes(),
)
def iceberg_profiling(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Run data profiling on Iceberg tables.

    This asset runs statistical profiling on Iceberg tables to gather
    data quality metrics and column statistics.
    """
    workflow_config = {
        "source": {
            "type": "iceberg",
            "serviceName": "iceberg_catalog",
            "serviceConnection": {
                "config": {
                    "type": "Iceberg",
                    "catalog": {
                        "name": "ol_warehouse",
                        "connection": {
                            "type": "Glue",
                            "awsConfig": {
                                "awsRegion": "us-east-1",
                            },
                        },
                    },
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "Profiler",
                    "schemaFilterPattern": {
                        "includes": ["ol_warehouse_.*"],
                    },
                }
            },
        },
        "processor": {
            "type": "orm-profiler",
            "config": {},
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)


@asset(
    key=["openmetadata", "redash", "metadata"],
    group_name="openmetadata",
    automation_condition=upstream_or_code_changes(),
)
def redash_metadata(
    context: AssetExecutionContext,
    openmetadata_client: OpenMetadataClient,
) -> Output:
    """Ingest metadata from Redash.

    This asset ingests Redash query and dashboard definitions into OpenMetadata.
    """
    workflow_config = {
        "source": {
            "type": "redash",
            "serviceName": "redash_analytics",
            "serviceConnection": {
                "config": {
                    "type": "Redash",
                    "hostPort": "https://redash.odl.mit.edu",
                }
            },
            "sourceConfig": {
                "config": {
                    "type": "DashboardMetadata",
                }
            },
        },
        "sink": {"type": "metadata-rest"},
        "workflowConfig": {
            "loggerLevel": "INFO",
            "openMetadataServerConfig": {},
        },
    }

    return run_metadata_workflow(context, openmetadata_client, workflow_config)
