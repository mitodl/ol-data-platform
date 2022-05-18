"""Repository for pipeline pulling MITx bigquery data to S3."""
from dagster import repository

from ol_orchestrate.lib.yaml_config_helper import load_yaml_config
from ol_orchestrate.mitx_bigquery.schedule import mitx_bigquery_daily_schedule
from ol_orchestrate.mitx_bigquery.solids import mitx_bigquery_pipeline
from ol_orchestrate.resources.bigquery_db import bigquery_db_resource


@repository
def mitx_bigquery_repository():
    """Repository for mitx bigquery pipeline.

    :returns: open data pipelines and schedules
    """
    return [
        mitx_bigquery_pipeline.to_job(
            resource_defs={"bigquery_db": bigquery_db_resource},
            config=load_yaml_config("/etc/dagster/mitx_bigquery.yaml"),
        ),
        mitx_bigquery_daily_schedule,
    ]
