"""Repository for pipeline pulling MITx bigquery data to S3."""
from dagster import repository

from ol_data_pipelines.mitx_bigquery.schedule import mitx_bigquery_daily_schedule
from ol_data_pipelines.mitx_bigquery.solids import mitx_bigquery_pipeline


@repository
def mitx_bigquery_repository():
    """Repository for mitx bigquery pipeline.

    :returns: open data pipelines and schedules
    """
    return [mitx_bigquery_pipeline, mitx_bigquery_daily_schedule]
