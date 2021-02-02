"""Repository for pipeline pulling MITx bigquery data to S3"""
from dagster import repository

from ol_data_pipelines.mitx_bigquery.solids import mitx_bigquery_pipeline


@repository
def mitx_bigquery_repository():
    """repository for mitx bigquery pipeline"""
    return [mitx_bigquery_pipeline]
