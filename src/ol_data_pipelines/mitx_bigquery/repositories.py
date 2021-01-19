from dagster import repository

from ol_data_pipelines.mitx_bigquery.solids import mitx_bigquery_pipeline


@repository
def mitx_bigquery_repository():
    return [mitx_bigquery_pipeline]
