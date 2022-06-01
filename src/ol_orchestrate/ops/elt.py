from dagster import op


@op
def sync_airbyte_connection():
    ...


@op
def materialize_dbt_model():
    ...
