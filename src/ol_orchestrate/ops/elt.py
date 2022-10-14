from dagster import op


@op
def sync_airbyte_connection():
    ...  # noqa: WPS428


@op
def materialize_dbt_model():
    ...  # noqa: WPS428
