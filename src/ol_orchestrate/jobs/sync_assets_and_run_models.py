from dagster import graph
from dagster_airbyte import airbyte_sync_op
from dagster_dbt import dbt_run_op
from dagster_dbt import dbt_cli_resource
from ol_orchestrate.ops.elt import materialize_dbt_model


@graph(
    description=("Sync Airbyte connections and run relevant dbt models."),
    tags={
        "source": "airbyte",
        "destination": "s3",
        "owner": "platform-engineering",
        "consumer": "data-analysts",
    },
)
def sync_assets_and_run_models():
    airbyte_sync = airbyte_sync_op.alias(name="sync_airbyte")
    materialize_dbt_model(airbyte_sync())
