import httpx
from dagster import Array, OpExecutionContext, Shape, job, op


@op(
    name="create_superset_datasets",
    config_schema={
        "models": Array(
            Shape(
                {
                    "schema_suffix": str,
                    "table": str,
                }
            )
        )
    },
    required_resource_keys={"vault", "superset_api"},
)
def create_superset_datasets(context: OpExecutionContext):
    context.log.info("Publishing Superset create_superset_datasets")
    models = context.op_config["models"]
    for model in models:
        schema_suffix = model["schema_suffix"]
        table = model["table"]
        try:
            context.log.info("Creating Superset dataset: %s.%s", schema_suffix, table)
            context.resources.superset_api.client.create_dataset(
                schema_suffix=schema_suffix,
                table_name=table,
            )
            context.log.info(
                "Successfully created dataset: %s.%s", schema_suffix, table
            )

        except httpx.HTTPStatusError:
            context.log.exception(
                "HTTPStatusError while creating dataset for %s.%s",
                schema_suffix,
                table,
            )


@job(
    name="superset_sync_job",
    description="Syncs dbt models to Superset as datasets",
)
def superset_sync_job():
    create_superset_datasets()
