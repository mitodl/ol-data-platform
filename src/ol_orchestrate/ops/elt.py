from dagster import Field, In, String, op
from dagster_airbyte import AirbyteOutput


@op(
    config_schema={
        "models_path": Field(
            String,
            is_required=True,
            description="The directory path to the models that you want to materialize",
        )
    },
    ins={"airbyte_output": In(dagster_type=AirbyteOutput)},
    required_resource_keys={"dbt"},
)
def materialize_dbt_model(context, airbyte_output):
    models = context.op_config["models_path"]
    context.resources.dbt.cli(deps)
    context.resources.dbt.run(models=models)
