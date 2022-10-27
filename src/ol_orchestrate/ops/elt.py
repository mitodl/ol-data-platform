from dagster import Field, In, String, op
from dagster_airbyte import AirbyteOutput


@op(
    ins={"airbyte_output": In(dagster_type=AirbyteOutput)},
    config_schema={
        "models_path": Field(
            String,
            is_required=True,
            description="The directory path to the models that you want to materialize",
        )
    },
)
def materialize_dbt_model(context, airbyte_output):
    models = context.op_config["models_path"]
    context.resources.dbt.run(models=models)
