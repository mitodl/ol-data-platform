from dagster import op


@op
def materialize_dbt_model(context):
    context.resources.dbt.run(models=context.op_config["materialize_dbt_model"])
