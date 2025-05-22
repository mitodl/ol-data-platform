from dagster import (
    RunRequest,
    SensorEvaluationContext,
    SensorResult,
    SkipReason,
    sensor,
)

from ol_orchestrate.jobs.superset_api import superset_sync_job
from ol_orchestrate.resources.superset_api import SupersetApiClientFactory


@sensor(
    name="sync_superset_datasets_from_dbt",
    job=superset_sync_job,
    minimum_interval_seconds=60 * 60 * 24,  # daily
)
def sync_superset_datasets_from_dbt(
    context: SensorEvaluationContext, superset_api: SupersetApiClientFactory
):
    all_assets = context.instance.get_asset_keys()
    # Filter only those from the "mart" or "reporting" groups based on naming
    dbt_models = [
        {"schema_suffix": asset.path[0], "table": asset.path[1]}
        for asset in all_assets
        if asset.path[0] in ["mart", "reporting"]
    ]
    # Get dataset names from Superset API
    superset_datasets = superset_api.client.get_dataset_list()
    existing_superset_datasets: list[str] = []

    existing_superset_datasets.extend(
        dataset["table_name"]
        for page_data in superset_datasets
        for dataset in page_data
    )

    datasets_to_create = [
        model
        for model in dbt_models
        if model["table"] not in existing_superset_datasets
    ]

    context.log.info("Datasets to create: %s", datasets_to_create)

    if not datasets_to_create:
        return SkipReason("No datasets need to be created on Superset")

    return SensorResult(
        run_requests=[
            RunRequest(
                run_key=None,
                run_config={
                    "ops": {
                        "create_superset_datasets": {
                            "config": {"models": datasets_to_create}
                        }
                    }
                },
            )
        ]
    )
