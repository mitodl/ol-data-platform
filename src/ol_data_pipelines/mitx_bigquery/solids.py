import json
from google.cloud.exceptions import NotFound
from dagster import (
    AssetMaterialization,
    EventMetadataEntry,
    ModeDefinition,
    Output,
    PresetDefinition,
    SolidExecutionContext,
    pipeline,
    solid,
    List,
    InputDefinition,
    OutputDefinition,
)
from ol_data_pipelines.resources.bigquery_db import bigquery_db_resource
from ol_data_pipelines.resources.outputs import daily_dir
from ol_data_pipelines.lib.dagster_types import DagsterPath, DatasetDagsterType


@solid(
    description=("Download mitx user data as parquet files "),
    required_resource_keys={"bigquery_db", "results_dir"},
    input_defs=[
        InputDefinition(
            name="datasets",
            dagster_type=List[DatasetDagsterType],
            description="List of datasets in mitx bigquery database",
        )
    ],
    output_defs=[
        OutputDefinition(
            name="user_query_folder",
            dagster_type=DagsterPath,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def download_user_data(
    context: SolidExecutionContext, datasets: List[DatasetDagsterType]
):
    """
    Download mitx user data as parquet files

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :param datasets: List of bigquery DatasetListItem objects
    :type edx_course_ids: List[DatasetDagsterType]

    :yield: A path definition that points to the the folder containing the user data
    """
    for dataset in datasets:
        table = (
            context.resources.bigquery_db.project
            + "."
            + dataset.dataset_id
            + ".user_info_combo"
        )

        user_query_folder = context.resources.results_dir.path
        try:
            rows = context.resources.bigquery_db.list_rows(table)
            dataframe = rows.to_dataframe()
            file = (
                str(user_query_folder)
                + "/user_info_combo_"
                + dataset.dataset_id
                + ".parquet"
            )
            dataframe.to_parquet(file)

        except NotFound:
            pass

    yield AssetMaterialization(
        asset_key="user_query",
        description="User data from mitx bigquery database",
        metadata_entries=[
            EventMetadataEntry.path(str(user_query_folder), "user_query_folder"),
        ],
    )
    yield Output(user_query_folder, "user_query_folder")


@solid(
    description="Get list of bigquery dataset pbjects containing user data",
    required_resource_keys={"bigquery_db"},
    output_defs=[
        OutputDefinition(
            name="datasets",
            dagster_type=List[DatasetDagsterType],
            description="List of of bigquery DatasetListItem objects from mitx database",
        )
    ],
)
def get_datasets(context: SolidExecutionContext):
    """
    Get list of bigquery dataset objects containing user data

    :return: List of bigquery DatasetListItem objects
    """

    datasets = context.resources.bigquery_db.list_datasets()
    return list(
        filter(lambda dataset: dataset.dataset_id.endswith("_latest"), datasets)
    )


@pipeline(
    description="Extract user data from mitx bigquery database.",
    mode_defs=[
        ModeDefinition(
            name="production",
            resource_defs={
                "bigquery_db": bigquery_db_resource,
                "results_dir": daily_dir,
            },
        )
    ],
    preset_defs=[
        PresetDefinition.from_files(
            name="production",
            config_files=["/etc/dagster/mitx_bigquery.yaml"],
            mode="production",
        ),
    ],
    tags={"source": "mitx", "destination": "s3", "owner": "platform-engineering"},
)
def mitx_bigquery_pipeline():
    """
    Pipeline for MITX user data extraction
    """
    download_user_data(get_datasets())
