"""Pipeline for uploading bigquery data to s3."""
import datetime

import pytz
from dagster import (  # noqa: WPS235
    AssetMaterialization,
    Bool,
    EventMetadataEntry,
    Field,
    InputDefinition,
    Int,
    List,
    ModeDefinition,
    Output,
    OutputDefinition,
    PresetDefinition,
    SolidExecutionContext,
    String,
    pipeline,
    solid,
)
from google.cloud.exceptions import NotFound
from pyarrow import fs

from ol_data_pipelines.lib.arrow_helper import write_parquet_file
from ol_data_pipelines.lib.dagster_types import DatasetDagsterType
from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config
from ol_data_pipelines.resources.bigquery_db import bigquery_db_resource


@solid(
    description=("Download mitx user data as parquet files "),
    required_resource_keys={"bigquery_db"},
    config_schema={
        "last_modified_days": Field(
            Int,
            is_required=False,
            default_value=3600,
            description="If set, ignore tables whose modified date is older than this many days",
        ),
        "outputs_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
    },
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
            dagster_type=String,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def download_user_data(  # noqa: WPS210
    context: SolidExecutionContext, datasets: List[DatasetDagsterType]
):
    """Download mitx user data as parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext
    :param datasets: List of bigquery DatasetListItem objects
    :type datasets: List[DatasetDagsterType]
    :yield: A path definition that points to the the folder containing the user data
    """
    file_system, output_folder = fs.FileSystem.from_uri(
        context.solid_config["outputs_dir"]
    )

    modified_minimum = datetime.datetime.utcnow().replace(
        tzinfo=pytz.utc
    ) - datetime.timedelta(days=context.solid_config["last_modified_days"])

    fields = [
        "user_id",
        "username",
        "email",
        "is_staff",
        "profile_name",
        "profile_meta",
        "enrollment_course_id",
        "enrollment_created",
    ]

    # MITx bigquery data is organized into datasets by course run
    # User data for each run is stored in a table named user_info_combo
    for dataset in datasets:
        table_name = "{project}.{dataset_id}.user_info_combo".format(
            project=context.resources.bigquery_db.project, dataset_id=dataset.dataset_id
        )

        try:
            bigquery_table = context.resources.bigquery_db.get_table(table_name)
        except NotFound:
            continue

        desired_schema = [
            column for column in bigquery_table.schema if column.name in fields
        ]

        if bigquery_table.modified > modified_minimum:
            rows = context.resources.bigquery_db.list_rows(
                bigquery_table, selected_fields=desired_schema
            )
            arrow_table = rows.to_arrow()

            file_name = "user_info_combo_{dataset_id}".format(
                dataset_id=dataset.dataset_id
            )

            write_parquet_file(file_system, output_folder, arrow_table, file_name)

            yield AssetMaterialization(
                description="Updated course file",
                asset_key=dataset.dataset_id,
                metadata_entries=[
                    EventMetadataEntry.text(
                        label="updated_file",
                        description="updated course file",
                        text=file_name,
                    ),
                    EventMetadataEntry.text(
                        label="updated_rows",
                        description="number of records",
                        text=str(rows.total_rows),
                    ),
                ],
            )

    yield Output(output_folder, "user_query_folder")


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
    """Get list of bigquery dataset objects containing user data.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext
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
            resource_defs={"bigquery_db": bigquery_db_resource},
        )
    ],
    preset_defs=[
        PresetDefinition(
            name="production",
            run_config=load_yaml_config("/etc/dagster/mitx_bigquery.yaml"),
            mode="production",
            tags={
                "sources": ["bigquery"],
                "destinations": ["s3"],
                "owner": "ol-engineering",
                "consumer": "ol-engineering",
            },
        )
    ],
)
def mitx_bigquery_pipeline():
    """Pipeline for MITX user data extraction."""
    download_user_data(get_datasets())
