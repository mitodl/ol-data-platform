"""Pipeline for uploading bigquery data to s3."""
import datetime
import types

import pytz
from dagster import (  # noqa: WPS235
    Array,
    AssetMaterialization,
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
    graph,
    op,
)
from google.cloud.exceptions import NotFound
from pyarrow import fs

from ol_data_pipelines.lib.arrow_helper import write_parquet_file
from ol_data_pipelines.lib.dagster_types import DatasetDagsterType
from ol_data_pipelines.lib.glue_helper import convert_schema, create_or_update_table

FIELDS = types.MappingProxyType(
    {
        "user_info_combo": [
            "user_id",
            "username",
            "email",
            "is_staff",
            "profile_name",
            "profile_meta",
            "enrollment_course_id",
            "enrollment_created",
            "profile_country",
        ],
        "person_course": [
            "user_id",
            "registered",
            "explored",
            "certified",
            "mode",
            "roles",
            "cert_created_date",
            "cert_modified_date",
            "course_id",
        ],
    }
)

DEFAULT_LAST_MODIFIED_DAYS = 3600


@op(
    description=("Download bigquery data as parquet files "),
    required_resource_keys={"bigquery_db"},
    config_schema={
        "last_modified_days": Field(
            Int,
            is_required=False,
            default_value=DEFAULT_LAST_MODIFIED_DAYS,
            description=(
                "If set, ignore tables whose modified date is older than"
                " this many days"
            ),
        ),
        "outputs_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
        "table_name": Field(
            String, is_required=True, description="Table that needs to be outputted"
        ),
        "athena_table_name": Field(
            String,
            is_required=False,
            description="Athena table. Ignored if outputs_dir is not an S3 path",
        ),
        "athena_database_name": Field(
            String,
            is_required=False,
            description="Athena database. Ignored if outputs_dir is not an S3 path",
        ),
        "athena_partition_keys": Field(
            Array(str),
            is_required=False,
            default_value=[],
            description=(
                "List of columns to use as partition keys. "
                "Ignored if outputs_dir is not an S3 path"
            ),
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
            name="output_folder",
            dagster_type=String,
            description="Path to user data rendered as parquet config_files",
        )
    ],
)
def export_bigquery_data(  # noqa: WPS210
    context: SolidExecutionContext, datasets: List[DatasetDagsterType]
):
    """Download mitx user data as parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext
    :param datasets: List of bigquery DatasetListItem objects
    :type datasets: List[DatasetDagsterType]

    :yields: A path definition that points to the the folder containing the data
    """
    file_system, output_folder = fs.FileSystem.from_uri(
        context.solid_config["outputs_dir"]
    )

    modified_minimum = datetime.datetime.utcnow().replace(
        tzinfo=pytz.utc
    ) - datetime.timedelta(days=context.solid_config["last_modified_days"])

    table_name = context.solid_config["table_name"]

    fields = FIELDS[table_name]
    first_update = True

    # MITx bigquery data is organized into datasets by course run
    # User data for each run is stored in a table named user_info_combo
    for dataset in datasets:
        full_table_name = "{project}.{dataset_id}.{table_name}".format(
            project=context.resources.bigquery_db.project,
            dataset_id=dataset.dataset_id,
            table_name=table_name,
        )

        try:
            bigquery_table = context.resources.bigquery_db.get_table(full_table_name)
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

            file_name = "{table_name}_{dataset_id}".format(
                table_name=table_name, dataset_id=dataset.dataset_id
            )

            write_parquet_file(file_system, output_folder, arrow_table, file_name)

            athena_schema_update_needed = (
                file_system.type_name == "s3"
                and first_update
                and context.solid_config["athena_table_name"]
                and context.solid_config["athena_database_name"]
            )

            if athena_schema_update_needed:
                formatted_schema = convert_schema(arrow_table.schema)

                create_or_update_table(
                    context.solid_config["athena_database_name"],
                    context.solid_config["athena_table_name"],
                    formatted_schema,
                    context.solid_config["outputs_dir"],
                    context.solid_config["athena_partition_keys"],
                )
                first_update = False

            yield AssetMaterialization(
                description="Updated course file",
                asset_key=dataset.dataset_id,
                metadata_entries=[
                    EventMetadataEntry.text(
                        label="updated_file",
                        description="updated data file",
                        text=file_name,
                    ),
                    EventMetadataEntry.text(
                        label="updated_rows",
                        description="number of records",
                        text=str(rows.total_rows),
                    ),
                ],
            )

    yield Output(output_folder, "output_folder")


@op(
    description="Get list of bigquery dataset pbjects containing user data",
    required_resource_keys={"bigquery_db"},
    output_defs=[
        OutputDefinition(
            name="datasets",
            dagster_type=List[DatasetDagsterType],
            description=(
                "List of of bigquery DatasetListItem objects from mitx database"
            ),
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


@graph(
    description="Extract user data from mitx bigquery database.",
    tags={
        "sources": ["bigquery"],
        "destinations": ["s3"],
        "owner": "ol-engineering",
        "consumer": "ol-engineering",
    },
)
def mitx_bigquery_pipeline():
    """Pipeline for MITX user data extraction."""
    datasets = get_datasets()
    export_user_info_combo = export_bigquery_data.alias("export_user_info_combo")
    export_user_info_combo(datasets)
    export_person_course = export_bigquery_data.alias("export_person_course")
    export_person_course(datasets)
