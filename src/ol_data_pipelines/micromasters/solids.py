from os import path

from dagster import (  # noqa: WPS235
    Field,
    Int,
    ModeDefinition,
    Output,
    OutputDefinition,
    PresetDefinition,
    SolidExecutionContext,
    String,
    pipeline,
    solid,
)
from pyarrow import fs
from pypika import PostgreSQLQuery, Table

from ol_data_pipelines.lib.arrow_helper import stream_to_parquet_file
from ol_data_pipelines.lib.yaml_config_helper import load_yaml_config
from ol_data_pipelines.resources.postgres_db import (
    DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
    postgres_db_resource,
)


@solid(
    description=("Query postgres and data as parquet files"),
    required_resource_keys={"postgres_db"},
    config_schema={
        "chunksize": Field(
            Int,
            is_required=False,
            default_value=DEFAULT_POSTGRES_QUERY_CHUNKSIZE,
            description="Number of rows per parquet file",
        ),
        "outputs_base_dir": Field(
            String, is_required=True, description="Path for output files"
        ),
        "folder_prefix": Field(
            String,
            is_required=False,
            default_value="micromasters",
            description="prefix",
        ),
    },
    output_defs=[
        OutputDefinition(
            name="auth_user_folder",
            dagster_type=String,
            description="Path to auth_user data rendered as parquet files",
        ),
        OutputDefinition(
            name="courses_course_folder",
            dagster_type=String,
            description="Path to courses_course data rendered as parquet files",
        ),
        OutputDefinition(
            name="courses_electivecourse_folder",
            dagster_type=String,
            description="Path to courses_electivecourse data rendered as parquet files",
        ),
        OutputDefinition(
            name="courses_electivesset_folder",
            dagster_type=String,
            description="Path to courses_electivesset data rendered as parquet files",
        ),
        OutputDefinition(
            name="courses_program_folder",
            dagster_type=String,
            description="Path to courses_program data rendered as parquet files",
        ),
        OutputDefinition(
            name="ecommerce_line_folder",
            dagster_type=String,
            description="Path to ecommerce line data rendered as parquet files",
        ),
        OutputDefinition(
            name="ecommerce_order_folder",
            dagster_type=String,
            description="Path to ecommerce order data rendered as parquet files",
        ),
        OutputDefinition(
            name="grades_micromasterscoursecertificate_folder",
            dagster_type=String,
            description="Path to grades micromasters certificate data rendered as parquet files",
        ),
        OutputDefinition(
            name="profiles_profile_folder",
            dagster_type=String,
            description="Path to profiles_profile data rendered as parquet files",
        ),
    ],
)
def fetch_micromasters_tables(context: SolidExecutionContext):
    """
    Fetch micromasters data and store it in parquet files.

    :param context: Dagster execution context for configuration data
    :type context: SolidExecutionContext

    :yields: A path definitions that points to the the folders containing the data
    """
    table_names = [
        "auth_user",
        "courses_course",
        "courses_electivecourse",
        "courses_electivesset",
        "courses_program",
        "ecommerce_line",
        "ecommerce_order",
        "grades_micromasterscoursecertificate",
        "profiles_profile",
    ]

    for table_name in table_names:
        table = Table(table_name)

        if table_name == "profiles_profile":
            # Pyarrow has trouble parsing the type for edx_language_proficiencies
            # in profiles_profile. Only pulling the columns we need for micromasters
            # reporting to avoid the issue for now
            query = PostgreSQLQuery.from_(table).select(
                "id",
                "address",
                "birth_country",
                "city",
                "country",
                "date_of_birth",
                "gender",
                "edx_name",
                "filled_out",
                "first_name",
                "last_name",
                "nationality",
                "postal_code",
                "romanized_first_name",
                "romanized_last_name",
                "state_or_territory",
                "user_id",
            )
        else:
            query = PostgreSQLQuery.from_(table).select(table.star)

        outputs_folder = f'{context.solid_config["folder_prefix"]}_{table_name}'
        file_system, output_folder = fs.FileSystem.from_uri(
            path.join(context.solid_config["outputs_base_dir"], outputs_folder)
        )

        stream_to_parquet_file(
            context.resources.postgres_db.run_chunked_query(
                query, context.solid_config["chunksize"]
            ),
            table_name,
            file_system,
            output_folder,
        )

        yield Output(output_folder, f"{table_name}_folder")


@pipeline(
    description="Retrieve data from micromasters write it as parquet files",
    mode_defs=[
        ModeDefinition(
            name="production",
            resource_defs={"postgres_db": postgres_db_resource},
        )
    ],
    preset_defs=[
        PresetDefinition(
            name="production",
            run_config=load_yaml_config("/etc/dagster/micromasters.yaml"),
            mode="production",
        )
    ],
    tags={
        "source": "micromasters",
        "destination": "s3",
        "owner": "platform-engineering",
    },
)
def pull_micromasters_data_pipeline():
    """Pipeline for micromasters database data to s3."""
    fetch_micromasters_tables()
