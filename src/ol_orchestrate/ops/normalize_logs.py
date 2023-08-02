from pathlib import Path

from dagster import (
    List,
    Nothing,
    op,
    OpExecutionContext,
    Out,
    Output,
    String,
    Config,
)

from dagster.core.definitions.input import In
from pydantic import Field


class LoadFilesConfig(Config):
    tracking_log_bucket: str = Field(
        description="S3 bucket where tracking logs are stored",
    )
    s3_key: str = Field(
        description="S3 key for accessing tracking log bucket",
    )
    s3_secret: str = Field(
        description="S3 secret key for accessing tracking log bucket",
    )
    s3_token: str = Field(
        description="STS token indicating the role assumed for the IAM credentials",
    )


class WriteFilesConfig(Config):
    tracking_log_bucket: str = Field(
        description="S3 bucket where tracking logs are stored",
    )


@op(
    name="load_s3_files_to_duckdb",
    description="Creates a new DuckDB table from all th files in the bucket",
    required_resource_keys={"duckdb"},
    ins={
        "log_date": In(
            dagster_type=String,
            description="Log date prefix to load files from (Format 'YYYY-MM-DD/')}'",
        )
    },
)
def load_files_to_table(
    context: OpExecutionContext, log_date: str, config: LoadFilesConfig
) -> Nothing:
    """Create an in-memory DuckDB table from a newline delimited JSON file.

    The schema is auto-inferred on read and only parses top level JSON. Supports file
    lists, glob patterns, and compressed files. All log files for a single date are
    loaded to a tracking_logs table.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :param config: Dagster execution config for propagaint configuration data.
    :type config: Config

    :log_date: S3 Bucket log date prefix to load logs from (Format 'YYYY-MM-DD/')
    :type log_date: str

    """
    source_bucket = config.tracking_log_bucket
    # DuckDB Glob Syntax: ** matches any number of subdirectories (including none)
    s3_path = f"s3://{source_bucket}/logs/{log_date}**"
    config.log.info(s3_path)
    with context.resources.duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS tracking_logs")
        conn.execute(
            f"""
            INSTALL httpfs;
            INSTALL json;
            LOAD httpfs;
            SET s3_access_key_id="{config.s3_key}";
            SET s3_secret_access_key="{config.s3_secret}";
            """
        )
        if config.s3_token:
            conn.execute(
                f"""
            SET s3_session_token="{config.s3_token}";
            """
            )
        conn.execute(
            f"""
            SET s3_region="us-east-1";
            CREATE TABLE tracking_logs AS
            SELECT * FROM read_ndjson_auto('{s3_path}',
            FILENAME=1, union_by_name=1, maximum_depth=1);
            """  # noqa: S608
        )


@op(
    name="transform_data_in_duckdb",
    description="Executes table updates to normalize data to a consistent format",
    required_resource_keys={"duckdb"},
    ins={"log_db": In(dagster_type=Nothing)},
    out={"columns": Out(dagster_type=List[String])},
)
def transform_log_data(context: OpExecutionContext) -> Nothing:
    """Transform records in the tracking_log table to normalize data.

    All columns are converted to VARCHAR and JSON is extracted to a
    string without escape characters. Integer time values are normalized to ISO8601
    format.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :yield: The list of columns from the log file
    """
    with context.resources.duckdb.get_connection() as conn:
        # update every column to VARCHAR type
        col_names = conn.execute(
            """SELECT column_name FROM temp.information_schema.columns
                    WHERE table_name = 'tracking_logs'
                """
        ).fetchall()
        # exclude filename, which is already a VARCHAR
        columns = [i[0] for i in col_names]
        columns.remove("filename")
        update_stmts = []
        for col in columns:
            conn.execute(f"ALTER TABLE tracking_logs ALTER {col} TYPE VARCHAR")
            update_stmts.append(f"{col} = json_extract_string({col}, '$')")
        # extract VARCHAR strings from json for every field
        update_query = (
            f"UPDATE tracking_logs SET {', '.join(update_stmts)}"  # noqa: S608
        )
        conn.execute(update_query)
        # convert integer timestamps to datetime
        conn.execute(
            """UPDATE tracking_logs
                    SET time = to_timestamp(CAST(time AS BIGINT))
                    WHERE TRY_CAST(time AS BIGINT) IS NOT NULL
                """
        )
        # convert all timestamps to iso8601 format
        conn.execute(
            """UPDATE tracking_logs
                    SET time = strftime(TRY_CAST(time AS TIMESTAMP), '%Y-%m-%d %H:%M:%S.%f')
                """  # noqa: E501
        )
        yield Output(columns, "columns")


@op(
    name="export_processed_data_to_s3",
    description="Exports data from the DuckDB table to a JSON file in S3",
    required_resource_keys={"s3", "duckdb"},
    ins={
        "log_db": In(dagster_type=Nothing),
        "columns": In(dagster_type=List[String]),
        "log_date": In(
            dagster_type=String,
            description="Log date prefix to load files from (Format 'YYYY-MM-DD/')}'",
        ),
    },
)
def write_file_to_s3(
    context: OpExecutionContext,
    columns: List[String],
    log_date: String,
    config: WriteFilesConfig,
) -> None:
    """Export data from the tracking_logs table in DuckDB to the S3 bucket.

    Processed files are written to the 'valid/' directory in the bucket and original
    date prefixes and filenames are preserved. Supports writing compressed files.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :param config: Dagster execution config for propagaint configuration data.
    :type config: Config

    :param columns: List of columns to export from table
    :type columns: List[String]
    """
    with context.resources.duckdb.get_connection() as conn:
        # get filenames from table
        file_names = conn.execute(
            "SELECT DISTINCT filename FROM tracking_logs"
        ).fetchall()
        files = ["".join(i) for i in file_names]
        for file_name in files:
            new_file_name = file_name.replace("logs", "valid")
            local_file_name = new_file_name.rsplit("/", maxsplit=1)[-1]
            config.log.info(new_file_name)
            conn.execute(
                f"""COPY (SELECT {', '.join(columns)} FROM tracking_logs
                        WHERE filename = '{file_name}')
                        TO '{local_file_name}' (FORMAT JSON)
                    """  # noqa: S608
            )
            # copy to S3
            context.resources.s3.upload_file(
                Filename=local_file_name,
                Bucket=config.op_config.tracking_log_bucket,
                Key=f"valid/{log_date}{local_file_name}",
            )
            Path(local_file_name).unlink()
    Path(context.resources.duckdb.database).unlink()
