import os

from dagster import (
    Field,
    OpExecutionContext,
    Out,
    String,
    op,
)

from dagster.core.definitions.input import In


@op(
    name="load_s3_files_to_duckdb",
    description="Creates a new DuckDB table from all th files in the bucket",
    required_resource_keys={"s3", "duckdb"},
    config_schema={
        "tracking_log_bucket": Field(
            String,
            is_required=True,
            description="S3 bucket where tracking logs are stored",
        ),
    },
    ins={
        "log_date": In(
            dagster_type=String,
            description="Log date prefix to load files from (Format 'YYYY-MM-DD/')}'",
        )
    },
    out=Out(io_manager_key="duckdb_io"),
)
def load_files_to_table(context: OpExecutionContext, log_date: String) -> None:
    """Creates an in-memory DuckDB table from a newline delimited JSON file,
    auto-inferring schema on read and only parsing top level JSON.
    Supports file lists, glob patterns, and compressed files. All log files for
    a single date are loaded to a tracking_logs table.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :log_date: S3 Bucket log date prefix to load logs from (Format 'YYYY-MM-DD/')
    :type log_date: str
    """
    source_bucket = context.op_config["tracking_log_bucket"]
    # DuckDB Glob Syntax: ** matches any number of subdirectories (including none)
    s3_path = f"s3://{source_bucket}/logs/{log_date}**"
    with context.resources.duckdb.get_connection() as conn:
        conn.execute("DROP TABLE IF EXISTS tracking_logs")
        conn.execute(
            f"""
            CREATE TABLE tracking_logs AS
            SELECT * FROM read_ndjson_auto('{s3_path}',
            FILENAME=1, union_by_name=1, maximum_depth=1)
            """  # noqa: S608
        )


@op(
    name="transform_data_in_duckdb",
    description="Executes table updates to normalize data to a consistent format",
    required_resource_keys={"duckdb"},
    ins={"log_db": In(input_manager_key="duckdb_io")},
    out=Out(io_manager_key="duckdb_io"),
)
def transform_log_data(context: OpExecutionContext, log_db) -> None:
    """Transforms records in the tracking_log table to normalize data.
        context, event, and time columns are converted to VARCHAR and JSON
        is extracted to a string without escape characters. Integer time
        values are normalized to ISO8601 format.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :log_date: S3 Bucket log date prefix to load logs from (Format 'YYYY-MM-DD/**')
    :type log_date: str
    """
    with context.resources.duckdb.get_connection() as conn:
        # explicity convert event, context, and time fields to VARCHAR
        conn.execute("ALTER TABLE tracking_logs ALTER context TYPE VARCHAR")
        conn.execute("ALTER TABLE tracking_logs ALTER event TYPE VARCHAR")
        conn.execute("ALTER TABLE tracking_logs ALTER time TYPE VARCHAR")
        conn.execute(
            """UPDATE tracking_logs SET
                    context = json_extract_string(context, '$'),
                    event = json_extract_string(event, '$'),
                    time = json_extract_string(time, '$')
                """
        )
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


@op(
    name="export_processed_data_to_s3",
    description="Exports data from the DuckDB table to a JSON file in S3",
    required_resource_keys={"s3", "duckdb"},
    config_schema={
        "tracking_log_bucket": Field(
            String,
            is_required=True,
            description="S3 bucket where tracking logs are stored",
        ),
        "log_date": Field(
            String,
            is_required=True,
        ),
    },
    ins={"log_db": In(input_manager_key="duckdb_io")},
)
def write_file_to_s3(context: OpExecutionContext, log_date, log_db) -> None:
    """Exports data from the tracking_logs table in DuckDB to the S3 bucket.
    Processed files are written to the 'valid/' directory in the bucket and
    original date prefixes and filenames are preserved. Supports writing
    compressed files.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :log_date: S3 Bucket log date prefix to export logs to (Format 'YYYY-MM-DD/')
    :type log_date: str

    :log_file: Tracking log file name (Format '{fileName}.log.gz')
    :type log_file: str
    """
    bucket = context.op_config["tracking_log_bucket"]
    source_s3_file_path = f"s3://{bucket}/logs/{log_date}{log_file}"  # noqa: F821
    dest_s3_file_path = f"s3://{bucket}/valid/{log_date}{log_file}"  # noqa: F821
    # query the table to select records by source s3 URI
    export_query_string = f"""
    	COPY (SELECT * FROM tracking_logs WHERE
    	filename = '{source_s3_file_path}') TO '{dest_s3_file_path}' (FORMAT JSON)
    """  # noqa: S608, E101
    # create s3 bucket objects if they don't exist
    try:
        # isolate directories above the file
        dir_path = "/".join(dest_s3_file_path.split("/")[0:-1])
        exists = os.path.exists(dir_path)  # noqa: PTH110
        if not exists:
            os.makedirs(dir_path, exist_ok=True)  # noqa: PTH103
    except FileExistsError:
        # directory already exists
        pass
    with context.resources.duckdb.get_connection() as conn:
        conn.execute(export_query_string)
