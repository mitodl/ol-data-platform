import os
from datetime import datetime

from dagster import (
    Field,
    List,
    OpExecutionContext,
    Out,
    Output,
    String,
    op,
)

from dagster.core.definitions.input import In


@op(
    name="get_bucket_prefixes_from_s3",
    description="Get S3 bucket prefixes of all log dates since the start date",
    required_resource_keys={"s3"},
    config_schema={
        "tracking_log_bucket": Field(
            String,
            is_required=True,
            description="S3 bucket where tracking logs are stored",
        ),
        "start_date": Field(
            String,
            is_required=True,
            description="First date of tracking logs to process",
        ),
    },
    out={
        "log_date_prefixes": Out(
            dagster_type=List[String],
            description="List of date prefixes ('YYYY-MM-DD/') to iterate over",
        )
    },
)
def get_log_dates(
    context: OpExecutionContext,
) -> List[String]:
    """Get the S3 bucket date prefixes for all log files since the start date.

    :param context: Dagster execution context for propagaint configuration data
    :type context: OpExecutionContext

    :yield: List of log date prefixes (Format 'YYYY-MM-DD/')
    """
    source_bucket = context.op_config["tracking_log_bucket"]
    start_date = context.op_config["start_date"]

    response = context.resources.s3.list_objects(
        Bucket=source_bucket, Prefix="logs/", Delimiter="/"
    )
    log_dates = []
    # isolate log dates from the list of bucket object prefixes
    for log_date in response.get("CommonPrefixes"):
        log_date_time = datetime.strptime(log_date.get("Prefix"), "logs/%Y-%m-%d/")
        # filter out logs before start_date
        if start_date <= log_date_time:
            log_dates.append(log_date.get("Prefix").split("/", maxsplit=1)[-1])
    yield Output(log_dates, "log_date_prefixes")


@op(
    name="get_file_names_from_s3",
    description="Get file names of all logs in a single dated S3 bucket",
    required_resource_keys={"s3"},
    config_schema={
        "tracking_log_bucket": Field(
            String,
            is_required=True,
            description="S3 bucket where tracking logs are stored",
        ),
    },
    ins={
        "log_date": In(
            dagster_type=List[String],
            description="S3 Bucket date prefix ('YYYY-MM-DD/') to check for log files",
        )
    },
    out={
        "log_file_names": Out(
            dagster_type=List[String],
            description="List of log files that will be normalized",
        )
    },
)
def get_log_file_names(context: OpExecutionContext, log_date: String) -> List[String]:
    """List the log file names in the bucket with the date prefix.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

        :log_date: S3 Bucket Object Prefix to check for log files (Format 'YYYY-MM-DD/').
        :type log_date: str

    :yield: List of tracking log files (Format '{fileName}.log.gz').
    """
    source_bucket = context.op_config["tracking_log_bucket"]
    response = context.resources.s3.list_objects_v2(
        Bucket=source_bucket, Prefix=f"logs/{log_date}"
    )
    log_files = []
    for item in response.get("Contents", []):
        log_files.append(item["Key"].split("/", maxsplit=2)[-1])
    yield Output(log_files, "log_file_names")


@op(
    name="load_s3_files_to_duckdb",
    description="Creates a new DuckDB table from all th files in the bucket",
    required_resource_keys={"s3", "duckDB"},
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
    conn.execute("DROP TABLE IF EXISTS tracking_logs")
    conn.execute(
        f"""
        CREATE TABLE tracking_logs AS
        SELECT * FROM read_ndjson_auto('{s3_path}',
        FILENAME=1, union_by_name=1, maximum_depth=1)
    """
    )


@op(
    name="transform_data_in_duckdb",
    description="Executes table updates to normalize data to a consistent format",
    required_resource_keys={"duckDB"},
)
def transform_log_data(context: OpExecutionContext) -> None:
    """Transforms records in the tracking_log table to normalize data.
        context, event, and time columns are converted to VARCHAR and JSON
        is extracted to a string without escape characters. Integer time
        values are normalized to ISO8601 format.

    :param context: Dagster execution context for propagaint configuration data.
    :type context: OpExecutionContext

    :log_date: S3 Bucket log date prefix to load logs from (Format 'YYYY-MM-DD/**')
    :type log_date: str
    """
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
    		"""
    )


@op(
    name="export_processed_data_to_s3",
    description="Exports data from the DuckDB table to a JSON file in S3",
    required_resource_keys={"s3", "duckDB"},
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
        ),
        "log_file": In(
            dagster_type=String,
            description="Tracking log file name (Format '{fileName}.log.gz')",
        ),
    },
)
def write_file_to_s3(
    context: OpExecutionContext,
    log_date: String,
    log_file: String,
) -> None:
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
    source_s3_file_path = f"s3://{bucket}/logs/{log_date}{log_file}"
    dest_s3_file_path = f"s3://{bucket}/valid/{log_date}{log_file}"
    # query the table to select records by source s3 URI
    export_query_string = f"""
    	COPY (SELECT * FROM tracking_logs WHERE
    	filename = '{source_s3_file_path}') TO '{dest_s3_file_path}' (FORMAT JSON)
    """
    # create s3 bucket objects if they don't exist
    try:
        # isolate directories above the file
        dir_path = "/".join(dest_s3_file_path.split("/")[0:-1])
        exists = os.path.exists(dir_path)
        if not exists:
            os.makedirs(dir_path, exist_ok=True)
    except FileExistsError:
        # directory already exists
        pass
    conn.execute(export_query_string)
