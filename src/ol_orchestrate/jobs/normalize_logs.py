from dagster import graph


from ol_orchestrate.ops.normalize_logs import (
    load_files_to_table,
    jsonify_log_data,
    transform_log_data,
    write_file_to_s3,
)


@graph(
    description=(
        "Normalize historical tracking log data stored in an S3 bucket"
        "by loading them to DuckDB, transforming the data into a consistent"
        "format, and uploading them to the 'valid/' path prefix in the same"
        "bucket on an ad hoc basis."
    ),
    tags={
        "source": "s3",
        "destination": "s3",
        "owner": "platform-engineering",
        "consumer": "platform-engineering",
    },
)
def normalize_tracking_logs():
    # load all files for one date to duckDB
    write_file_to_s3(transform_log_data(load_files_to_table()))


@graph(
    description=(
        "Normalize historical tracking log data stored in an S3 bucket"
        "by loading them to DuckDB, transforming the data into a consistent"
        "format, and uploading them to the 'logs/' path prefix in the same"
        "bucket on an ad hoc basis."
    ),
    tags={
        "source": "s3",
        "destination": "s3",
        "owner": "platform-engineering",
        "consumer": "IRx",
    },
)
def jsonify_tracking_logs():
    # load all files for one date to duckDB
    write_file_to_s3(jsonify_log_data(load_files_to_table()))
