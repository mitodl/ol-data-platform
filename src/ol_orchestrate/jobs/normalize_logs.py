from dagster import graph


from ol_orchestrate.ops.normalize_logs import (
    get_log_dates,
    get_log_file_names,
    load_files_to_table,
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
    log_dates = (
        get_log_dates()
    )  # configs: bucket, start_date | args: NONE, | resources: s3,
    # split up work by date
    for date in log_dates:
        # load all files for one date to duckDB
        load_files_to_table(
            log_date=date
        )  # configs: bucket | args: log_date, | resources: s3, duckDB
        transform_log_data()  # configs: NONE | args: NONE, | resources: duckDB
        # get a list of file names for export query
        log_files = get_log_file_names(
            log_date=date
        )  # configs: bucket | args: log_date, | resources: s3,
        # write one outfile per source file
        for file in log_files:
            write_file_to_s3(
                log_file=file
            )  # configs: bucket | args: log_date, log_file | resources: s3, duckDB
