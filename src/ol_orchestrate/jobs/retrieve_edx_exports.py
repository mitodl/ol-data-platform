from dagster import graph

from ol_orchestrate.ops.normalize_logs import (
    load_files_to_table,
    transform_log_data,
    write_file_to_s3,
)
from ol_orchestrate.ops.retrieve_edx_exports import (
    download_edx_data,
    extract_course_files,
    upload_files,
)


@graph(
    description=(
        "Retrieve and extract edX.org course exports and csvs maintained by"
        "institutional research from a gcs bucket into S3 buckets on a weekly basis."
    ),
    tags={
        "source": "gcs",
        "destination": "s3",
        "owner": "institutional-research",
        "consumer": "platform-engineering",
    },
)
def retrieve_edx_course_exports():
    upload_files(extract_course_files(download_edx_data()))


@graph(
    description=(
        "Retrieve and extract edX.org tracking logs maintained by"
        "institutional research from a gcs bucket into S3 buckets on a weekly basis."
    ),
    tags={
        "source": "gcs",
        "destination": "s3",
        "owner": "institutional-research",
        "consumer": "platform-engineering",
    },
)
def retrieve_edx_tracking_logs():
    file_list = upload_files(download_edx_data())
    file_list.map(
        lambda file_date: write_file_to_s3(
            transform_log_data(load_files_to_table(file_date))
        )
    )
