from dagster import graph

from ol_orchestrate.ops.retrieve_edx_exports import (
    download_edx_data,
    extract_files,
    upload_files,
)


@graph(
    description=(
        "Retrieve and extract edX.org exports maintained by institutional research "
        "from a gcs bucket into S3 buckets on a weekly basis."
    ),
    tags={
        "source": "gcs",
        "destination": "s3",
        "owner": "institutional-research",
        "consumer": "platform-engineering",
    },
)
def retrieve_edx_exports():
    upload_files(extract_files(download_edx_data()))


# TODO: Separate jobs for course exports, tracking logs, and csvs  # noqa: E501, FIX002, TD002, TD003
