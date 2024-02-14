from dagster import graph

from ol_orchestrate.assets.edxorg_archive import (
    process_edxorg_archive_bundle,
)
from ol_orchestrate.ops.retrieve_edx_exports import (
    download_edx_data,
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
    process_edxorg_archive_bundle()


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
    upload_files(download_edx_data())
