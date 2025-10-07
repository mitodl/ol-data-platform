from dagster import graph

from legacy_openedx.ops.edx_gcs_courses import (
    download_edx_gcs_course_data,
    upload_edx_gcs_course_data_to_s3,
)


@graph(
    description=(
        "Extract Open edX data maintained by institutional research "
        "from a gcs bucket into a date-stamped paths in S3 on a "
        "nightly basis."
    ),
    tags={
        "source": "gcs",
        "destination": "s3",
        "owner": "institutional-research",
        "consumer": "platform-engineering",
    },
)
def sync_gcs_to_s3():
    upload_edx_gcs_course_data_to_s3(download_edx_gcs_course_data())
