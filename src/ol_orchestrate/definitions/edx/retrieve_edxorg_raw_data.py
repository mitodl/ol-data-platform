"""
Synchronize and process edx.org data archives and tracking logs.

The current flow of data from edx.org into the Open Learning data platform is routed
through IRx (MIT Institutional Research).

The data that we receive from edx.org is produced as two separate formats.  Tracking
logs are provided to us from IRx as individual gzipped JSONL files containing the
tracking events.  The remainder of the data is generated via the Analytics Exporter
logic found at https://github.com/openedx/edx-analytics-exporter/

The data packages contained in the tar.gz files generated by the analytics exporter are
segmented by course ID.  For each course, we receive:

    - A series of TSV files (with a .sql extension) that are representations of data
      pulled from the edX MySQL database

    - A BSON dump of the forum data for the course

    - A tar.gz archive of the course XML as generated by the edX course export
      functionality

For integration with the Open Learning platform, the tracking logs are synchronized and
processed so that nested JSON objects are 'stringified'.  This ensures that we are able
to process the nested documents via the Trino query engine.

The course archives are unpacked and uploaded to different S3 destinations.  The TSV
files are ingested and processed via Airbyte so that the structured data can be queried
via Trino and processed with dbt.  The course XML archives are processed to determine
the duration values of video elements.  There is also a course structure document that
is used to enrich the tracking events.

The overall flow of data is as follows:

    - Tracking Logs:

        1. Download the edX tracking logs from the Google Cloud Storage bucket owned by
           IRx.

        2. Normalize the log records

        3. Upload the normalized records as JSONL files to S3

        4. Ingest the tracking logs via Airbyte

    - Course data archives:

        1. Download the edX course export data from the Google Cloud Storage bucket
           owned by IRx.

        2. Enumerate the contents of the archive:

            - Upload TSV files to a /csv prefix in S3 for ingestion via Airbyte

            - Process course structure documents and upload as JSONL files to S3 for
              ingestion via Airbyte

            - Upload BSON files to a /forum prefix in S3

            - Upload XML files to a /courses prefix in S3
"""

from typing import Any, Literal

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource

from ol_orchestrate.assets.edxorg_archive import (
    dummy_edxorg_course_structure,
    edxorg_archive_partitions,
    edxorg_raw_data_archive,
    edxorg_raw_tracking_logs,
    flatten_edxorg_course_structure,
    gcs_edxorg_archive_sensor,
    gcs_edxorg_tracking_log_sensor,
    normalize_edxorg_tracking_log,
)
from ol_orchestrate.io_managers.filepath import (
    FileObjectIOManager,
    S3FileObjectIOManager,
)
from ol_orchestrate.io_managers.gcs import GCSFileIOManager
from ol_orchestrate.jobs.retrieve_edx_exports import (
    retrieve_edx_course_exports,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.secrets.vault import Vault

if DAGSTER_ENV == "dev":
    vault_config = {
        "vault_addr": VAULT_ADDRESS,
        "vault_auth_type": "github",
    }
    vault = Vault(**vault_config)
    vault._auth_github()  # noqa: SLF001
else:
    vault_config = {
        "vault_addr": VAULT_ADDRESS,
        "vault_role": "dagster-server",
        "vault_auth_type": "aws-iam",
        "aws_auth_mount": "aws",
    }
    vault = Vault(**vault_config)
    vault._auth_aws_iam()  # noqa: SLF001

gcs_connection = GCSConnection(
    **vault.client.secrets.kv.v1.read_secret(
        mount_point="secret-data", path="pipelines/edx/org/gcp-oauth-client"
    )["data"]
)


def s3_uploads_bucket(
    dagster_env: Literal["dev", "qa", "production"],
) -> dict[str, Any]:
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": "edxorg-raw-data"},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "edxorg-raw-data",
        },
    }
    return bucket_map[dagster_env]


def edxorg_data_archive_config(dagster_env):
    return {
        "ops": {
            "process_edxorg_archive_bundle": {
                "config": {
                    "s3_bucket": s3_uploads_bucket(dagster_env)["bucket"],
                    "s3_prefix": s3_uploads_bucket(dagster_env)["prefix"],
                }
            },
        }
    }


def edxorg_tracking_logs_config(dagster_env):
    return {
        "ops": {
            "edxorg__raw_data__tracking_logs": {
                "config": {
                    "s3_bucket": s3_uploads_bucket(dagster_env)["bucket"],
                    "bucket_prefix": s3_uploads_bucket(dagster_env)["prefix"],
                }
            },
        }
    }


edxorg_course_data_job = retrieve_edx_course_exports.to_job(
    name="retrieve_edx_course_exports",
    config=edxorg_data_archive_config(DAGSTER_ENV),
    partitions_def=edxorg_archive_partitions,
)

edxorg_tracking_logs_job = define_asset_job(
    name="process_raw_edxorg_tracking_logs",
    selection=AssetSelection.assets(normalize_edxorg_tracking_log),
    config=edxorg_tracking_logs_config(DAGSTER_ENV),
)

file_regex = {
    "courses": r"COLD/mitx-\d{4}-\d{2}-\d{2}.tar.gz$",
    "logs": r"COLD/mitx-edx-events-\d{4}-\d{2}-\d{2}.log.gz$",
}

retrieve_edx_exports = Definitions(
    resources={
        "gcp_gcs": gcs_connection,
        "s3": S3Resource(),
        "exports_dir": DailyResultsDir.configure_at_launch(),
        "io_manager": FileObjectIOManager(
            vault=Vault(**vault_config),
            vault_gcs_token_path="secret-data/pipelines/edx/org/gcp-oauth-client",  # noqa: S106
        ),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "gcs_input": GCSFileIOManager(gcs=gcs_connection),
        "vault": vault,
    },
    sensors=[
        gcs_edxorg_archive_sensor.with_updated_job(edxorg_course_data_job),
        gcs_edxorg_tracking_log_sensor,
    ],
    jobs=[edxorg_course_data_job, edxorg_tracking_logs_job],
    assets=[
        edxorg_raw_data_archive.to_source_asset(),
        edxorg_raw_tracking_logs.to_source_asset(),
        normalize_edxorg_tracking_log,
        dummy_edxorg_course_structure,
        flatten_edxorg_course_structure,
    ],
)