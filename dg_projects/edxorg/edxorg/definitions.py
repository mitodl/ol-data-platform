"""
EdX.org data synchronization and processing definitions.

Synchronizes and processes edx.org data archives and tracking logs from IRx (MIT
Institutional Research). This includes course exports, tracking logs, and program
credential reports.
"""

import os
from functools import partial
from typing import Any, Literal

from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    Definitions,
    ScheduleDefinition,
    SensorDefinition,
    define_asset_job,
    job,
)
from dagster_aws.s3 import S3Resource

from edxorg.assets.edxorg_api import (
    edxorg_mitx_course_metadata,
    edxorg_program_metadata,
)
from edxorg.assets.edxorg_archive import (
    dummy_edxorg_course_structure,
    edxorg_raw_data_archive,
    edxorg_raw_tracking_logs,
    flatten_edxorg_course_structure,
    normalize_edxorg_tracking_log,
)
from edxorg.assets.openedx_course_archives import (
    dummy_edxorg_course_xml,
    extract_edxorg_courserun_metadata,
)
from edxorg.io_managers.gcs import GCSFileIOManager
from edxorg.jobs.retrieve_edx_exports import retrieve_edx_course_exports
from edxorg.ops.object_storage import (
    S3DownloadConfig,
    S3UploadConfig,
    download_files_from_s3,
    upload_files_to_s3,
)
from ol_orchestrate.io_managers.filepath import (
    FileObjectIOManager,
    S3FileObjectIOManager,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory
from ol_orchestrate.resources.outputs import DailyResultsDir, SimpleResultsDir
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.sensors.object_storage import (
    gcs_multi_file_sensor,
    s3_multi_file_sensor,
)

# Initialize vault with resilient loading
try:
    if DAGSTER_ENV == "dev":
        vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
        vault._auth_github()  # noqa: SLF001
        vault_authenticated = True
    else:
        vault = Vault(
            vault_addr=VAULT_ADDRESS, vault_role="dagster-server", aws_auth_mount="aws"
        )
        vault._auth_aws_iam()  # noqa: SLF001
        vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False


def s3_uploads_bucket(
    dagster_env: Literal["dev", "qa", "production"],
) -> dict[str, Any]:
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": ""},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "",
        },
    }
    return bucket_map[dagster_env]


# Initialize GCS connection with resilient loading
try:
    if vault_authenticated:
        gcs_connection = GCSConnection(
            **vault.client.secrets.kv.v1.read_secret(
                mount_point="secret-data", path="pipelines/edx/org/gcp-oauth-client"
            )["data"]
        )
    else:
        # Mock GCS connection for testing
        gcs_connection = GCSConnection(
            service_account_json='{"type": "service_account"}',
            project_id="test-project",
        )
except Exception as e:  # noqa: BLE001
    import warnings

    warnings.warn(
        f"Failed to initialize GCS connection: {e}. Using mock.", stacklevel=2
    )
    # Create minimal mock GCS connection for testing
    gcs_connection = GCSConnection(
        service_account_json='{"type": "service_account"}',
        project_id="test-project",
        client_email="test@test.iam.gserviceaccount.com",
        client_id="123456",
        client_x509_cert_url="https://test.com/cert",
        private_key="-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----",  # pragma: allowlist secret  # noqa: E501
        private_key_id="test",
        auth_uri="https://accounts.google.com/o/oauth2/auth",
        token_uri="https://oauth2.googleapis.com/token",  # noqa: S106 (not a password)
    )

# EdX.org course data and tracking logs jobs
edxorg_course_data_job = retrieve_edx_course_exports.to_job(
    name="retrieve_edxorg_raw_data",
    resource_defs={
        "results_dir": DailyResultsDir.configure_at_launch(),
        "gcp_gcs": gcs_connection,
        "gcs_input": GCSFileIOManager(gcs=gcs_connection),
    },
)

edxorg_tracking_logs_job = define_asset_job(
    name="refresh_edxorg_tracking_logs",
    selection=AssetSelection.assets(normalize_edxorg_tracking_log),
)

# Program credentials sync
dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")
download_config = S3DownloadConfig(
    source_bucket="edx-program-reports",
)
upload_config = S3UploadConfig(
    destination_bucket=f"ol-data-lake-landing-zone-{dagster_deployment.lower()}",
    destination_prefix="edxorg-program-credentials",
)


@job(
    name="sync_edxorg_program_reports",
    description="Replicate program credential reports from edx.org into our own S3 "
    "bucket so that it can be ingested into the OL data lake.",
    config={
        "ops": {
            "download_files_from_s3": {"config": download_config.dict()},
            "upload_files_to_s3": {"config": upload_config.dict()},
        }
    },
)
def sync_edxorg_program_reports():
    upload_files_to_s3(download_files_from_s3())


# Sensors
try:
    from edxorg.sensors.object_storage import (
        gcs_edxorg_archive_sensor,
        gcs_edxorg_tracking_log_sensor,
    )

    sensors_available = True
except Exception:  # noqa: BLE001
    sensors_available = False
    gcs_edxorg_archive_sensor = None
    gcs_edxorg_tracking_log_sensor = None

edxorg_program_reports_sensor = SensorDefinition(
    name="edxorg_program_reports_sensor",
    evaluation_fn=partial(
        s3_multi_file_sensor,
        "edx-program-reports",
        bucket_prefix="reports_v2/MITx/",
        run_config_fn=lambda new_keys: {
            "ops": {
                "download_files_from_s3": {
                    "config": {
                        **download_config.dict(),
                        "object_keys": list(new_keys),
                    }
                },
                "upload_files_to_s3": {"config": upload_config.dict()},
            }
        },
    ),
    job=sync_edxorg_program_reports,
    default_status=DefaultSensorStatus.STOPPED,
    minimum_interval_seconds=86400,
)

edxorg_course_bundle_sensor = SensorDefinition(
    evaluation_fn=partial(gcs_multi_file_sensor, "simeon-mitx-course-tarballs"),
    name="edxorg_course_bundle_sensor",
    minimum_interval_seconds=86400,
    job=edxorg_course_data_job,
    default_status=DefaultSensorStatus.STOPPED,
)

# Schedule
edxorg_api_daily_schedule = ScheduleDefinition(
    name="edxorg_api_daily_schedule",
    job=define_asset_job(
        name="edxorg_api_daily_job",
        selection=AssetSelection.assets(
            edxorg_program_metadata, edxorg_mitx_course_metadata
        ),
    ),
    cron_schedule="0 5 * * *",
    execution_timezone="UTC",
)

# Build sensor list
sensor_list = [edxorg_program_reports_sensor, edxorg_course_bundle_sensor]
if sensors_available and gcs_edxorg_archive_sensor and gcs_edxorg_tracking_log_sensor:
    sensor_list.extend(
        [
            gcs_edxorg_archive_sensor.with_updated_job(edxorg_course_data_job),
            gcs_edxorg_tracking_log_sensor,
        ]
    )

# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": FileObjectIOManager(
            vault=vault,
            vault_gcs_token_path="secret-data/pipelines/edx/org/gcp-oauth-client",  # noqa: S106
        ),
        "s3file_io_manager": S3FileObjectIOManager(
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "gcs_input": GCSFileIOManager(gcs=gcs_connection),
        "vault": vault,
        "edxorg_api": OpenEdxApiClientFactory(deployment="edxorg", vault=vault),
        "s3": S3Resource(profile_name="edxorg"),
        "s3_download": S3Resource(profile_name="edxorg"),
        "s3_upload": S3Resource(),
        "results_dir": SimpleResultsDir.configure_at_launch(),
    },
    sensors=sensor_list,
    jobs=[
        edxorg_course_data_job,
        edxorg_tracking_logs_job,
        sync_edxorg_program_reports,
    ],
    assets=[
        edxorg_raw_data_archive.to_source_asset(),
        edxorg_raw_tracking_logs.to_source_asset(),
        normalize_edxorg_tracking_log,
        dummy_edxorg_course_structure,
        flatten_edxorg_course_structure,
        extract_edxorg_courserun_metadata,
        dummy_edxorg_course_xml,
        edxorg_program_metadata,
        edxorg_mitx_course_metadata,
    ],
    schedules=[edxorg_api_daily_schedule],
)
