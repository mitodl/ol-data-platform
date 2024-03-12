import os
from functools import partial

from dagster import DefaultSensorStatus, Definitions, SensorDefinition
from dagster_aws.s3.resources import s3_resource

from ol_orchestrate.jobs.edx_gcs_courses import sync_gcs_to_s3
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.resources.gcp_gcs import GCSConnection
from ol_orchestrate.resources.outputs import DailyResultsDir
from ol_orchestrate.resources.secrets.vault import Vault
from ol_orchestrate.sensors.object_storage import gcs_multi_file_sensor

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

resources = {
    "gcp_gcs": gcs_connection,
    "s3": s3_resource,
    "results_dir": DailyResultsDir.configure_at_launch(),
}

course_upload_bucket = {
    "qa": "edxorg-qa-edxapp-courses",
    "production": "edxorg-production-edxapp-courses",
}

dagster_deployment = os.getenv("DAGSTER_ENVIRONMENT", "qa")

gcs_sync_job = sync_gcs_to_s3.to_job(
    name="edx_gcs_course_retrieval",
    config={
        "ops": {
            "edx_upload_gcs_course_tarballs": {
                "config": {
                    "edx_etl_results_bucket": course_upload_bucket[dagster_deployment]
                }
            }
        }
    },
)

edx_gcs_courses = Definitions(
    sensors=[
        SensorDefinition(
            evaluation_fn=partial(gcs_multi_file_sensor, "simeon-mitx-course-tarballs"),
            name="edxorg_course_bundle_sensor",
            minimum_interval_seconds=86400,
            job=gcs_sync_job,
            default_status=DefaultSensorStatus.RUNNING,
        )
    ],
    resources=resources,
    jobs=[gcs_sync_job],
)
