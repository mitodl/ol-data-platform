"""Canvas course export definitions.

Exports Canvas course content and metadata for specified course IDs.
"""

from typing import Any

from dagster import (
    ConfigurableResource,
    Definitions,
    OpExecutionContext,
    RunRequest,
    define_asset_job,
    schedule,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket

from canvas.assets.canvas import (
    canvas_course_ids,
    course_content_metadata,
    export_course_content,
)
from canvas.resources.api_client_factory import ApiClientFactory
from canvas.sensors.canvas import canvas_google_sheet_course_id_sensor

# Initialize vault with resilient loading
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    from ol_orchestrate.resources.secrets.vault import Vault

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = Vault(vault_addr=VAULT_ADDRESS, vault_auth_type="github")
    vault_authenticated = False

# Get Google Sheets credentials
try:
    if vault_authenticated:
        gs_secrets = vault.client.secrets.kv.v1.read_secret(
            mount_point="secret-data",
            path="pipelines/google-service-account",
        )["data"]
    else:
        gs_secrets = {}
except Exception:  # noqa: BLE001
    gs_secrets = {}


class GoogleSheetConfig(ConfigurableResource):
    service_account_json: dict[str, Any]  # Service account JSON credentials
    # Google Sheet ID for canvas course IDs
    sheet_id: str = "13AoothEhEvWs2cJEEfZETm7E6h3-ZY4tD11KX_ARe1A"
    worksheet_id: int = 1472315099  # Worksheet ID (gid) within the Google Sheet


# Asset job that will be executed per partition (course_id)
canvas_course_export_job = define_asset_job(
    name="canvas_course_export_job",
    selection=[export_course_content],
    partitions_def=canvas_course_ids,
)


@schedule(
    cron_schedule="0 */6 * * *",
    job=canvas_course_export_job,
    execution_timezone="Etc/UTC",
    required_resource_keys={"google_sheet_config"},
)
def canvas_course_export_schedule(context: OpExecutionContext):
    """Return a RunRequest for each canvas course ID found in the Google Sheet"""
    partition_keys = context.instance.get_dynamic_partitions("canvas_course_ids")

    return [
        RunRequest(
            run_key=partition_key,
            partition_key=partition_key,
        )
        for partition_key in partition_keys
    ]


# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        "canvas_api": ApiClientFactory(
            deployment="canvas",
            client_class="CanvasApiClient",
            mount_point="secret-data",
            config_path="pipelines/canvas",
            kv_version="1",
            vault=vault,
        ),
        "learn_api": ApiClientFactory(
            deployment="mit-learn",
            client_class="MITLearnApiClient",
            mount_point="secret-global",
            config_path="shared_hmac",
            kv_version="2",
            vault=vault,
        ),
        "google_sheet_config": GoogleSheetConfig(service_account_json=gs_secrets),
    },
    assets=[export_course_content, course_content_metadata],
    schedules=[canvas_course_export_schedule],
    sensors=[canvas_google_sheet_course_id_sensor],
)
