import os

from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
    default_io_manager,
)
from ol_orchestrate.lib.utils import authenticate_vault

from b2b_organization.assets.data_export import export_b2b_organization_data
from b2b_organization.sensors.b2b_organization import b2b_organization_list_sensor

b2b_org_bucket = os.environ.get(
    "B2B_ORGANIZATION_BUCKET", "ol-data-lake-landing-zone-production"
)

b2b_bucket_map = {
    "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
    "ci": {"bucket": "ol-data-lake-landing-zone-ci", "prefix": "b2b-organization-data"},
    "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": "b2b-organization-data"},
    "production": {"bucket": b2b_org_bucket, "prefix": ""},
}

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


b2b_organization_data_export_job = define_asset_job(
    name="b2b_organization_data_export_job",
    selection=[export_b2b_organization_data],
)

# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": default_io_manager(DAGSTER_ENV),
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=b2b_bucket_map[DAGSTER_ENV]["bucket"],
            path_prefix=b2b_bucket_map[DAGSTER_ENV]["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
    },
    assets=[export_b2b_organization_data],
    jobs=[b2b_organization_data_export_job],
    sensors=[b2b_organization_list_sensor],
)
