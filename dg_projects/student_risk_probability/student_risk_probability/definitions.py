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
from ol_orchestrate.lib.utils import authenticate_vault, s3_uploads_bucket
from student_risk_probability.assets.risk_probability import student_risk_probability

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


data_export_job = define_asset_job(
    name="student_risk_probability_data_export_job",
    selection=[student_risk_probability],
)

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
    },
    assets=[student_risk_probability],
    jobs=[data_export_job],
)
