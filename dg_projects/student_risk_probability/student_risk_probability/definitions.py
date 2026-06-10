import os

from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
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

if DAGSTER_ENV == "dev":
    database_name = (
        f"ol_warehouse_production_{os.environ.get('DBT_SCHEMA_SUFFIX')}_reporting"
    )
else:
    database_name = "ol_warehouse_production_reporting"

data_export_job = define_asset_job(
    name="student_risk_probability_data_export_job",
    selection=[student_risk_probability],
)

# Create unified definitions
defs = Definitions(
    resources={
        "io_manager": PolarsIcebergIOManager(
            name="iceberg_io_manager",
            config=IcebergCatalogConfig(
                properties={
                    "type": "glue",
                    "glue.region": "us-east-1",
                    # Write/commit via fsspec/s3fs (aiobotocore) instead of the
                    # default PyArrow S3 FileIO, whose native threads deadlock on
                    # K8s in handle_output and ignore the configured S3 timeouts.
                    # reader_override above only covers the Polars read path.
                    "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                    "s3.connect-timeout": "10",
                    "s3.request-timeout": "120",
                }
            ),
            namespace=database_name,
            reader_override="pyiceberg",
        ),
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
