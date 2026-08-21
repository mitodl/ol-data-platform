import os

from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from feedback_clustering.assets.feedback_clustering import feedback_redacted
from feedback_clustering.resources.llm import LLMClientFactory
from ol_orchestrate.lib.constants import DAGSTER_ENV, VAULT_ADDRESS
from ol_orchestrate.lib.dagster_helpers import (
    default_file_object_io_manager,
)
from ol_orchestrate.lib.failures import with_failure_hooks
from ol_orchestrate.lib.sentry import init_sentry
from ol_orchestrate.lib.utils import (
    authenticate_vault,
    s3_uploads_bucket,
    unauthenticated_vault,
)

init_sentry("feedback_clustering")

# Initialize vault with resilient loading
try:
    vault = authenticate_vault(DAGSTER_ENV, VAULT_ADDRESS)
    vault_authenticated = True
except Exception as e:  # noqa: BLE001 (resilient loading)
    import warnings

    warnings.warn(
        f"Failed to authenticate with Vault: {e}. Using mock configuration.",
        stacklevel=2,
    )
    vault = unauthenticated_vault(VAULT_ADDRESS)
    vault_authenticated = False

if DAGSTER_ENV == "dev":
    database_name = (
        f"ol_warehouse_production_{os.environ.get('DBT_SCHEMA_SUFFIX')}_intermediate"
    )
else:
    database_name = "ol_warehouse_production_intermediate"

feedback_redacted_job = define_asset_job(
    name="feedback_redacted_job",
    selection=[feedback_redacted],
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
                    "s3.region": "us-east-1",
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
        "llm": LLMClientFactory(vault=vault),
    },
    assets=with_failure_hooks([feedback_redacted]),
    jobs=[feedback_redacted_job],
)
