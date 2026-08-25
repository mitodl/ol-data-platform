from dagster import (
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from ml.assets.feedback_redacted import feedback_redacted
from ml.assets.feedback_summaries import feedback_summaries
from ml.assets.risk_probability import student_risk_probability
from ml.resources.llm import LLMClientFactory
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

init_sentry("ml")

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

data_export_job = define_asset_job(
    name="student_risk_probability_data_export_job",
    selection=[student_risk_probability],
)

feedback_redacted_job = define_asset_job(
    name="feedback_redacted_job",
    selection=[feedback_redacted],
)

feedback_summaries_job = define_asset_job(
    name="feedback_summaries_job",
    selection=[feedback_summaries],
)

# Create unified definitions
defs = Definitions(
    resources={
        # namespace intentionally omitted here: dagster's DbIOManager schema
        # precedence is output metadata > io_manager namespace > asset key
        # prefix > "public", and the AssetKey prefix (["reporting", ...] /
        # ["intermediate", ...]) is a literal Glue database name, not env-aware
        # -- it does not expand to ol_warehouse_production_<suffix>_reporting.
        # Each asset sets its own env-aware `metadata={"schema": ...}` instead
        # (see risk_probability.py / feedback_redacted.py), which is what
        # actually determines its write target; a shared namespace here would
        # force both assets to the same schema.
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
    assets=with_failure_hooks(
        [student_risk_probability, feedback_redacted, feedback_summaries]
    ),
    jobs=[data_export_job, feedback_redacted_job, feedback_summaries_job],
)
