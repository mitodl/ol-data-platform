import os

from dagster import (
    AssetSelection,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
    Definitions,
    define_asset_job,
)
from dagster_aws.s3 import S3Resource
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from ml.assets.feedback_category_proposals import feedback_category_proposals
from ml.assets.feedback_cluster_assignment import feedback_cluster_assignment
from ml.assets.feedback_cluster_identity import feedback_cluster_identity
from ml.assets.feedback_clusters import feedback_clusters
from ml.assets.feedback_embeddings import feedback_embeddings
from ml.assets.feedback_redacted import feedback_redacted
from ml.assets.feedback_sentiment_eval import feedback_sentiment_eval
from ml.assets.feedback_summaries import feedback_summaries
from ml.assets.risk_probability import student_risk_probability
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import configure_opik_keycloak_auth
from ml.schedules.feedback_clusters import feedback_clusters_schedule
from ml.sensors.feedback_clusters import feedback_clusters_growth_sensor
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

# Must run before any Opik client/tracer/@opik.track call in this process --
# see ml/resources/opik_auth.py. A no-op (returns False) wherever
# OPIK_URL_OVERRIDE isn't set for this deployment.
configure_opik_keycloak_auth(vault)

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

feedback_embeddings_job = define_asset_job(
    name="feedback_embeddings_job",
    selection=[feedback_embeddings],
)

feedback_clusters_job = define_asset_job(
    name="feedback_clusters_job",
    selection=[feedback_clusters],
)

# Human-triggered only, a one-time (or occasional) decision aid -- not a
# production pipeline step
feedback_sentiment_eval_job = define_asset_job(
    name="feedback_sentiment_eval_job",
    selection=[feedback_sentiment_eval],
)

# Covers the feedback pipeline's auto-cascading assets (feedback_clusters itself
# runs on its own schedule/sensor). Re-enable in the UI after deploy if it was
# previously on. Stopped by default.
feedback_pipeline_automation_sensor = AutomationConditionSensorDefinition(
    name="feedback_pipeline_automation_sensor",
    target=AssetSelection.assets(
        feedback_summaries,
        feedback_embeddings,
        feedback_cluster_identity,
        feedback_cluster_assignment,
        feedback_category_proposals,
    ),
    default_status=DefaultSensorStatus.STOPPED,
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
        # Bedrock in every deployed env (IAM metadata auth, no API key needed) --
        # only "dev" lacks the Bedrock IAM role, so it needs an API key there.
        # SUMMARY_PROVIDER/EMBEDDING_PROVIDER override the client_class picked here.
        # LLM_BASE_URL/LLM_AZURE_ENDPOINT are only required for
        # 'openai_compatible'/'azure_openai' -- shared with embedding_llm below on
        # the assumption local testing points both at the same gateway (e.g.
        # Parley); set client_class independently per resource if that's not true.
        "llm": LLMClientFactory(
            client_class=os.environ.get(
                "SUMMARY_PROVIDER",
                "anthropic" if DAGSTER_ENV == "dev" else "bedrock",
            ),
            base_url=os.environ.get("LLM_BASE_URL"),
            azure_endpoint=os.environ.get("LLM_AZURE_ENDPOINT"),
        ),
        # Separate resource, not a reused "llm": Anthropic/Bedrock has no embeddings
        # API, so this needs its own client_class/key. Which Bedrock embedding
        # model to keep is still open (§B.1's bake-off); bedrock_embeddings is the
        # default deployed envs need since no OPENAI_API_KEY is provisioned there.
        "embedding_llm": LLMClientFactory(
            client_class=os.environ.get(
                "EMBEDDING_PROVIDER",
                "openai" if DAGSTER_ENV == "dev" else "bedrock_embeddings",
            ),
            # Only required (and only read) when EMBEDDING_PROVIDER='openai_compatible'
            # -- e.g. a local gateway like Parley that fronts multiple providers
            # behind one OpenAI-shaped API. Shared var with "llm" above.
            base_url=os.environ.get("LLM_BASE_URL"),
        ),
    },
    assets=with_failure_hooks(
        [
            student_risk_probability,
            feedback_redacted,
            feedback_summaries,
            feedback_embeddings,
            feedback_clusters,
            feedback_cluster_identity,
            feedback_cluster_assignment,
            feedback_category_proposals,
            feedback_sentiment_eval,
        ]
    ),
    jobs=[
        data_export_job,
        feedback_redacted_job,
        feedback_summaries_job,
        feedback_embeddings_job,
        feedback_clusters_job,
        feedback_sentiment_eval_job,
    ],
    schedules=[feedback_clusters_schedule],
    sensors=[feedback_pipeline_automation_sensor, feedback_clusters_growth_sensor],
)
