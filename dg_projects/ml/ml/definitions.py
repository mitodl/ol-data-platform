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
from ml.assets.courseware_description import courseware_concept_description
from ml.assets.courseware_extraction import (
    courseware_extraction,
    courseware_extraction_text,
)
from ml.assets.courseware_graph import courseware_concept_graph
from ml.assets.courseware_release import (
    courseware_knowledge_release,
    courseware_release_publication,
)
from ml.assets.courseware_resolution import courseware_resolution
from ml.assets.feedback_category_proposals import feedback_category_proposals
from ml.assets.feedback_cluster_assignment import feedback_cluster_assignment
from ml.assets.feedback_cluster_identity import feedback_cluster_identity
from ml.assets.feedback_clusters import feedback_clusters
from ml.assets.feedback_embeddings import feedback_embeddings
from ml.assets.feedback_redacted import feedback_redacted
from ml.assets.feedback_sentiment_eval import feedback_sentiment_eval
from ml.assets.feedback_summaries import feedback_summaries
from ml.assets.learn_embedding_checks import learn_parity_definitions
from ml.assets.learn_embeddings import (
    content_file_embedding_chunks,
    content_file_embedding_vectors,
    embedding_strategies,
    learn_content_file_text,
    topic_embedding_chunks,
    topic_embedding_vectors,
)
from ml.assets.risk_probability import student_risk_probability
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import configure_ol_llm
from ml.sensors.courseware import courseware_extraction_text_sensor
from ml.sensors.feedback_clusters import feedback_clusters_growth_sensor
from ml.sensors.learn_embeddings import learn_content_file_text_sensor
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
from ol_orchestrate.resources.embeddings import EmbeddingsResource

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

# Must run before any Opik client/tracer/@opik.track call or ol-llm model in this
# process -- see ml/resources/opik_auth.py. Tracing stays off wherever
# is_opik_configured() is False for this deployment.
configure_ol_llm(vault)

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
# runs from feedback_clusters_growth_sensor). Re-enable in the UI after deploy if it was
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

# Claim extraction, one run per course run, and the corpus-wide resolution,
# description and graph steps after it, and the knowledge release that publishes
# their outputs together. Extraction runs only for the course runs
# courseware_extraction_allowlist approves for the deployed extractor version, so
# the allowlist bounds what a first observation or a promotion launches. Stopped
# by default, and enabled after courseware_extraction_text_sensor has observed the
# runs.
#
# Every run it launches carries COURSEWARE_MODEL_CALLS_TAG, which ol-infrastructure's
# run_coordinator tag_concurrency_limits caps. Dagster's instance config has no
# per-pool limit (only pools.default_limit, which would bind every pool), so without
# the tag limit a promotion of N runs would start N runs at once, each making
# max_workers concurrent model calls and committing to the same three Iceberg tables.
COURSEWARE_MODEL_CALLS_TAG = "courseware/model_calls"
courseware_automation_sensor = AutomationConditionSensorDefinition(
    name="courseware_automation_sensor",
    target=AssetSelection.assets(
        courseware_extraction,
        courseware_resolution,
        courseware_concept_description,
        courseware_concept_graph,
        courseware_knowledge_release,
        courseware_release_publication,
    ),
    run_tags={COURSEWARE_MODEL_CALLS_TAG: "true"},
    default_status=DefaultSensorStatus.STOPPED,
)

# Chunking and embedding of Learn's content files and topics. Every run it launches
# carries LEARN_EMBEDDINGS_TAG for ol-infrastructure's run_coordinator
# tag_concurrency_limits to bound the model calls and the concurrent commits to the
# vector tables, as for courseware. That limit does not exist yet, so add it before
# turning this on: a chunker change requests every course run at once. Stopped by
# default: the first build of each table is a backfill launched by hand.
LEARN_EMBEDDINGS_TAG = "learn_embeddings/model_calls"
learn_embeddings_automation_sensor = AutomationConditionSensorDefinition(
    name="learn_embeddings_automation_sensor",
    target=AssetSelection.assets(
        content_file_embedding_chunks,
        content_file_embedding_vectors,
        topic_embedding_chunks,
        topic_embedding_vectors,
        embedding_strategies,
    ),
    run_tags={LEARN_EMBEDDINGS_TAG: "true"},
    default_status=DefaultSensorStatus.STOPPED,
)

iceberg_catalog = IcebergCatalogConfig(
    properties={
        "type": "glue",
        "glue.region": "us-east-1",
        # Write/commit via fsspec/s3fs (aiobotocore) instead of the
        # default PyArrow S3 FileIO, whose native threads deadlock on
        # K8s in handle_output and ignore the configured S3 timeouts.
        # reader_override below only covers the Polars read path.
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.region": "us-east-1",
        "s3.connect-timeout": "10",
        "s3.request-timeout": "120",
    }
)

dynamic_partitions_io_manager = PolarsIcebergIOManager(
    name="iceberg_io_manager",
    config=iceberg_catalog,
    reader_override="pyiceberg",
    db_io_manager="custom",
)

# The smokechecks of the Learn vectors against Learn's Qdrant run with every
# build of the vector assets. Dev has no Learn cluster, so it has neither the
# checks nor their resource.
learn_parity_resources, learn_parity_checks = learn_parity_definitions(
    vault, DAGSTER_ENV
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
            config=iceberg_catalog,
            reader_override="pyiceberg",
        ),
        # The same catalog for the assets whose partitions have a dynamic
        # dimension. Dagster's default DbIOManager resolves a multi-partition key
        # range without the instance, which a dynamic dimension needs, so writing
        # fails; dagster-iceberg's custom one reads the run's keys directly.
        "courseware_io_manager": dynamic_partitions_io_manager,
        "learn_embeddings_io_manager": dynamic_partitions_io_manager,
        "s3file_io_manager": default_file_object_io_manager(
            dagster_env=DAGSTER_ENV,
            bucket=s3_uploads_bucket(DAGSTER_ENV)["bucket"],
            path_prefix=s3_uploads_bucket(DAGSTER_ENV)["prefix"],
        ),
        "vault": vault,
        "s3": S3Resource(),
        # Category proposals and sentiment eval; feedback_summaries and
        # feedback_embeddings call models through ol-llm (SUMMARY_MODEL in
        # ml.lib.summarize, EMBEDDING_MODEL in ml.lib.embed). Bedrock in every
        # deployed env (IAM metadata auth, no API key needed) -- only "dev" lacks
        # the Bedrock IAM role, so it needs an API key there.
        # SUMMARY_PROVIDER overrides the client_class picked here.
        # LLM_BASE_URL/LLM_AZURE_ENDPOINT are only required for
        # 'openai_compatible'/'azure_openai'.
        "llm": LLMClientFactory(
            client_class=os.environ.get(
                "SUMMARY_PROVIDER",
                "anthropic" if DAGSTER_ENV == "dev" else "bedrock",
            ),
            base_url=os.environ.get("LLM_BASE_URL"),
            azure_endpoint=os.environ.get("LLM_AZURE_ENDPOINT"),
        ),
        # The Learn embedding assets call Azure OpenAI through ol-llm. Deployed
        # envs authenticate with the pod's workload identity; dev has no Azure
        # identity, so ol-llm's client falls back to AZURE_OPENAI_API_KEY there.
        "embeddings": EmbeddingsResource(
            vault=vault,
            environment=DAGSTER_ENV,
            azure_ad_auth=DAGSTER_ENV != "dev",
        ),
        # Learn's cluster, read-only, for the learn_v1 smokechecks (not in dev).
        **learn_parity_resources,
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
            courseware_extraction_text,
            courseware_extraction,
            courseware_resolution,
            courseware_concept_description,
            courseware_concept_graph,
            courseware_knowledge_release,
            courseware_release_publication,
            learn_content_file_text,
            content_file_embedding_chunks,
            content_file_embedding_vectors,
            topic_embedding_chunks,
            topic_embedding_vectors,
            embedding_strategies,
        ]
    ),
    asset_checks=learn_parity_checks,
    jobs=[
        data_export_job,
        feedback_redacted_job,
        feedback_summaries_job,
        feedback_embeddings_job,
        feedback_clusters_job,
        feedback_sentiment_eval_job,
    ],
    sensors=[
        feedback_pipeline_automation_sensor,
        feedback_clusters_growth_sensor,
        courseware_extraction_text_sensor,
        courseware_automation_sensor,
        learn_content_file_text_sensor,
        learn_embeddings_automation_sensor,
    ],
)
