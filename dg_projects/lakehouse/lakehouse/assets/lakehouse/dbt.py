import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AutomationCondition,
    OpExecutionContext,
    job,
    op,
)
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliInvocation,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV

from lakehouse.lib.dbt_environment import DBT_AUTOMATION_ENABLED, DBT_TARGET
from lakehouse.lib.surrogate_key_drift import (
    SURROGATE_KEY_STATE_ARTIFACT,
    SurrogateKeyDrift,
    detect_drift,
    full_refresh_build_args,
)
from lakehouse.resources.dbt_s3_artifacts import DbtS3ArtifactsResource

DBT_REPO_DIR = (
    Path(__file__).parents[5].joinpath("src/ol_dbt")
    if DAGSTER_ENV == "dev"
    else Path("/opt/dbt")
)

dbt_project = DbtProject(project_dir=DBT_REPO_DIR, target=DBT_TARGET)
dbt_project.prepare_if_dev()

# Built once and reused rather than reconstructed for every dbt node. None
# outside DBT_AUTOMATION_ENVIRONMENTS -- see that declaration for why the
# condition, and not dbt_automation_sensor's default_status, is what actually
# holds an environment closed.
_DBT_AUTOMATION_CONDITION = (
    upstream_or_code_changes() if DBT_AUTOMATION_ENABLED else None
)


class DbtAutomationTranslator(DagsterDbtTranslator):
    def get_automation_condition(
        self,
        dbt_resource_props: Mapping[str, Any],  # noqa: ARG002
    ) -> AutomationCondition | None:
        return _DBT_AUTOMATION_CONDITION

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        """
        Extract the group name from the schema configuration in the dbt resource
        properties.
        """
        return dbt_resource_props.get("config", {}).get("schema", None)


def _surrogate_key_drift(
    context: AssetExecutionContext, dbt_s3_artifacts: DbtS3ArtifactsResource
) -> SurrogateKeyDrift | None:
    """Compare this image's surrogate keys against the last build's, or None.

    None means the check is unavailable, not that nothing drifted — without the
    artifacts bucket there is nowhere to keep the baseline, so there is nothing
    to compare against and nothing to record afterwards.

    The parsed manifest is decoded here (rather than through
    ``load_manifest``) because the hash inputs come from each node's
    ``raw_code``, which the registry deliberately does not retain.
    """
    if not dbt_s3_artifacts.s3_bucket:
        context.log.warning(
            "DBT_ARTIFACTS_S3_BUCKET is not configured; surrogate-key drift cannot be "
            "detected. A re-keyed dimension will silently orphan the FKs its "
            "incremental descendants hold."
        )
        return None
    manifest = json.loads(dbt_project.manifest_path.read_text())
    previous = dbt_s3_artifacts.read_json_artifact(
        SURROGATE_KEY_STATE_ARTIFACT, context
    )
    return detect_drift(manifest, previous)


def _models_built_by(invocation: DbtCliInvocation) -> set[str]:
    """Names of the models a finished invocation actually built.

    Read from run_results rather than from the asset selection: this asset is a
    subset build, and run_results is the only record of which models the subset
    resolved to.
    """
    results = invocation.get_artifact("run_results.json")["results"]
    return {
        result["unique_id"].split(".")[-1]
        for result in results
        if result["unique_id"].startswith("model.")
    }


def _repair_surrogate_key_drift(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    drift: SurrogateKeyDrift,
    built: set[str],
    build_vars: list[str],
) -> bool:
    """Rebuild the incremental models whose FKs this build's re-key invalidated.

    Returns whether the drift is now fully handled, which is the caller's cue
    to record the new key state. False leaves the previous state in place so
    the next run re-detects and finishes the job; raising (a failed repair
    build) has the same effect, since the caller never gets to the write.

    Runs *after* the main build, not before: the repair has to read the
    dimension's new keys, and those only exist once the build that regenerates
    them has run. Running first would rebuild the fact tables against the
    previous keys and orphan them all over again.

    Invoked without a Dagster context so it emits no second materialization for
    assets the main build already reported, and so its explicit ``--select``
    is not merged with the context's own selection.
    """
    models, complete = drift.resolved_against(built)
    if models:
        context.log.info(
            "Surrogate-key drift since the last build -- rebuilding the affected "
            "incremental models from scratch so their foreign keys match the "
            "regenerated dimension: %s",
            drift.describe(),
        )
        dbt.cli(full_refresh_build_args(models, build_vars), raise_on_error=True).wait()
    if not complete:
        context.log.warning(
            "This run did not build every model the surrogate-key drift touches, so "
            "the key state is not being recorded -- the next run will re-detect and "
            "finish the repair. Outstanding: %s",
            ", ".join(sorted(set(drift.models) - built)) or "the re-keyed model itself",
        )
    return complete


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    # Complementary partition with dbt_starrocks.py's starrocks_dbt_assets: tag a
    # model `starrocks` to move it from this Trino-scoped asset set into that
    # StarRocks-scoped one without touching either Python asset definition.
    exclude="tag:starrocks",
    dagster_dbt_translator=DbtAutomationTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
)
def full_dbt_project(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    dbt_s3_artifacts: DbtS3ArtifactsResource,
):
    build_vars: list[str] = []
    if DAGSTER_ENV == "dev":
        schema_suffix = os.getenv("DBT_SCHEMA_SUFFIX", "dev")
        build_vars = ["--vars", f"schema_suffix: {schema_suffix}"]

    drift = _surrogate_key_drift(context, dbt_s3_artifacts)

    build_invocation = dbt.cli(["build", *build_vars], context=context)
    yield from (build_invocation.stream().fetch_column_metadata().fetch_row_counts())

    if drift is not None and _repair_surrogate_key_drift(
        context, dbt, drift, _models_built_by(build_invocation), build_vars
    ):
        dbt_s3_artifacts.write_json_artifact(
            SURROGATE_KEY_STATE_ARTIFACT, drift.current_state, context
        )

    # Upload this run's results to a per-run versioned S3 key so OpenMetadata can
    # ingest the model/test outcomes of every incremental and full run.
    #
    # manifest.json and catalog.json are NOT generated here: producing the catalog
    # recompiles the whole project and queries every relation, which is far too
    # expensive to repeat on each incremental subset build. That work lives in the
    # dedicated `dbt_docs_artifacts_job`, which runs on a daily schedule.
    if DAGSTER_ENV != "dev":
        if not dbt_s3_artifacts.s3_bucket:
            context.log.warning(
                "DBT_ARTIFACTS_S3_BUCKET is not configured; dbt run results will "
                "not be uploaded to S3 for OpenMetadata ingestion."
            )
        else:
            dbt_s3_artifacts.upload_artifacts(
                build_invocation.target_path, ["run_results.json"], context
            )


@op(description="Generate dbt docs artifacts and upload them to S3 for OpenMetadata.")
def generate_dbt_docs_artifacts(
    context: OpExecutionContext,
    dbt: DbtCliResource,
    dbt_s3_artifacts: DbtS3ArtifactsResource,
) -> None:
    if not dbt_s3_artifacts.s3_bucket:
        context.log.warning(
            "DBT_ARTIFACTS_S3_BUCKET is not configured; dbt docs artifacts will "
            "not be uploaded to S3 for OpenMetadata ingestion."
        )
        return

    # Run without a Dagster context so it covers the full project (not just a
    # selected subset) and doesn't emit redundant asset materialization events.
    docs_invocation = dbt.cli(["docs", "generate"], raise_on_error=False)
    docs_invocation.wait()

    # manifest.json and catalog.json are deduplicated by content hash, so they are
    # only re-uploaded when their content has actually changed.
    artifacts = ["manifest.json"]
    if (docs_invocation.target_path / "catalog.json").exists():
        artifacts.append("catalog.json")
    else:
        context.log.warning(
            "dbt docs generate did not produce catalog.json; "
            "it will be omitted from the OpenMetadata artifact upload"
        )

    dbt_s3_artifacts.upload_artifacts(docs_invocation.target_path, artifacts, context)


@job(description="Regenerate dbt docs artifacts for OpenMetadata on a schedule.")
def dbt_docs_artifacts_job() -> None:
    generate_dbt_docs_artifacts()
