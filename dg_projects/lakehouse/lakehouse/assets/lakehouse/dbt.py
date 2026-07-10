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
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import DAGSTER_ENV

from lakehouse.resources.dbt_s3_artifacts import DbtS3ArtifactsResource

DBT_REPO_DIR = (
    Path(__file__).parents[5].joinpath("src/ol_dbt")
    if DAGSTER_ENV == "dev"
    else Path("/opt/dbt")
)


def _resolve_dbt_target() -> str:
    """Resolve the dbt profile target for this environment.

    Single source of truth shared by the DbtProject (which parses the manifest,
    and therefore the Dagster asset graph) and the DbtCliResource that executes
    it, so the graph always matches what actually runs. ``DAGSTER_DBT_TARGET``
    overrides the mapping when set.
    """
    if override := os.environ.get("DAGSTER_DBT_TARGET"):
        return override
    # qa and production both execute against the production target.
    return {"dev": "dev_production", "ci": "ci"}.get(DAGSTER_ENV, "production")


DBT_TARGET = _resolve_dbt_target()

dbt_project = DbtProject(project_dir=DBT_REPO_DIR, target=DBT_TARGET)
dbt_project.prepare_if_dev()

# Built once and reused rather than reconstructed for every dbt node.
_DBT_AUTOMATION_CONDITION = upstream_or_code_changes()


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


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=DbtAutomationTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
)
def full_dbt_project(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    dbt_s3_artifacts: DbtS3ArtifactsResource,
):
    dbt_build_args = ["build"]
    if DAGSTER_ENV == "dev":
        schema_suffix = os.getenv("DBT_SCHEMA_SUFFIX", "dev")
        dbt_build_args += ["--vars", f"schema_suffix: {schema_suffix}"]

    build_invocation = dbt.cli(dbt_build_args, context=context)
    yield from (build_invocation.stream().fetch_column_metadata().fetch_row_counts())

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
