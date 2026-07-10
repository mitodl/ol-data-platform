import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dagster import AssetExecutionContext, AutomationCondition
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

dbt_project = DbtProject(
    project_dir=DBT_REPO_DIR, target=os.environ.get("DAGSTER_DBT_TARGET", DAGSTER_ENV)
)
dbt_project.prepare_if_dev()


class DbtAutomationTranslator(DagsterDbtTranslator):
    def get_automation_condition(
        self,
        dbt_resource_props: Mapping[str, Any],  # noqa: ARG002
    ) -> AutomationCondition | None:
        return upstream_or_code_changes()

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

    if DAGSTER_ENV != "dev":
        if not dbt_s3_artifacts.s3_bucket:
            context.log.warning(
                "DBT_ARTIFACTS_S3_BUCKET is not configured; dbt artifacts will not "
                "be uploaded to S3 for OpenMetadata ingestion."
            )
        else:
            # `dbt docs generate` recompiles the project and writes its own
            # run_results.json to the target path. Point it at a dedicated
            # subdirectory so it does not overwrite the build's run_results.json
            # (which records the actual model/test outcomes we ship per run).
            docs_target_path = build_invocation.target_path / "docs"
            # Run docs generate without context so it covers the full project and
            # doesn't emit redundant Dagster events.
            docs_invocation = dbt.cli(
                ["docs", "generate"],
                target_path=docs_target_path,
                raise_on_error=False,
            )
            docs_invocation.wait()

            # run_results.json is uploaded to a per-run versioned key so results
            # from every incremental and full run are captured; it must come from
            # the build invocation, not docs generate.
            dbt_s3_artifacts.upload_artifacts(
                build_invocation.target_path, ["run_results.json"], context
            )

            # manifest.json and catalog.json describe the full project and are
            # deduplicated by content hash, only uploaded when their content has
            # changed.
            docs_artifacts = ["manifest.json"]
            if (docs_target_path / "catalog.json").exists():
                docs_artifacts.append("catalog.json")
            else:
                context.log.warning(
                    "dbt docs generate did not produce catalog.json; "
                    "it will be omitted from the OpenMetadata artifact upload"
                )

            dbt_s3_artifacts.upload_artifacts(docs_target_path, docs_artifacts, context)
