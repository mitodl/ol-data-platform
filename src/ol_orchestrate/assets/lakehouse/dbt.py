import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from dagster import AssetExecutionContext, AutomationCondition
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)

from ol_orchestrate.lib.constants import DAGSTER_ENV

DBT_REPO_DIR = (
    Path(__file__).parent.parent.parent.parent.joinpath("ol_dbt")
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
    ) -> Optional[AutomationCondition]:
        no_upstream_dependencies_in_process = (
            ~AutomationCondition.any_deps_in_progress()
        )
        has_upstream_changes = AutomationCondition.any_deps_updated()
        has_code_changes = AutomationCondition.code_version_changed()
        return no_upstream_dependencies_in_process & (
            has_upstream_changes | has_code_changes
        )


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=DbtAutomationTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
)
def full_dbt_project(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from (
        dbt.cli(["build"], context=context)
        .stream()
        .fetch_column_metadata()
        .fetch_row_counts()
    )
