import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from dagster import AssetExecutionContext, AutomationCondition
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

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


class EagerAutomationTranslator(DagsterDbtTranslator):
    def get_automation_condition(
        self,
        dbt_resource_props: Mapping[str, Any],  # noqa: ARG002
    ) -> Optional[AutomationCondition]:
        return AutomationCondition.eager()


@dbt_assets(
    manifest=dbt_project.manifest_path,
    project=dbt_project,
    dagster_dbt_translator=EagerAutomationTranslator(),
)
def full_dbt_project(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
