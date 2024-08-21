import os
from pathlib import Path

from dagster import AssetExecutionContext
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

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


@dbt_assets(manifest=dbt_project.manifest_path)
def full_dbt_project(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
