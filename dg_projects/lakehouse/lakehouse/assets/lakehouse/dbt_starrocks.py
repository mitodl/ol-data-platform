import os

from dagster import AssetExecutionContext
from dagster_dbt import (
    DagsterDbtTranslator,
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV

from lakehouse.assets.lakehouse.dbt import DBT_REPO_DIR
from lakehouse.resources.starrocks import StarRocksResource

# b2b_analytics models are gated `+enabled: "{{ target.type == 'starrocks' }}"` in
# dbt_project.yml, so they only exist in a manifest parsed against one of these
# targets -- full_dbt_project's manifest is always parsed against a Trino target
# and never sees them. Matches the dbt_target choices in
# src/ol_dbt_cli/ol_dbt_cli/commands/starrocks.py's _ENVS map.
STARROCKS_DBT_TARGET_MAP = {
    "dev": "starrocks_qa_vault",
    "ci": "starrocks_qa_vault",
    "qa": "starrocks_qa_vault",
    "production": "starrocks_production",
}

# `prepare_if_dev()` below only ever parses (never opens a DB connection), but
# profiles.yml's env_var() calls for the starrocks targets have no defaults and
# raise immediately if unset. Default them for a bare `dagster dev` so
# import doesn't fail for developers who haven't run `ol-dbt starrocks` (which
# sets real values) -- mirrors the dummy build-time credentials used in
# the Dockerfile's manifest-generation step.
os.environ.setdefault("DBT_STARROCKS_HOST", "localhost")
os.environ.setdefault("DBT_STARROCKS_USERNAME", "dev")
os.environ.setdefault("DBT_STARROCKS_PASSWORD", "dev")

# Separate target-path so this manifest doesn't collide with full_dbt_project's
# manifest at the default "target/" (both dbt projects share the same project_dir).
starrocks_dbt_project = DbtProject(
    project_dir=DBT_REPO_DIR,
    target=os.environ.get(
        "DAGSTER_DBT_STARROCKS_TARGET", STARROCKS_DBT_TARGET_MAP[DAGSTER_ENV]
    ),
    target_path="target/starrocks",
)
starrocks_dbt_project.prepare_if_dev()


@dbt_assets(
    manifest=starrocks_dbt_project.manifest_path,
    project=starrocks_dbt_project,
    select="b2b_analytics",
    dagster_dbt_translator=DagsterDbtTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
)
def b2b_analytics_starrocks_dbt_assets(
    context: AssetExecutionContext,
    dbt: DbtCliResource,
    starrocks: StarRocksResource,
):
    """Build the b2b_analytics dbt models directly against StarRocks.

    The StarRocks profile (unlike the Trino profile used elsewhere in this
    project) has no static service-account password sitting in the pod
    environment -- credentials come from Vault's dynamic database secrets
    engine and must be generated fresh for this run. Shares the same
    `starrocks` resource (and Vault mount) as `refresh_starrocks_analytics_mvs`,
    which depends on this asset.
    """
    username, password = starrocks.generate_credentials()
    os.environ["DBT_STARROCKS_USERNAME"] = username
    os.environ["DBT_STARROCKS_PASSWORD"] = password
    os.environ["DBT_STARROCKS_HOST"] = starrocks.host

    build_invocation = dbt.cli(["build"], context=context)
    yield from build_invocation.stream()
