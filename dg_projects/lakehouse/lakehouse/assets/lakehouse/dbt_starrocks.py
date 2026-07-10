import os
import threading
import time

from dagster import AssetExecutionContext
from dagster_dbt import (
    DagsterDbtTranslatorSettings,
    DbtCliResource,
    DbtProject,
    dbt_assets,
)
from dagster_dbt.errors import DagsterDbtCliRuntimeError

from lakehouse.assets.lakehouse.dbt import (
    DBT_REPO_DIR,
    DbtAutomationTranslator,
    resolve_dbt_target,
)
from lakehouse.resources.starrocks import StarRocksResource

# tag:starrocks models (see dbt_project.yml) are additionally gated
# `+enabled: "{{ target.type == 'starrocks' }}"`, so they only exist in a
# manifest parsed against one of these targets -- full_dbt_project's manifest
# is always parsed against a Trino target and never sees them. Matches the
# dbt_target choices in src/ol_dbt_cli/ol_dbt_cli/commands/starrocks.py's
# _ENVS map. Migrating an existing model onto StarRocks means tagging it here
# (dbt_project.yml or model-level config) and giving it a matching +enabled
# condition -- this asset set and full_dbt_project's exclude="tag:starrocks"
# then pick it up automatically, no Python change needed.
STARROCKS_DBT_TARGET_MAP = {
    "dev": "starrocks_qa_vault",
    # ci connects directly to its own FE service (no port-forward), same
    # connection shape as production -- matches _ENVS["ci"]["dbt_target"].
    "ci": "starrocks_production",
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
    target=resolve_dbt_target(
        STARROCKS_DBT_TARGET_MAP,
        override_env_var="DAGSTER_DBT_STARROCKS_TARGET",
        default="starrocks_production",
    ),
    target_path="target/starrocks",
)
starrocks_dbt_project.prepare_if_dev()

# Passing a DbtProject to project_dir makes DbtCliResource pick up its target/
# profiles_dir automatically (see dagster_dbt.core.resource.DbtCliResource).
# This must be a SEPARATE resource from the shared "dbt" key used by
# full_dbt_project, which is pinned to a Trino target -- reusing that one here
# would silently run `dbt build --target production` and build nothing, since
# b2b_analytics is disabled for any non-starrocks target.
starrocks_dbt_cli = DbtCliResource(project_dir=starrocks_dbt_project)

# Serializes credential-injection + dbt invocation so two b2b_analytics builds
# materializing in the same process (e.g. a manual run overlapping the
# schedule) can't clobber each other's DBT_STARROCKS_* env vars between the
# assignment below and dagster_dbt's env snapshot at subprocess spawn.
_ENV_LOCK = threading.Lock()

_MAX_BUILD_ATTEMPTS = 3
_RETRY_BASE_DELAY = 1  # seconds; doubles each attempt
# Same MySQL-wire-protocol error signatures StarRocksResource.execute() retries
# on: a freshly-generated Vault user may not yet be visible on the FE node dbt
# connects to. dbt build has no adapter-level retry of its own, so without this
# a replication-lag race fails the whole build instead of a single statement.
_RETRIABLE_ERROR_MARKERS = ("1044", "1045", "2006", "2013")


def _looks_retriable(exc: Exception) -> bool:
    return any(marker in str(exc) for marker in _RETRIABLE_ERROR_MARKERS)


@dbt_assets(
    manifest=starrocks_dbt_project.manifest_path,
    project=starrocks_dbt_project,
    # Complementary partition with full_dbt_project's exclude="tag:starrocks".
    select="tag:starrocks",
    dagster_dbt_translator=DbtAutomationTranslator(
        settings=DagsterDbtTranslatorSettings(enable_code_references=True)
    ),
)
def starrocks_dbt_assets(
    context: AssetExecutionContext,
    starrocks_dbt: DbtCliResource,
    starrocks: StarRocksResource,
):
    """Build the tag:starrocks dbt models directly against StarRocks.

    The StarRocks profile (unlike the Trino profile used elsewhere in this
    project) has no static service-account password sitting in the pod
    environment -- credentials come from Vault's dynamic database secrets
    engine and must be generated fresh for this run. Shares the same
    `starrocks` resource (and Vault mount) as `refresh_starrocks_analytics_mvs`,
    which depends on this asset.
    """
    last_exc: DagsterDbtCliRuntimeError | None = None
    for attempt in range(_MAX_BUILD_ATTEMPTS):
        if attempt:
            delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
            context.log.warning(
                "dbt build failed (attempt %d/%d) -- retrying in %ds with fresh "
                "Vault credentials: %s",
                attempt,
                _MAX_BUILD_ATTEMPTS,
                delay,
                last_exc,
            )
            time.sleep(delay)

        username, password = starrocks.generate_credentials()
        with _ENV_LOCK:
            os.environ["DBT_STARROCKS_USERNAME"] = username
            os.environ["DBT_STARROCKS_PASSWORD"] = password
            os.environ["DBT_STARROCKS_HOST"] = starrocks.host
            try:
                events = list(starrocks_dbt.cli(["build"], context=context).stream())
            except DagsterDbtCliRuntimeError as exc:
                if attempt == _MAX_BUILD_ATTEMPTS - 1 or not _looks_retriable(exc):
                    raise
                last_exc = exc
                continue

        yield from events
        return
