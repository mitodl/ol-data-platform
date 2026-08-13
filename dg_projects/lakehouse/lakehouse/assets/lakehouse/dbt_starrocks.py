import json
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
)
from lakehouse.lib.dbt_environment import STARROCKS_DBT_TARGET
from lakehouse.lib.starrocks_dbt import (
    MAX_BUILD_ATTEMPTS,
    documented_columns,
    drifted_relations,
    live_column_query,
    live_columns,
    looks_retriable,
    retry_delay,
)
from lakehouse.resources.starrocks import StarRocksResource

# tag:starrocks models (see dbt_project.yml) are additionally gated
# `+enabled: "{{ target.type == 'starrocks' }}"`, so they only exist in a
# manifest parsed against a StarRocks target -- full_dbt_project's manifest is
# always parsed against a Trino target and never sees them. Migrating an
# existing model onto StarRocks means tagging it here (dbt_project.yml or
# model-level config) and giving it a matching +enabled condition -- this asset
# set and full_dbt_project's exclude="tag:starrocks" then pick it up
# automatically, no Python change needed.
#
# The per-environment target map lives in lakehouse.lib.dbt_environment
# alongside the Trino one, so the two cannot drift apart the way they did
# before RFC 12711 step 1.

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
    target=STARROCKS_DBT_TARGET,
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

# Serializes credential-injection + dbt subprocess spawn so two b2b_analytics
# builds materializing in the same process (e.g. a manual run overlapping the
# schedule) can't clobber each other's DBT_STARROCKS_* env vars between the
# assignment below and the subprocess spawn -- NOT held across the actual
# build (dbt.cli() spawns the subprocess synchronously and it inherits
# os.environ at that point; nothing after that call can still race).
_ENV_LOCK = threading.Lock()

# Retry knobs, the retriable-error classifier, and the column-drift comparison
# live in lakehouse.lib so they can be unit-tested without a parsed dbt manifest
# on disk (importing this module evaluates the @dbt_assets decorator below,
# which needs one).


def _stale_materialized_views(starrocks: StarRocksResource) -> list[str]:
    """MVs whose columns in StarRocks disagree with the dbt manifest.

    dbt cannot find these itself. dbt-core only replaces an existing
    materialized view under --full-refresh, and asks the adapter for
    configuration changes otherwise -- but dbt-starrocks'
    `starrocks__get_materialized_view_configuration_changes` returns nothing,
    so a plain build logs "no configuration changes were identified" and leaves
    the old SELECT in place. Green build, green refresh, change never landed.

    That made shipping a column a two-step release with a hand-run
    `dbt run --full-refresh --select b2b_analytics` in the middle, and nothing
    but memory enforcing the order -- while ol-analytics-api's `build_select`
    projects each model's own field list, so deploying the consumer first turns
    the miss into an unknown-column error at request time.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    documented = documented_columns(manifest)
    query, params = live_column_query(documented)
    return drifted_relations(documented, live_columns(starrocks.fetch(query, params)))


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

    Escalates to --full-refresh when a materialized view's columns in StarRocks
    have fallen behind the manifest, since a plain build would not notice --
    see `_stale_materialized_views`.
    """
    build_args = ["build"]
    stale = _stale_materialized_views(starrocks)
    if stale:
        # --full-refresh drops and recreates every selected MV, not just the
        # drifted ones, which is why it is conditional: each recreated view is
        # briefly absent, and ol-analytics-api queries these live.
        context.log.info(
            "Materialized views whose columns differ from the dbt manifest -- "
            "building with --full-refresh so the new SELECT actually lands: %s",
            ", ".join(stale),
        )
        build_args.append("--full-refresh")

    last_exc: DagsterDbtCliRuntimeError | None = None
    for attempt in range(MAX_BUILD_ATTEMPTS):
        if attempt:
            delay = retry_delay(attempt)
            context.log.warning(
                "dbt build failed (attempt %d/%d) -- retrying in %ds with fresh "
                "Vault credentials: %s",
                attempt,
                MAX_BUILD_ATTEMPTS,
                delay,
                last_exc,
            )
            time.sleep(delay)

        username, password = starrocks.generate_credentials()
        with _ENV_LOCK:
            os.environ["DBT_STARROCKS_USERNAME"] = username
            os.environ["DBT_STARROCKS_PASSWORD"] = password
            os.environ["DBT_STARROCKS_HOST"] = starrocks.host
            invocation = starrocks_dbt.cli(build_args, context=context)

        # The subprocess above is already spawned (and has already inherited
        # the env set under the lock) by the time .cli() returns -- streaming
        # its output takes minutes and must happen outside the lock, or a
        # second concurrent build would be blocked from even starting until
        # this one finishes instead of just for the moment of env injection.
        try:
            events = list(invocation.stream())
        except DagsterDbtCliRuntimeError as exc:
            if attempt == MAX_BUILD_ATTEMPTS - 1 or not looks_retriable(exc):
                raise
            last_exc = exc
            continue

        yield from events
        return
