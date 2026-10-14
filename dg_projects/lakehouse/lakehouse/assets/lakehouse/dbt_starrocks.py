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
    MissingBaseTablesError,
    base_table_relations,
    documented_columns,
    drifted_relations,
    live_column_query,
    live_columns,
    looks_retriable,
    retry_delay,
    unreadable_base_tables,
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


def _stale_materialized_views(
    context: AssetExecutionContext, starrocks: StarRocksResource
) -> list[str]:
    """MVs whose columns in StarRocks disagree with the dbt manifest.

    Nothing depends on this with the locked dbt-starrocks, where a plain
    build already replaces every view (see `starrocks_dbt_assets`). It is kept
    for an adapter that starts answering "no configuration changes": dbt then
    refreshes an existing view in place, an edited SELECT never lands, and
    ol-analytics-api's `build_select` turns a column it expects into an
    unknown-column error at request time.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    documented = documented_columns(manifest)
    if not documented:
        # Nothing to compare against, and `live_column_query` would build an
        # empty `IN ()` -- a syntax error. Logged rather than passed over in
        # silence: it means the schema YAML lost its `columns:`, which also
        # disables the rebuild these models depend on.
        context.log.warning(
            "No StarRocks materialized view documents any columns -- skipping "
            "the column-drift check. An edited MV SELECT will not be rebuilt."
        )
        return []
    query, params = live_column_query(documented)
    return drifted_relations(documented, live_columns(starrocks.fetch(query, params)))


def _require_base_tables(
    context: AssetExecutionContext, starrocks: StarRocksResource
) -> None:
    """Refuse to build while a base table of the MVs can't be read.

    The run fails with every view still in place. They keep the previous
    SELECT and the previous refresh's rows until a later run succeeds; the
    alternative is a dropped view and a 500 in ol-analytics-api.
    """
    manifest = json.loads(starrocks_dbt_project.manifest_path.read_text())
    relations = base_table_relations(manifest, os.environ)
    context.log.info(
        "Checking the %d base tables of the materialized views before the build",
        len(relations),
    )
    unreadable = unreadable_base_tables(relations, starrocks.fetch, log=context.log)
    if unreadable:
        raise MissingBaseTablesError(unreadable)


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

    The deployed StarRocks profiles (unlike the Trino profile used elsewhere in
    this project) have no static service-account password sitting in the pod
    environment -- credentials come from Vault's dynamic database secrets
    engine and must be generated fresh for this run. The local profiles name
    their own login and ignore the two credential variables set below. Shares the same
    `starrocks` resource (and Vault mount) as `refresh_starrocks_analytics_mvs`,
    which depends on this asset.

    Every build drops and recreates every materialized view, with or without
    --full-refresh. dbt-starrocks' configuration-changes macro
    (`starrocks__get_materialized_view_configuration_changes`) has an empty
    body, which returns whitespace and not none, so dbt's materialization
    takes its alter path for an existing view, and dbt-starrocks implements
    alter as drop then create. A CREATE that fails leaves the view dropped, so
    the views' base tables are checked before any build, see
    `_require_base_tables`. Each view is also empty from its CREATE until its
    next refresh finishes, and ol-analytics-api queries these live.

    Escalates to --full-refresh when a materialized view's columns in StarRocks
    have fallen out of step with the manifest, see `_stale_materialized_views`.
    """
    _require_base_tables(context, starrocks)
    build_args = ["build"]
    stale = _stale_materialized_views(context, starrocks)
    if stale:
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
                "credentials: %s",
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
