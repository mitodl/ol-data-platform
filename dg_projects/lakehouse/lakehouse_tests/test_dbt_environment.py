"""Tests for per-environment dbt target and data lake resolution.

These lock down RFC 12711 step 1. The bug was not that any single value was
wrong -- it was that nothing forced the two dbt projects in this code location
to answer the same question the same way, and nothing forced an environment to
answer at all. So the assertions here are mostly about agreement and
exhaustiveness rather than about specific target names.
"""

import importlib
from pathlib import Path

import pytest
import yaml
from lakehouse.lib import dbt_environment
from lakehouse.lib.dbt_environment import (
    DATA_LAKE_ENV_MAP,
    DBT_AUTOMATION_ENVIRONMENTS,
    DBT_TARGET_MAP,
    STARROCKS_DBT_TARGET_MAP,
    STARROCKS_LOCAL_TARGETS,
    resolve_for_environment,
    starrocks_is_local,
)
from ol_dbt_cli.commands import starrocks as starrocks_cli
from ol_orchestrate.lib import constants
from ol_orchestrate.lib.constants import VALID_DAGSTER_ENVS

ENVIRONMENTS = ("dev", "ci", "qa", "production")

PROFILES_PATH = Path(__file__).parents[3] / "src/ol_dbt/profiles.yml"

ALL_MAPS = pytest.mark.parametrize(
    ("name", "value_map"),
    [
        ("DBT_TARGET_MAP", DBT_TARGET_MAP),
        ("STARROCKS_DBT_TARGET_MAP", STARROCKS_DBT_TARGET_MAP),
        ("DATA_LAKE_ENV_MAP", DATA_LAKE_ENV_MAP),
    ],
)


@ALL_MAPS
def test_every_environment_is_declared(name, value_map):
    """No environment may be reached by falling through to a default.

    `qa` was absent from the Trino map and inherited `default="production"`,
    so the QA code location built the production warehouse while the StarRocks
    project beside it targeted QA.
    """
    assert set(value_map) == set(ENVIRONMENTS), name


@ALL_MAPS
def test_unknown_environment_raises(name, value_map, monkeypatch):
    """A new environment must fail loudly rather than inherit another's."""
    monkeypatch.setattr("lakehouse.lib.dbt_environment.DAGSTER_ENV", "staging")
    with pytest.raises(KeyError, match="staging"):
        resolve_for_environment(
            value_map, override_env_var="NOT_SET_ANYWHERE", what=name
        )


# `dev` is excluded deliberately: the dev StarRocks target is the local k3d
# cluster while the dev Trino target is production. That divergence is real,
# and it is exactly why "which cluster" and "which lake" have to be separate
# axes.
DEPLOYED_ENVIRONMENTS = ("ci", "qa", "production")


@pytest.mark.parametrize("environment", DEPLOYED_ENVIRONMENTS)
def test_both_dbt_projects_agree_on_environment(environment, monkeypatch):
    """The Trino and StarRocks projects must resolve to the same environment.

    They are one Dagster code location, so `qa` meaning QA for one and
    production for the other is always a bug -- the immediate cause of the B2B
    dashboard 500s in RC. Compares which environment each target belongs to,
    not the target strings, which differ by engine.
    """
    monkeypatch.setattr("lakehouse.lib.dbt_environment.DAGSTER_ENV", environment)
    trino = resolve_for_environment(
        DBT_TARGET_MAP, override_env_var="UNSET", what="trino"
    )
    starrocks = resolve_for_environment(
        STARROCKS_DBT_TARGET_MAP, override_env_var="UNSET", what="starrocks"
    )
    assert ("qa" in trino) == ("qa" in starrocks), (
        f"{environment}: trino -> {trino}, starrocks -> {starrocks}"
    )


def test_dev_reads_the_lake_its_cluster_can_see():
    """`dev` is the local StarRocks, whose only lake catalog is the local one.

    Its target has to be a local one too: only those log in without Vault.
    """
    assert STARROCKS_DBT_TARGET_MAP["dev"] in STARROCKS_LOCAL_TARGETS
    assert DATA_LAKE_ENV_MAP["dev"] == "local"


def test_only_dev_is_local():
    """A deployed environment on a local target would log in as a bare root."""
    for environment in DEPLOYED_ENVIRONMENTS:
        assert STARROCKS_DBT_TARGET_MAP[environment] not in STARROCKS_LOCAL_TARGETS
        assert DATA_LAKE_ENV_MAP[environment] != "local"


def test_local_targets_are_declared_profiles():
    """A target missing from profiles.yml only fails when dbt parses."""
    profiles = yaml.safe_load(PROFILES_PATH.read_text())
    assert set(profiles["open_learning"]["outputs"]) >= STARROCKS_LOCAL_TARGETS


def test_local_targets_match_the_cli():
    """The CLI keeps its own copy of the set for the same guard."""
    assert starrocks_cli._LOCAL_DBT_TARGETS == STARROCKS_LOCAL_TARGETS


def test_local_profiles_take_no_credentials_from_the_environment():
    """Dagster exports Vault credentials as DBT_STARROCKS_*; these ignore them."""
    outputs = yaml.safe_load(PROFILES_PATH.read_text())["open_learning"]["outputs"]
    for target in STARROCKS_LOCAL_TARGETS:
        assert (outputs[target]["username"], outputs[target]["password"]) == (
            "root",
            "",
        )


@pytest.mark.parametrize("environment", ENVIRONMENTS)
def test_every_environment_s_own_target_passes_the_login_guard(environment):
    target = STARROCKS_DBT_TARGET_MAP[environment]
    assert starrocks_is_local(target, environment) is (environment == "dev")


@pytest.mark.parametrize(
    ("target", "environment"),
    [
        ("starrocks_local", "production"),
        ("starrocks_local_b2b", "qa"),
        ("starrocks_local_b2b", "ci"),
        ("starrocks_qa_vault", "dev"),
        ("starrocks_production", "dev"),
    ],
)
def test_an_override_cannot_cross_between_local_and_vault(target, environment):
    """The host follows the environment and the login follows the target.

    A local target under a deployed environment would send a bare root login to
    that environment's FE; a Vault target under dev would send QA credentials
    to the local port.
    """
    with pytest.raises(ValueError, match=target):
        starrocks_is_local(target, environment)


def test_data_lake_env_values_are_real_catalogs():
    """Values are interpolated into `ol_data_lake_<env>`, so typos are silent."""
    assert set(DATA_LAKE_ENV_MAP.values()) <= {"local", "qa", "production"}


@ALL_MAPS
def test_override_env_var_wins(name, value_map, monkeypatch):
    monkeypatch.setenv("DAGSTER_DBT_OVERRIDE_UNDER_TEST", "an_explicit_override")
    assert (
        resolve_for_environment(
            value_map,
            override_env_var="DAGSTER_DBT_OVERRIDE_UNDER_TEST",
            what=name,
        )
        == "an_explicit_override"
    )


def test_automation_environments_exist():
    """A typo here fails in the expensive direction.

    Opt-in means an unrecognized name reads as "automation off everywhere", so
    production would quietly stop materializing and nothing downstream would
    report it -- the assets would just stop carrying a condition. The module
    raises at import; this asserts the shipped set is clean.
    """
    assert set(VALID_DAGSTER_ENVS) >= DBT_AUTOMATION_ENVIRONMENTS


def test_only_production_automates():
    """No environment builds unattended against a warehouse it isn't for.

    `qa` in particular: with DBT_TARGET_MAP missing a `qa` entry, every dbt run
    from the QA code location hit the production warehouse -- all 18 pre-fix
    run_results.json objects in s3://dagster-data-qa/ read `"target":
    "production"`. Step 1 fixed where a QA build lands; this fixes whether one
    starts unasked. Adding `qa` belongs to RFC 12711 step 8, once the
    QA lake can actually fill the models.
    """
    assert frozenset({"production"}) == DBT_AUTOMATION_ENVIRONMENTS


@pytest.mark.parametrize(
    ("environment", "expected"),
    [(env, env == "production") for env in VALID_DAGSTER_ENVS],
)
def test_the_boolean_the_assets_read_follows_the_declaration(
    environment, expected, monkeypatch
):
    """The gate has to survive someone starting the sensor in the UI.

    `default_status` seeds the instance state once and is overridden forever
    after by a manual toggle -- exactly the invisible instance setting this
    replaces. So the enforcing half is that an environment outside the
    declaration produces assets with NO AutomationCondition, which a
    hand-started sensor evaluates to nothing. This asserts the boolean those
    assets read; the translator itself cannot be imported here without a parsed
    dbt manifest.

    Named environments rather than whichever one the test process happens to be
    running as. DBT_AUTOMATION_ENABLED is computed at import, so asserting it
    bare passes or fails on the developer's shell -- with
    DAGSTER_ENVIRONMENT=production set, this file used to fail here and nowhere
    else.

    Setting the variable is not enough on its own to move it: DAGSTER_ENV is
    resolved when `ol_orchestrate.lib.constants` is imported, so that module has
    to be reloaded before this one or the reload silently re-reads the old
    value. `monkeypatch.undo()` puts the real environment back before the
    restoring reload, so the module is left as the rest of the suite found it
    whatever the shell was set to.
    """
    monkeypatch.setenv("DAGSTER_ENVIRONMENT", environment)
    importlib.reload(constants)
    importlib.reload(dbt_environment)
    try:
        assert dbt_environment.DBT_AUTOMATION_ENABLED is expected
    finally:
        monkeypatch.undo()
        importlib.reload(constants)
        importlib.reload(dbt_environment)


def test_a_new_environment_does_not_automate_by_default(monkeypatch):
    """The whole argument for opt-in, asserted rather than assumed.

    An environment nobody has thought about yet must not materialize dbt models
    unattended. That is the safe direction, which is why this axis does not
    borrow the no-fallback rule the target maps need -- there, a missing entry
    would mean writing some other environment's warehouse.
    """
    monkeypatch.setattr(dbt_environment, "DAGSTER_ENV", "staging")
    assert "staging" not in dbt_environment.DBT_AUTOMATION_ENVIRONMENTS
