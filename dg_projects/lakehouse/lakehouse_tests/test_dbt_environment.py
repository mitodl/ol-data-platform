"""Tests for per-environment dbt target and data lake resolution.

These lock down RFC 12711 step 1. The bug was not that any single value was
wrong -- it was that nothing forced the two dbt projects in this code location
to answer the same question the same way, and nothing forced an environment to
answer at all. So the assertions here are mostly about agreement and
exhaustiveness rather than about specific target names.
"""

import pytest
from lakehouse.lib import dbt_environment
from lakehouse.lib.dbt_environment import (
    DATA_LAKE_ENV_MAP,
    DBT_AUTOMATION_ENVIRONMENTS,
    DBT_TARGET_MAP,
    STARROCKS_DBT_TARGET_MAP,
    resolve_for_environment,
)
from ol_orchestrate.lib.constants import VALID_DAGSTER_ENVS

ENVIRONMENTS = ("dev", "ci", "qa", "production")

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


# `dev` is excluded deliberately: a laptop cannot reach the production
# StarRocks FE (an in-cluster service), so the dev StarRocks target
# port-forwards to the QA cluster while the dev Trino target is production.
# That divergence is real, and it is exactly why "which cluster" and "which
# lake" have to be separate axes -- dev's lake is pinned to production by
# DATA_LAKE_ENV_MAP regardless of the cluster it connects to.
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


def test_dev_reads_production_lake():
    """`dev` connects to the QA StarRocks cluster but must READ production.

    This is the case `'qa' in target.name` got wrong: the dev target is
    `starrocks_qa_vault`, so the substring test sent local b2b builds at the
    empty QA lake and the models could not be developed locally at all.
    """
    assert STARROCKS_DBT_TARGET_MAP["dev"] == "starrocks_qa_vault"
    assert DATA_LAKE_ENV_MAP["dev"] == "production"


def test_data_lake_env_values_are_real_catalogs():
    """Values are interpolated into `ol_data_lake_<env>`, so typos are silent."""
    assert set(DATA_LAKE_ENV_MAP.values()) <= {"qa", "production"}


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
    starts unasked. Flipping `qa` to "on" belongs to RFC 12711 step 8, once the
    QA lake can actually fill the models.
    """
    assert frozenset({"production"}) == DBT_AUTOMATION_ENVIRONMENTS


def test_automation_is_off_in_this_test_environment():
    """The gate has to survive someone starting the sensor in the UI.

    `default_status` seeds the instance state once and is overridden forever
    after by a manual toggle -- exactly the invisible instance setting this
    replaces. So the enforcing half is that an automation-off environment
    produces assets with NO AutomationCondition, which a hand-started sensor
    evaluates to nothing. This asserts the boolean those assets read; the
    translator itself cannot be imported here without a parsed dbt manifest.
    """
    assert dbt_environment.DBT_AUTOMATION_ENABLED is False


def test_a_new_environment_does_not_automate_by_default(monkeypatch):
    """The whole argument for opt-in, asserted rather than assumed.

    An environment nobody has thought about yet must not materialize dbt models
    unattended. That is the safe direction, which is why this axis does not
    borrow the no-fallback rule the target maps need -- there, a missing entry
    would mean writing some other environment's warehouse.
    """
    monkeypatch.setattr(dbt_environment, "DAGSTER_ENV", "staging")
    assert "staging" not in dbt_environment.DBT_AUTOMATION_ENVIRONMENTS
