"""Tests for the per-instigator environment declaration.

Same shape as lakehouse_tests/test_scheduled_automation.py, for the same
reason: what is locked down is not any one instigator's environment set -- those
are judgement calls the Cohort 2 enable will revisit -- but that every one has
to state a set, and that stating it is what decides whether Dagster ever sees
the instigator at all.
"""

import importlib
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from dagster import AssetKey, DefaultScheduleStatus, DefaultSensorStatus
from delivery.lib.scheduled_automation import (
    INSTIGATOR_ENVIRONMENTS,
    instigators_for_environment,
)
from ol_orchestrate.lib.constants import VALID_DAGSTER_ENVS

# Packages whose module-level code reads DAGSTER_ENVIRONMENT, so a build under
# a different environment has to re-import them.
_ENVIRONMENT_SENSITIVE_ROOTS = frozenset({"delivery", "ol_orchestrate"})

FAILURE_NOTIFICATION_SENSOR_NAMES = frozenset(
    {"run_failure_notification_sensor", "asset_check_failure_sensor"}
)


@dataclass(frozen=True)
class _Stub:
    """Stands in for a ScheduleDefinition/SensorDefinition.

    Nothing in instigators_for_environment touches anything but ``.name``, so
    these tests need neither Vault nor a loaded code location.
    """

    name: str


ALL_CANDIDATES = [_Stub(name) for name in INSTIGATOR_ENVIRONMENTS]


def test_every_declared_environment_exists():
    """A typo'd environment fails open -- the instigator registers nowhere.

    Which is indistinguishable from a deliberate decision to disable it, so the
    module raises at import. This asserts the shipped map is clean.
    """
    for name, environments in INSTIGATOR_ENVIRONMENTS.items():
        assert environments <= set(VALID_DAGSTER_ENVS), name


def test_undeclared_instigator_raises():
    """The no-fallback rule.

    A new schedule or sensor wired into Definitions without an entry must fail
    the code location's import in every environment, rather than picking up a
    default nobody chose and then only being visible as instance state.
    """
    with pytest.raises(KeyError, match="brand_new_sensor"):
        instigators_for_environment(
            [_Stub("brand_new_sensor")], environment="production"
        )


@pytest.mark.parametrize("environment", VALID_DAGSTER_ENVS)
def test_filter_keeps_exactly_what_is_declared(environment):
    kept = {
        i.name
        for i in instigators_for_environment(ALL_CANDIDATES, environment=environment)
    }
    expected = {
        name
        for name, environments in INSTIGATOR_ENVIRONMENTS.items()
        if environment in environments
    }
    assert kept == expected


def test_nothing_registers_outside_production():
    """The regression this module closes.

    default_status=RUNNING is unconditional, so before the registration gate a
    fresh QA or local instance seeded all five moved instigators RUNNING --
    including the delete-webhook sensor below. Absent beats stopped: a UI toggle
    can undo a status, it cannot conjure an unregistered instigator.
    """
    for environment in ("dev", "ci", "qa"):
        assert (
            instigators_for_environment(ALL_CANDIDATES, environment=environment) == []
        )


def test_destructive_sensor_is_production_only():
    """Named explicitly so widening it has to be a deliberate edit to this test.

    ovs_videos_stale_cleanup_sensor dispatches delete webhooks to MIT Learn.
    """
    assert INSTIGATOR_ENVIRONMENTS["ovs_videos_stale_cleanup_sensor"] == frozenset(
        {"production"}
    )


def test_instructor_onboarding_is_production_only():
    """There is one access forge repository, not one per environment.

    A tick anywhere else would commit to the real one's default branch.
    """
    assert INSTIGATOR_ENVIRONMENTS["instructor_onboarding_daily_schedule"] == frozenset(
        {"production"}
    )


def test_every_registered_instigator_is_declared():
    """Guards the pairing between Definitions and this map.

    Catches a schedule or sensor added to Definitions but left out of the map --
    which would otherwise only surface as an import failure at deploy time.
    """
    from delivery import definitions  # noqa: PLC0415

    registered = {s.name for s in definitions.defs.schedules or []} | {
        s.name for s in definitions.defs.sensors or []
    }
    assert registered <= set(INSTIGATOR_ENVIRONMENTS)


@contextmanager
def _repository_for(environment: str) -> Iterator[Any]:
    """Build the code location as the gRPC server would under ``environment``.

    ``DAGSTER_ENV`` is resolved once, at import of
    ``ol_orchestrate.lib.constants``, and ``instigators_for_environment``
    defaults to it, so the gate in ``delivery.definitions`` can only be
    exercised by re-importing the tree with the variable set. Same pattern as
    data_loading_tests/test_definitions.py.
    """
    saved_modules = {
        name: module
        for name, module in sys.modules.items()
        if name.split(".")[0] in _ENVIRONMENT_SENSITIVE_ROOTS
    }
    saved_environment = os.environ.get("DAGSTER_ENVIRONMENT")

    def _purge() -> None:
        for name in list(sys.modules):
            if name.split(".")[0] in _ENVIRONMENT_SENSITIVE_ROOTS:
                del sys.modules[name]

    os.environ["DAGSTER_ENVIRONMENT"] = environment
    _purge()
    try:
        module = importlib.import_module("delivery.definitions")
        yield module.defs.get_repository_def()
    finally:
        _purge()
        sys.modules.update(saved_modules)
        if saved_environment is None:
            os.environ.pop("DAGSTER_ENVIRONMENT", None)
        else:
            os.environ["DAGSTER_ENVIRONMENT"] = saved_environment


@pytest.mark.parametrize("environment", VALID_DAGSTER_ENVS)
def test_failure_notification_sensors_register_in_production_only(environment):
    """Read off the built repository, not the gate, so dropping them fails here.

    Losing them in production silences failure alerting for the whole
    deployment, and they declare default_status=RUNNING, so a registration
    anywhere else would start them there too.
    """
    with _repository_for(environment) as repo:
        sensors = {sensor.name: sensor for sensor in repo.sensor_defs}

        registered = set(sensors) & FAILURE_NOTIFICATION_SENSOR_NAMES
        if environment == "production":
            assert registered == FAILURE_NOTIFICATION_SENSOR_NAMES
            assert all(
                sensors[name].default_status == DefaultSensorStatus.RUNNING
                for name in registered
            )
        else:
            assert registered == set()


@pytest.mark.parametrize("environment", VALID_DAGSTER_ENVS)
def test_instructor_onboarding_schedule_registers_running_in_production_only(
    environment,
):
    """Read off the built repository, so the wiring and status are both pinned.

    It was RUNNING in lakehouse, and instigator state is keyed on location name,
    so dropping default_status would stop it on the move. Registering it
    anywhere else would commit to the real access-forge repository.
    """
    with _repository_for(environment) as repo:
        schedules = {schedule.name: schedule for schedule in repo.schedule_defs}

        if environment == "production":
            assert (
                schedules["instructor_onboarding_daily_schedule"].default_status
                == DefaultScheduleStatus.RUNNING
            )
        else:
            assert "instructor_onboarding_daily_schedule" not in schedules


WEBHOOK_DELIVERY_SENSOR_NAMES = frozenset(
    {
        "mit_climate_delivery_sensor",
        "mitpe_delivery_sensor",
        "oll_delivery_sensor",
        "mit_edx_programs_delivery_sensor",
    }
)

DBT_MODELS_DIR = Path(__file__).parents[3] / "src" / "ol_dbt" / "models"


def _webhook_nodes(repo: Any) -> dict[AssetKey, Any]:
    graph = repo.asset_graph
    return {
        key: graph.get(key)
        for key in graph.get_all_asset_keys()
        if key.path[0] == "mit_learn_delivery"
    }


@pytest.mark.parametrize("environment", VALID_DAGSTER_ENVS)
def test_webhooks_are_conditioned_only_where_their_sensor_registers(environment):
    """A conditioned webhook with no declared sensor is not inert.

    Dagster sweeps it into the synthesized default_automation_condition_sensor,
    which registers in every environment. So outside production the webhooks
    must carry no condition at all, and in production each must be targeted by
    its own sensor rather than the default one.
    """
    with _repository_for(environment) as repo:
        webhooks = _webhook_nodes(repo)
        sensors = {sensor.name: sensor for sensor in repo.sensor_defs}
        assert len(webhooks) == len(WEBHOOK_DELIVERY_SENSOR_NAMES)

        if environment != "production":
            assert set(sensors) & WEBHOOK_DELIVERY_SENSOR_NAMES == set()
            assert all(node.automation_condition is None for node in webhooks.values())
            return

        targeted = set()
        for name in WEBHOOK_DELIVERY_SENSOR_NAMES:
            sensor = sensors[name]
            assert sensor.default_status == DefaultSensorStatus.STOPPED
            targeted |= sensor.asset_selection.resolve(repo.asset_graph)
        assert targeted == set(webhooks)
        assert all(node.automation_condition is not None for node in webhooks.values())

        default = sensors.get("default_automation_condition_sensor")
        if default is not None:
            assert not default.asset_selection.resolve(repo.asset_graph) & targeted


def test_webhook_deps_are_keys_lakehouse_emits():
    """The deps are what on_cron waits on, so a wrong key never fires.

    dagster-dbt keys a model with a configured schema as [schema, model name],
    and the integrations layer sets ``+schema: integrations``. The deps were
    written as ["integrations", "learn", model], which no code location
    defines, so they would have been treated as never updated.
    """
    with _repository_for("production") as repo:
        for key, node in _webhook_nodes(repo).items():
            assert node.parent_keys, key
            for parent in node.parent_keys:
                schema, model = parent.path
                assert schema == "integrations", (key, parent)
                assert list(DBT_MODELS_DIR.glob(f"integrations/**/{model}.sql")), (
                    key,
                    parent,
                )
