"""Tests for the per-schedule environment declaration.

The companion to test_dbt_environment.py, and the assertions have the same
shape for the same reason: what is being locked down is not any one schedule's
environment set -- those are judgement calls RFC 12711 step 8 will revisit --
but that every schedule has to state one, and that stating it is what decides
whether Dagster ever sees the schedule at all.
"""

import pytest
from lakehouse.lib.scheduled_automation import (
    SCHEDULE_ENVIRONMENTS,
    schedules_for_environment,
)
from ol_orchestrate.lib.constants import VALID_DAGSTER_ENVS

# The tuples are (id, schedule); nothing in schedules_for_environment inspects
# the second element, so a string stands in for a ScheduleDefinition and these
# tests need neither a dbt manifest nor a live Airbyte workspace.
ALL_CANDIDATES = [(schedule_id, schedule_id) for schedule_id in SCHEDULE_ENVIRONMENTS]


def test_every_declared_environment_exists():
    """A typo'd environment fails open -- the schedule never registers anywhere.

    Which is indistinguishable from a deliberate decision to disable it, so the
    module raises at import. This asserts the shipped map is clean.
    """
    for schedule_id, environments in SCHEDULE_ENVIRONMENTS.items():
        assert environments <= set(VALID_DAGSTER_ENVS), schedule_id


def test_undeclared_schedule_raises():
    """The no-fallback rule, one layer over from resolve_for_environment.

    A new ScheduleDefinition wired into Definitions without an entry must fail
    the code location's import in every environment, rather than picking up a
    default nobody chose and then only being visible as instance state.
    """
    with pytest.raises(KeyError, match="brand_new_nightly"):
        schedules_for_environment(
            [("brand_new_nightly", object())], environment="production"
        )


@pytest.mark.parametrize("environment", VALID_DAGSTER_ENVS)
def test_filter_keeps_exactly_what_is_declared(environment):
    kept = schedules_for_environment(ALL_CANDIDATES, environment=environment)
    expected = [
        schedule_id
        for schedule_id, environments in SCHEDULE_ENVIRONMENTS.items()
        if environment in environments
    ]
    assert kept == expected


def test_dev_and_ci_register_no_schedules():
    """Neither reaches a warehouse worth writing on a timer.

    `dev` is a laptop and `ci` is ephemeral; a cron tick in either would be a
    surprise, and for the schedules that push to GitHub or rewrite Iceberg
    metadata it would be a surprise with external side effects.
    """
    for environment in ("dev", "ci"):
        assert schedules_for_environment(ALL_CANDIDATES, environment=environment) == []


def test_only_ingestion_is_allowed_in_qa():
    """QA gets fed, but does not build or publish on its own.

    The distinction RFC 12711 turns on: a QA build of a union model whose
    branches are missing emits a partial result that looks like working data.
    Ingestion has no such failure mode -- more of it is strictly better -- so
    the sync_and_stage family runs in QA while the dbt schedules do not.
    """
    assert schedules_for_environment(ALL_CANDIDATES, environment="qa") == [
        "daily_sync_and_stage"
    ]


def test_qa_cannot_run_dbt_docs_generate_on_a_timer():
    """The nearest thing to a measured incident, and what it does not show.

    A 2026-07-01 `docs generate` ran from the QA code location against the
    production warehouse and filed production's catalog under QA's artifacts
    prefix, which QA's OpenMetadata ingests. It was a launch of the JOB, not a
    tick of this schedule -- the hours fit no daily cron. #2508 fixed the
    target it ran against; this only stops the timer, which is the half that
    could have run unattended.
    """
    assert "qa" not in SCHEDULE_ENVIRONMENTS["dbt_docs_artifacts_daily"]


def test_instructor_onboarding_is_production_only():
    """There is one access forge repository, not one per environment.

    So a tick outside production would push a commit to the real one. This
    schedule passed no default_status at all, relying on ScheduleDefinition's
    implicit STOPPED -- the weakest form of the gate this replaces.
    """
    assert SCHEDULE_ENVIRONMENTS["instructor_onboarding_daily_schedule"] == frozenset(
        {"production"}
    )


def test_production_registers_every_schedule():
    """Nothing is dropped where the map is meant to be a no-op.

    This change is a declaration, not a behaviour change: production's set is
    what it registered before, so a green deploy there proves the mechanism
    without proving anything about the environments it now excludes.
    """
    kept = schedules_for_environment(ALL_CANDIDATES, environment="production")
    assert kept == list(SCHEDULE_ENVIRONMENTS)


def test_family_id_covers_every_generated_member():
    """The sync_and_stage schedules are named from the live Airbyte workspace.

    How many exist in a given environment depends on that workspace, not on
    this repo, so they cannot be declared by name. One family id decides all of
    them, and the filter must not care how many there are.
    """
    generated = [
        ("daily_sync_and_stage", f"daily_sync_and_stage_{i}") for i in range(5)
    ]
    assert len(schedules_for_environment(generated, environment="qa")) == len(generated)
    assert schedules_for_environment(generated, environment="dev") == []
