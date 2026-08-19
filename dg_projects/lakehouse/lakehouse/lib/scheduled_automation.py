"""Which environments may register each cron schedule in this code location.

The companion to ``DBT_AUTOMATION_MAP`` in :mod:`lakehouse.lib.dbt_environment`,
covering the path that map deliberately excluded. Between them, every unattended
run this code location can start is now declared in the repo.

Why a second mechanism rather than one flag
-------------------------------------------
``DBT_AUTOMATION_MAP`` enforces itself by withholding the ``AutomationCondition``
from the assets, so a sensor someone starts by hand evaluates to nothing. A
``ScheduleDefinition`` has no equivalent inner switch: ``default_status`` only
seeds the instance's instigator state on first deploy, and any later UI toggle
wins forever. So the enforcing move here is *registration* -- a schedule left out
of ``Definitions.schedules`` is not stopped, it is absent, and nothing in the
Dagster UI can start it. Dagster synthesizes sensors it was not given
(``get_default_automation_condition_sensor``) but never schedules, so omission is
final in a way that stopping is not.

That difference is also why this is not one boolean shared with the dbt map.
Only three of these six schedules run dbt at all; the iceberg maintenance pair
rewrites Iceberg metadata, and instructor onboarding pushes a commit to a GitHub
repository. "May dbt materialize itself here" is the wrong question to ask of
those, and answering it for them would have hidden the more interesting one.

Why it is per schedule rather than per environment
--------------------------------------------------
Because the schedules disagree. ``daily_sync_and_stage_*`` is the ingestion path
QA needs *more* of -- RFC 12711 step 8 revives it -- while
``dbt_docs_artifacts_daily`` is the schedule measurably observed running from the
QA code location against the PRODUCTION warehouse (a 5.2 MB ``docs generate``
artifact, ``args.target = "production"``, 4370 nodes, filed under QA's prefix on
2026-07-01, which is where QA's OpenMetadata reads from -- so QA metadata was
describing production). A single per-environment switch would have to be wrong
about one of them.

Adding a schedule
-----------------
Add its id below. There is no fallback, and that is the whole point: a new
``ScheduleDefinition`` handed to :func:`schedules_for_environment` without an
entry raises at import, in every environment, rather than quietly inheriting a
default someone would then have to discover from instance state.
"""

from collections.abc import Iterable, Mapping

from ol_orchestrate.lib.constants import DAGSTER_ENV, VALID_DAGSTER_ENVS

# Keys are schedule ids, not necessarily schedule names: the sync_and_stage
# family is generated one-per-Airbyte-connection-group from the LIVE workspace,
# so its members' names are not knowable from the repo. Its id names the family.
#
# Every entry today is the environment set that matches observed intent, so
# registering this map is not meant to change behaviour anywhere. Its value is
# that the answer now lives in the repo: before this, whether any of these ran
# in QA was instance state nothing here could see, and one of them had been
# running against the wrong warehouse for months without leaving a trace in the
# repo that it was even allowed to.
SCHEDULE_ENVIRONMENTS: Mapping[str, frozenset[str]] = {
    # Airbyte sync plus the dbt staging models at depth=1, one schedule per
    # connection group. QA as well as production: this is ingestion, and a QA
    # lake that is never fed is the problem RFC 12711 exists to fix. Harmless in
    # dev/ci, where SKIP_AIRBYTE leaves the workspace empty and the loop that
    # builds these produces nothing to register.
    #
    # Note this reaches less far than it did before DBT_AUTOMATION_MAP: the
    # downstream handoff it was written for is severed where automation is off,
    # so a QA run of one of these builds staging and stops. Intended -- step 8
    # flips `qa` to "on" once the QA lake can actually fill the models.
    "daily_sync_and_stage": frozenset({"qa", "production"}),
    # Both rewrite Iceberg metadata -- expire snapshots, compact manifests.
    # They resolve through trino_host_map/trino_catalog_map, which have always
    # been environment-correct, so unlike the dbt schedules these were never
    # misrouted. Production-only for two different reasons: expiring snapshots
    # under a QA lake being rebuilt would fight step 8 rather than help it, and
    # `dev` maps to the PRODUCTION catalog, so a tick on a laptop would expire
    # production's snapshots. That second one is why "off in dev" is not merely
    # tidiness here.
    "iceberg_dbt_maintenance_nightly": frozenset({"production"}),
    "iceberg_raw_maintenance_nightly": frozenset({"production"}),
    # `dbt docs generate` for OpenMetadata. The one that demonstrably fired from
    # QA against production (see the module docstring). Production-only; the job
    # stays registered everywhere, so a deliberate, attended QA run is still one
    # click away, which is what step 8 needs.
    "dbt_docs_artifacts_daily": frozenset({"production"}),
    # Builds the tag:starrocks models, then refreshes their downstream MVs.
    # Always target-correct via STARROCKS_DBT_TARGET, but a QA run reads the
    # empty QA lake and publishes a B2B dashboard's worth of silently partial
    # data -- the exact failure mode RFC 12711 is about. Step 8's call to flip.
    "b2b_analytics_starrocks_nightly": frozenset({"production"}),
    # Not a data-platform schedule at all: it pushes a commit to the access
    # forge GitHub repository. There is one of those, not one per environment,
    # so a QA tick would write the real repo. This one was gated only by
    # `ScheduleDefinition`'s implicit STOPPED default -- it passed no
    # default_status at all.
    "instructor_onboarding_daily_schedule": frozenset({"production"}),
}

_UNDECLARED_ENVIRONMENTS = {
    schedule_id: sorted(environments - set(VALID_DAGSTER_ENVS))
    for schedule_id, environments in SCHEDULE_ENVIRONMENTS.items()
    if environments - set(VALID_DAGSTER_ENVS)
}
if _UNDECLARED_ENVIRONMENTS:
    # A typo'd environment name here fails open -- the schedule simply never
    # registers anywhere, which looks exactly like a deliberate decision to
    # disable it. Caught at import instead.
    msg = (
        f"SCHEDULE_ENVIRONMENTS names environments that do not exist: "
        f"{_UNDECLARED_ENVIRONMENTS}. Known environments: "
        f"{sorted(VALID_DAGSTER_ENVS)}."
    )
    raise ValueError(msg)


def schedules_for_environment[T](
    candidates: Iterable[tuple[str, T]], *, environment: str = DAGSTER_ENV
) -> list[T]:
    """Keep the schedules *environment* is declared to run, drop the rest.

    *candidates* pairs each schedule with its id in
    :data:`SCHEDULE_ENVIRONMENTS`. The id is passed rather than read off the
    schedule because the sync_and_stage family's names are generated from the
    live Airbyte workspace and so cannot be enumerated here.

    Raises KeyError for an id with no entry, which is the same no-fallback rule
    :func:`lakehouse.lib.dbt_environment.resolve_for_environment` applies to
    environments.
    """
    kept = []
    for schedule_id, schedule in candidates:
        try:
            environments = SCHEDULE_ENVIRONMENTS[schedule_id]
        except KeyError:
            msg = (
                f"No environments declared for schedule {schedule_id!r} "
                f"(known: {sorted(SCHEDULE_ENVIRONMENTS)}). Add an explicit "
                f"entry to SCHEDULE_ENVIRONMENTS -- defaulting here would let a "
                f"new schedule start itself in an environment nobody chose."
            )
            raise KeyError(msg) from None
        if environment in environments:
            kept.append(schedule)
    return kept
