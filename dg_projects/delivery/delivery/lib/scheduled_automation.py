"""Which environments may register each schedule and sensor in this location.

The companion to ``lakehouse.lib.scheduled_automation``, applying the same rule
to the push axis. Opt-in for the same reason: the omitted answer is "does not
run", which is the safe one.

Why registration rather than ``default_status``
-----------------------------------------------
``default_status`` seeds instigator state on first deploy and nothing more, so
it answers "what should this be set to here?" and cannot answer "may this run
here at all". Those came apart when ``learning_resources`` became ``delivery``.
Instigator state is keyed on ``(location_name, repository_name, name)``, so the
renamed location starts with no state anywhere, and the five instigators that
were RUNNING in production needed ``default_status=RUNNING`` to survive the
move. That flag is unconditional: it would have seeded them RUNNING in a fresh
QA or local instance too, where the old location had them STOPPED -- including
``ovs_videos_stale_cleanup_sensor``, which dispatches delete webhooks. The same
holds for everything moved in since, each RUNNING in production when it moved:
the two failure notification sensors from ``data_platform`` and
``instructor_onboarding_daily_schedule`` from ``lakehouse``.

Registration is the enforcing move. An instigator left out of ``Definitions`` is
not stopped, it is absent, and nothing in the Dagster UI can start it. The two
mechanisms compose rather than compete: this map decides where each one exists,
``default_status`` decides how it starts where it does exist.

Why every entry is production-only
----------------------------------
Because that is the only place any of them has been observed to run. The
RUNNING ones were read off production's instigator state, or its ticks, when
each moved in. The four Cohort 2 delivery schedules have never executed anywhere --
production Loki over 30 days and 1.86B lines has zero mentions of them, against
12,585 for ``ovs_videos_api_schedule`` in the same location. They stay
registered in production, still STOPPED, so the Cohort 2 enable remains a UI
toggle and this map does not change that procedure.

Nothing here claims a QA delivery path should never exist. It claims none does
today, and that a location rename is the wrong moment to create one implicitly.

Why this keys on the instigator's own name
------------------------------------------
Unlike the lakehouse map, which is keyed on ids because the ``sync_and_stage``
family's names are generated from the live Airbyte workspace, every name here is
a static literal in ``delivery.definitions``. Reading ``.name`` off the object
removes the id/name pair that would otherwise be free to drift.

Adding a schedule or sensor
---------------------------
Add its name below. There is no fallback: an instigator handed to
:func:`instigators_for_environment` without an entry raises at import, in every
environment, rather than quietly inheriting a default someone would then have to
discover from instance state.
"""

from collections.abc import Iterable, Mapping
from typing import Protocol

from dagster import (
    AssetsDefinition,
    AssetSelection,
    AutomationCondition,
    AutomationConditionSensorDefinition,
    DefaultSensorStatus,
)
from ol_orchestrate.lib.constants import DAGSTER_ENV, VALID_DAGSTER_ENVS

# The day boundary for webhook delivery: lakehouse's non_airbyte_staging_daily
# fires at this tick, and dbt_automation_sensor rebuilds the integrations models
# from that staging afterwards.
LEARN_DELIVERY_CRON = "0 6 * * *"
LEARN_DELIVERY_SENSOR_INTERVAL_SECONDS = 300

INSTIGATOR_ENVIRONMENTS: Mapping[str, frozenset[str]] = {
    # Extraction, not delivery: fetches the Sloan Executive Education API and
    # writes both outputs through s3file_io_manager. Production-only because
    # there is one Sloan API, not one per environment, so a QA tick would call
    # the partner's production endpoint. RUNNING in production.
    "learning_resource_api_schedule": frozenset({"production"}),
    # Every 10 minutes, discovering new OVS videos; feeds the discovery sensor
    # below, so a tick here is what fans out webhook delivery. RUNNING in
    # production.
    "ovs_videos_api_schedule": frozenset({"production"}),
    # Cohort 2 delivery. Merged and registered but never executed anywhere (see
    # above). Registered in production so the enable stays a UI toggle; absent
    # elsewhere so a fresh instance cannot start delivering on its own. These
    # were cron schedules until they were found to fire before the models they
    # read had been rebuilt -- see deliver_after_upstream.
    "mit_climate_delivery_sensor": frozenset({"production"}),
    "mitpe_delivery_sensor": frozenset({"production"}),
    "oll_delivery_sensor": frozenset({"production"}),
    "mit_edx_programs_delivery_sensor": frozenset({"production"}),
    # Cohort 3 delivery (podcasts), on the same terms as Cohort 2: registered
    # STOPPED in production, and not to be started before mit-learn #3557
    # lands the endpoint it POSTs to.
    "podcast_delivery_sensor": frozenset({"production"}),
    # Commits the instructor user list to the access forge GitHub repository's
    # default branch. There is one of those, not one per environment, so a tick
    # outside production would write the real repo. Moved from lakehouse, where
    # it carried the same production-only entry. RUNNING in production.
    "instructor_onboarding_daily_schedule": frozenset({"production"}),
    # Dispatches ovs_videos_webhook_job per discovered partition. RUNNING in
    # production.
    "ovs_videos_discovery_sensor": frozenset({"production"}),
    # Sends DELETE webhooks to MIT Learn for videos that have gone away. The
    # destructive one, and the reason this map covers sensors and not only
    # schedules. RUNNING in production.
    "ovs_videos_stale_cleanup_sensor": frozenset({"production"}),
    # Drops the local partition definition after a delete has been delivered.
    # RUNNING in production.
    "ovs_videos_delete_partition_cleanup_sensor": frozenset({"production"}),
    # Deployment-wide failure alerting, from ol_orchestrate.sensors.
    # failure_notification, registered here and nowhere else (issue #2260).
    # Both were RUNNING in production and STOPPED in QA under data_platform on
    # 2026-09-22, read off each daemon's ticks; CI runs no Dagster. Production
    # only keeps that: a QA registration would start them, since they now
    # declare default_status=RUNNING.
    "run_failure_notification_sensor": frozenset({"production"}),
    "asset_check_failure_sensor": frozenset({"production"}),
}

_UNDECLARED_ENVIRONMENTS = {
    name: sorted(environments - set(VALID_DAGSTER_ENVS))
    for name, environments in INSTIGATOR_ENVIRONMENTS.items()
    if environments - set(VALID_DAGSTER_ENVS)
}
if _UNDECLARED_ENVIRONMENTS:
    # A typo'd environment name here fails open -- the instigator simply never
    # registers anywhere, which looks exactly like a deliberate decision to
    # disable it. Caught at import instead.
    msg = (
        f"INSTIGATOR_ENVIRONMENTS names environments that do not exist: "
        f"{_UNDECLARED_ENVIRONMENTS}. Known environments: "
        f"{sorted(VALID_DAGSTER_ENVS)}."
    )
    raise ValueError(msg)


class _NamedInstigator(Protocol):
    @property
    def name(self) -> str: ...


def instigators_for_environment[T: _NamedInstigator](
    candidates: Iterable[T], *, environment: str = DAGSTER_ENV
) -> list[T]:
    """Keep the schedules and sensors *environment* is declared to run.

    Raises KeyError for a name with no entry in
    :data:`INSTIGATOR_ENVIRONMENTS`, which is the same no-fallback rule
    ``lakehouse.lib.scheduled_automation.schedules_for_environment`` applies.
    """
    kept = []
    for instigator in candidates:
        try:
            environments = INSTIGATOR_ENVIRONMENTS[instigator.name]
        except KeyError:
            msg = (
                f"No environments declared for instigator {instigator.name!r} "
                f"(known: {sorted(INSTIGATOR_ENVIRONMENTS)}). Add an explicit "
                f"entry to INSTIGATOR_ENVIRONMENTS -- defaulting here would let "
                f"a new schedule or sensor start itself in an environment "
                f"nobody chose."
            )
            raise KeyError(msg) from None
        if environment in environments:
            kept.append(instigator)
    return kept


def deliver_after_upstream(
    webhook: AssetsDefinition,
    sensor_name: str,
    *,
    environment: str = DAGSTER_ENV,
) -> tuple[AssetsDefinition, AutomationConditionSensorDefinition]:
    """Deliver *webhook* once a day, after everything it reads is rebuilt.

    Replaces a cron schedule, which POSTed whatever the integrations model held
    at the tick. Those ticks (06:00-07:00) landed at or before the 06:00 staging
    build the models are derived from, so each delivery sent yesterday's data.
    ``on_cron`` waits past the tick until every dep has materialized since it,
    then requests once. If a dep is not rebuilt that day (dbt_automation_sensor
    skips a model whose inputs did not change) nothing is delivered, which is
    also what a full-sync receiver needs: the previous batch is still current.

    The deps live in lakehouse. The automation daemon evaluates against the
    workspace asset graph, so a dep in another code location is observed the
    same as a local one, provided the key matches what lakehouse emits.

    One sensor per source, rather than one over all of them, so each source is
    still enabled on its own. The condition is attached only where the sensor
    registers: a conditioned asset no declared sensor targets is swept into
    Dagster's synthesized ``default_automation_condition_sensor``, which would
    exist even in environments this map leaves out.

    :param webhook: The delivery asset.
    :param sensor_name: Its sensor's name, which must have an entry in
        :data:`INSTIGATOR_ENVIRONMENTS`.
    :param environment: The environment to build for.
    :returns: The asset, conditioned where it may run, and its sensor.
    :rtype: tuple[AssetsDefinition, AutomationConditionSensorDefinition]
    """
    sensor = AutomationConditionSensorDefinition(
        sensor_name,
        target=AssetSelection.assets(webhook),
        minimum_interval_seconds=LEARN_DELIVERY_SENSOR_INTERVAL_SECONDS,
        default_status=DefaultSensorStatus.STOPPED,
    )
    if environment not in INSTIGATOR_ENVIRONMENTS[sensor_name]:
        return webhook, sensor
    condition = AutomationCondition.on_cron(LEARN_DELIVERY_CRON)
    conditioned = webhook.map_asset_specs(
        lambda spec: spec.replace_attributes(automation_condition=condition)
    )
    return conditioned, sensor
