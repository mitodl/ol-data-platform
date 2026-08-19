"""Per-environment dbt wiring: which target to write, which data lake to read.

Lives in ``lakehouse.lib`` (rather than beside the assets that use it) so it
can be unit-tested without a parsed dbt manifest on disk -- importing
``assets.lakehouse.dbt`` evaluates a ``@dbt_assets`` decorator, which needs
one. Same reason ``lakehouse.lib.starrocks_dbt`` exists.

Three independent axes, and conflating the first two is what RFC 12711 step 1
exists to undo:

* **target** -- which cluster to connect to, and which warehouse to WRITE.
* **data lake env** -- which ``ol_data_lake_<env>`` catalog to READ.
* **automation** -- whether the dbt asset graph materializes itself here.

They diverge for ``dev``: a developer's StarRocks target port-forwards to the
QA cluster but should read production data. The b2b sources used to infer the
lake from ``'qa' in target.name``, which got that case wrong and made those
models undevelopable locally.

Every environment appears in every map. There is deliberately no fallback --
``qa`` used to be absent from the Trino map and fell through to ``production``,
so the QA code location wrote the production warehouse while the StarRocks
project next to it targeted QA. The two disagreed for months because neither
had to say what it meant.

Adding an environment
---------------------
A fifth environment, ``local`` (k3d + Tilt, its own object store and Iceberg
catalog), is planned -- see RFC 12711's Local-2/3/4 tasks, which specify that
``local`` extends *this* convention rather than introducing a third resolution
style. It is NOT a rename of ``dev``: ``dev`` connects to the remote QA cluster
and reads the production lake, while ``local`` reaches neither and is fed by
local ingest and fixtures. Both will need to exist.

When it lands, ``DAGSTER_ENV`` gains the value and every map below needs an
entry -- plus ``_ENVS`` in ``ol_dbt_cli/commands/starrocks.py``, which mirrors
these. Until then ``resolve_for_environment`` raises on it, which is the point:
the failure is a missing declaration, not a silently inherited warehouse.
Automation is the exception to that rule and deliberately so: a new environment
is simply absent from ``DBT_AUTOMATION_ENVIRONMENTS`` and therefore does not
materialize dbt models unattended. Deciding it should is a separate, later act.
"""

import os
from collections.abc import Mapping

from ol_orchestrate.lib.constants import DAGSTER_ENV, VALID_DAGSTER_ENVS

# Trino. "qa" resolves to the QA warehouse, which is what the rest of the QA
# code location was already configured for -- trino_host_map/trino_catalog_map
# in definitions.py have pointed "qa" at the QA Starburst cluster and
# ol_data_lake_qa all along. Only this map disagreed.
#
# NOTE: profiles.yml has no "ci" output, so that entry does not name a real
# target. Pre-existing, and inert today because nothing runs dbt under
# DAGSTER_ENVIRONMENT=ci; left alone rather than guessed at.
DBT_TARGET_MAP: Mapping[str, str] = {
    "dev": "dev_production",
    "ci": "ci",
    "qa": "qa",
    "production": "production",
}

# StarRocks. These name a CLUSTER and its auth, not a data lake: dev and qa
# share starrocks_qa_vault because a developer port-forwards to the QA cluster.
# Which catalog each then reads is DATA_LAKE_ENV_MAP's job, not this map's.
# Matches the dbt_target choices in ol_dbt_cli/commands/starrocks.py's _ENVS.
STARROCKS_DBT_TARGET_MAP: Mapping[str, str] = {
    "dev": "starrocks_qa_vault",
    # ci connects directly to its own FE service (no port-forward), same
    # connection shape as production -- matches _ENVS["ci"]["dbt_target"].
    "ci": "starrocks_production",
    "qa": "starrocks_qa_vault",
    "production": "starrocks_production",
}

# Whether this environment automates the dbt chain at all: whether dbt and
# Superset assets carry an AutomationCondition, and whether
# ``dbt_automation_sensor`` starts running.
#
# Until RFC 12711 step 1, DBT_TARGET_MAP had no ``qa`` entry and fell through to
# production, so anything running dbt from the QA code location hit the
# PRODUCTION warehouse. Things did. All 18 run_results.json objects in
# s3://dagster-data-qa/openmetadata/dbt-artifacts/runs/ predate that fix and
# every one reads ``"target": "production"``: 13 ``docs generate`` runs between
# 2026-05-11 and 2026-07-01, then 5 ``build`` runs to 2026-08-11. The generates
# filed production's catalog under QA's artifacts prefix, which is where QA's
# OpenMetadata reads from -- so QA metadata described production.
#
# None of that was this sensor, and saying so matters. The timestamps fit
# neither its four-hour interval nor any cron (two on some days), and it is
# stopped in QA. What triggered them is in the Dagster run history, not in S3;
# ``dbt_docs_artifacts_job`` is the only thing here that runs ``docs generate``.
# So this map does not retroactively fix those runs -- it closes a path that
# happened never to be taken. Nothing in the repo said the sensor was allowed to
# run in QA; that it did not was luck, and luck is what a declaration replaces.
#
# The paths that WERE taken are still ungated -- see SCOPE below.
#
# ``default_status`` alone would not have prevented it. It only seeds the
# instance state on first deploy; once a sensor has been toggled in the UI, the
# instance wins forever. So the enforcing half of this declaration is the
# AutomationCondition: with automation off, the assets carry no condition and a
# sensor someone starts by hand evaluates to nothing.
#
# ``qa`` is absent because a QA dbt build is empty or partial until RFC 12711
# step 8 lands the QA app_postgres layer -- an automated partial build of a union
# model emits data that looks fine and silently drops rows. Adding it belongs to
# that step.
#
# WHY OPT-IN, when the maps above are exhaustive and raise on a missing
# environment: because the two axes fail in opposite directions. A missing entry
# in DBT_TARGET_MAP means "write whichever warehouse the default names" -- there
# is no safe answer, so it has to raise, and that fall-through is precisely how
# QA came to build production. A missing entry here means "do not run", which is
# both safe and what a new environment wants on its first day. Making someone
# type ``off`` to get the behaviour they would get anyway is ceremony, not a
# guard -- and they are already in this file, because the three maps above will
# raise until they add entries there.
#
# SCOPE, so the name is not read for more than it does: this covers the
# AutomationCondition path only. The cron ScheduleDefinitions in definitions.py
# are the other way this code location starts work unattended, and they are
# declared separately in ``lakehouse.lib.scheduled_automation`` -- each with its
# own environment set rather than one answer covering all of them, because they
# disagree with each other about QA
# (the ingestion family belongs there, the dbt and GitHub-writing ones do not).
# Read the two together for the whole answer to "what may run here on its own".
# Still open for RFC 12711 (https://github.com/mitodl/hq/discussions/12711):
# whether production's synthesized default_automation_condition_sensor over
# staging (see dbt_automation_sensor's target in definitions.py) should be closed
# by dropping staging's condition rather than only excluding it from the target.
DBT_AUTOMATION_ENVIRONMENTS: frozenset[str] = frozenset({"production"})

if not set(VALID_DAGSTER_ENVS) >= DBT_AUTOMATION_ENVIRONMENTS:
    # The one direction opt-in does fail badly: a typo reads as "off
    # everywhere", so production would quietly stop materializing and nothing
    # downstream would report it -- the assets would just stop carrying a
    # condition. Cheap to rule out here.
    msg = (
        f"DBT_AUTOMATION_ENVIRONMENTS names environments that do not exist: "
        f"{sorted(DBT_AUTOMATION_ENVIRONMENTS - set(VALID_DAGSTER_ENVS))}. "
        f"Known environments: {sorted(VALID_DAGSTER_ENVS)}."
    )
    raise ValueError(msg)

DBT_AUTOMATION_ENABLED = DAGSTER_ENV in DBT_AUTOMATION_ENVIRONMENTS

# Which lake each environment READS. Mirrors trino_catalog_map in
# definitions.py (which cannot be imported here -- it imports the modules that
# import this one); keep the two in step when adding an environment.
DATA_LAKE_ENV_MAP: Mapping[str, str] = {
    "dev": "production",
    "ci": "qa",
    "qa": "qa",
    "production": "production",
}


def resolve_for_environment(
    value_map: Mapping[str, str], *, override_env_var: str, what: str
) -> str:
    """Resolve *what* for this environment from *value_map*.

    Single source of truth shared by a DbtProject (which parses the manifest,
    and therefore the Dagster asset graph) and the DbtCliResource that executes
    it, so the graph always matches what actually runs. *override_env_var*
    takes precedence over the mapping when set.

    Raises KeyError when the current environment is absent from *value_map*:
    a fallback here is what let ``qa`` silently inherit production.
    """
    if override := os.environ.get(override_env_var):
        return override
    try:
        return value_map[DAGSTER_ENV]
    except KeyError:
        msg = (
            f"No {what} declared for DAGSTER_ENVIRONMENT={DAGSTER_ENV!r} "
            f"(known: {sorted(value_map)}). Add an explicit entry -- a fallback "
            f"here would let a new environment inherit another one's warehouse."
        )
        raise KeyError(msg) from None


DBT_TARGET = resolve_for_environment(
    DBT_TARGET_MAP,
    override_env_var="DAGSTER_DBT_TARGET",
    what="dbt target",
)

STARROCKS_DBT_TARGET = resolve_for_environment(
    STARROCKS_DBT_TARGET_MAP,
    override_env_var="DAGSTER_DBT_STARROCKS_TARGET",
    what="StarRocks dbt target",
)

DATA_LAKE_ENV = resolve_for_environment(
    DATA_LAKE_ENV_MAP,
    override_env_var="DAGSTER_DBT_DATA_LAKE_ENV",
    what="data lake environment",
)

# Exported to the dbt subprocess rather than passed as a --vars flag: dbt does
# not recursively render dbt_project.yml `vars` values, and an env var reaches
# both the manifest parse and the build without threading vars through
# DbtProject. Read by _b2b_analytics__sources.yml.
os.environ["DBT_DATA_LAKE_ENV"] = DATA_LAKE_ENV
