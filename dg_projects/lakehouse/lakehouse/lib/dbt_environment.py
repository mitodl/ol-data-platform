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
``DBT_AUTOMATION_MAP`` is the one to think hardest about for a new environment,
since it decides whether that environment materializes dbt models unattended.
"""

import os
from collections.abc import Mapping

from ol_orchestrate.lib.constants import DAGSTER_ENV

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
# production, so this sensor firing in the QA code location built the PRODUCTION
# warehouse. It did: the newest dbt run_results.json in s3://dagster-data-qa/
# before that fix is 2026-08-11 and reads ``"target": "production"``. Nothing in
# the repo said whether the sensor was allowed to run there -- it was a hand-made
# instance setting, which is how the misrouting stayed invisible for months.
#
# ``default_status`` alone would not have prevented it. It only seeds the
# instance state on first deploy; once a sensor has been toggled in the UI, the
# instance wins forever. So the enforcing half of this declaration is the
# AutomationCondition: with automation off, the assets carry no condition and a
# sensor someone starts by hand evaluates to nothing.
#
# ``qa`` is off because a QA dbt build is empty or partial until RFC 12711 step 8
# lands the QA app_postgres layer -- an automated partial build of a union model
# emits data that looks fine and silently drops rows. Flip it to "on" there.
#
# SCOPE, so the name is not read for more than it does: this covers the
# AutomationCondition path only. The cron ScheduleDefinitions in definitions.py
# -- b2b_analytics_starrocks_nightly (which builds the tag:starrocks models),
# dbt_docs_artifacts_daily, and the two iceberg maintenance schedules -- are
# registered in every environment and gated only by
# ``default_status=DefaultScheduleStatus.STOPPED``, which is the same
# seed-once-then-the-instance-wins setting this map replaces for sensors. Whether
# any of them is running in QA is instance state the repo cannot see -- the same
# blind spot, one layer over.
# Left open for RFC 12711 (https://github.com/mitodl/hq/discussions/12711):
# whether those schedules join this declaration, and
# whether production's synthesized default_automation_condition_sensor over
# staging (see dbt_automation_sensor's target in definitions.py) should be closed
# by dropping staging's condition rather than only excluding it from the target.
DBT_AUTOMATION_MAP: Mapping[str, str] = {
    "dev": "off",
    "ci": "off",
    "qa": "off",
    "production": "on",
}

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

_AUTOMATION_VALUES = frozenset({"on", "off"})
_automation = resolve_for_environment(
    DBT_AUTOMATION_MAP,
    override_env_var="DAGSTER_DBT_AUTOMATION",
    what="dbt automation mode",
)
if _automation not in _AUTOMATION_VALUES:
    # Checked rather than assumed: `== "on"` alone would read a typo, or an
    # operator's DAGSTER_DBT_AUTOMATION=true, as "off" -- quietly disabling
    # production automation instead of failing the import.
    msg = (
        f"dbt automation mode is {_automation!r}; expected one of "
        f"{sorted(_AUTOMATION_VALUES)}."
    )
    raise ValueError(msg)

DBT_AUTOMATION_ENABLED = _automation == "on"

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
