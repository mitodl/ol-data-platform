"""Per-environment dbt wiring: which target to write, which data lake to read.

Lives in ``lakehouse.lib`` (rather than beside the assets that use it) so it
can be unit-tested without a parsed dbt manifest on disk -- importing
``assets.lakehouse.dbt`` evaluates a ``@dbt_assets`` decorator, which needs
one. Same reason ``lakehouse.lib.starrocks_dbt`` exists.

Two independent axes, and conflating them is what RFC 12711 step 1 exists to
undo:

* **target** -- which cluster to connect to, and which warehouse to WRITE.
* **data lake env** -- which ``ol_data_lake_<env>`` catalog to READ.

They diverge for ``dev``: a developer's StarRocks target port-forwards to the
QA cluster but should read production data. The b2b sources used to infer the
lake from ``'qa' in target.name``, which got that case wrong and made those
models undevelopable locally.

Every environment appears in every map. There is deliberately no fallback --
``qa`` used to be absent from the Trino map and fell through to ``production``,
so the QA code location wrote the production warehouse while the StarRocks
project next to it targeted QA. The two disagreed for months because neither
had to say what it meant.
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
