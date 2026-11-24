"""The staging models no ``sync_and_stage_*`` job builds.

Staging is built by the per-connection ``sync_and_stage_*`` jobs, which select
an Airbyte group and its dbt children at depth 1, and ``dbt_automation_sensor``
subtracts the whole staging group from its target. A raw table loaded by
anything other than Airbyte (dlt in data_loading, or a Dagster asset in another
code location) therefore has no path to its staging model at all. When edxorg's
database tables moved from Airbyte to dlt, their seven staging models kept the
last tables Airbyte-era runs built (0 rows, 2026-06-24) while raw filled back
up, and nothing reported it.

The inventory is the source of truth for which loader owns a raw table, so the
selection is derived from it rather than listed. A unit that flips to
``loader: dlt`` is picked up here in the same change, as long as dbt declares a
source for its raw table. A table with no source (keycloak today)
selects nothing, and nothing reports that.

A staging model that reads another of these staging models rather than the raw
table has no path either, so it is selected too. The Open edX document and
transcript models moved onto stg__openedx__s3__course_text_snapshots in #3429
and were not rebuilt again until this covered them.
"""

from collections.abc import Iterable, Mapping
from typing import Any

from ol_dbt_cli.lib.inventory import Unit

AIRBYTE_LOADER = "airbyte"
STAGING_SCHEMA = "staging"


def non_airbyte_raw_tables(units: Iterable[Unit]) -> set[str]:
    """Return the raw tables of every inventory unit not loaded by Airbyte.

    :param units: Parsed inventory units.
    :returns: Raw table names owned by dlt, Dagster, or any other non-Airbyte loader.
    :rtype: set[str]
    """
    return {
        table["raw_table"]
        for unit in units
        if unit.data["loader"] != AIRBYTE_LOADER
        for table in unit.tables
    }


def staging_models_reading(
    manifest: Mapping[str, Any],
    raw_tables: set[str],
    excluded: Iterable[str] = (),
) -> set[str]:
    """Return the staging models fed by any of ``raw_tables``.

    A model is fed by a table when it reads it directly or reads another staging
    model that is. Staging is identified by ``config.schema``, the same field the
    dbt translator turns into the Dagster group name.

    :param manifest: A parsed dbt ``manifest.json``.
    :param raw_tables: dbt source table names to match.
    :param excluded: Staging model names to leave out, and not to walk through:
        a model fed only through one of them is left out too, since a job without
        the parent would read a relation it did not build.
    :returns: Names of the staging models with one of those sources upstream
        through staging models only.
    :rtype: set[str]
    """
    excluded = set(excluded)
    staging = {
        unique_id: node
        for unique_id, node in manifest["nodes"].items()
        if node["resource_type"] == "model"
        and node["config"].get("schema") == STAGING_SCHEMA
        and node["name"] not in excluded
    }
    fed_by = {
        unique_id
        for unique_id, source in manifest["sources"].items()
        if source["name"] in raw_tables
    }
    selected: set[str] = set()
    while True:
        reached = {
            unique_id
            for unique_id, node in staging.items()
            if unique_id not in selected
            and fed_by.intersection(node["depends_on"]["nodes"])
        }
        if not reached:
            return {staging[unique_id]["name"] for unique_id in selected}
        selected |= reached
        fed_by |= reached
