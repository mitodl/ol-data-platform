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
``loader: dlt`` is picked up here in the same change, with no schedule to edit.
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
    manifest: Mapping[str, Any], raw_tables: set[str]
) -> set[str]:
    """Return the staging models that read directly from any of ``raw_tables``.

    Staging is identified by ``config.schema``, the same field the dbt
    translator turns into the Dagster group name.

    :param manifest: A parsed dbt ``manifest.json``.
    :param raw_tables: dbt source table names to match.
    :returns: Names of the staging models with one of those sources as a parent.
    :rtype: set[str]
    """
    source_ids = {
        unique_id
        for unique_id, source in manifest["sources"].items()
        if source["name"] in raw_tables
    }
    return {
        node["name"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["config"].get("schema") == STAGING_SCHEMA
        and source_ids.intersection(node["depends_on"]["nodes"])
    }
