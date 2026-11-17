"""Which raw history snapshots start from their keyset's load rather than raw's.

A snapshot whose deletes come from a keyset (``delete_capture: keyset`` in its
``raw_history`` meta, written by ``ol-dbt inventory history``) reads the raw
table and ``<raw_table>__keyset``, which data_loading reloads after each load of
the raw table. In a lake where that keyset runs (the meta's ``keyset_lakes``),
the snapshot depends on the keyset asset and runs after it, once per load, under
``keyset_snapshot_upstream_or_code_changes``. Elsewhere the delete filter reads
no keyset, so the snapshot keeps the dbt condition every model has.

Kept apart from ``assets.lakehouse.dbt`` so it can be tested without a parsed
manifest.
"""

from collections.abc import Mapping
from typing import Any

from dagster import AssetKey, AutomationCondition
from ol_orchestrate.lib.automation_policies import (
    keyset_snapshot_upstream_or_code_changes,
)

# The dbt source name of the raw tables, and so the first part of their keys.
RAW_DATA_SOURCE = "ol_warehouse_raw_data"


def snapshot_keyset_key(
    dbt_resource_props: Mapping[str, Any], data_lake_env: str
) -> AssetKey | None:
    """Return the keyset asset a snapshot starts from in this lake, or None.

    :param dbt_resource_props: the manifest node.
    :param data_lake_env: the lake the build reads raw from (``DATA_LAKE_ENV``).
    :returns: the keyset table's asset key when the node is a keyset-filtered
        raw history snapshot and its keyset runs in ``data_lake_env``.
    :rtype: AssetKey | None
    """
    if dbt_resource_props["resource_type"] != "snapshot":
        return None
    # Only the generated raw history snapshots carry this meta.
    raw_history = dbt_resource_props["config"]["meta"].get("raw_history")
    if raw_history is None or raw_history["delete_capture"] != "keyset":
        return None
    if data_lake_env not in raw_history["keyset_lakes"]:
        return None
    return AssetKey([RAW_DATA_SOURCE, raw_history["keyset_table"]])


def dbt_automation_condition(
    dbt_resource_props: Mapping[str, Any],
    data_lake_env: str,
    default: AutomationCondition | None,
) -> AutomationCondition | None:
    """Return a dbt node's condition: the keyset one for a keyset snapshot.

    :param dbt_resource_props: the manifest node.
    :param data_lake_env: the lake the build reads raw from.
    :param default: every other node's condition; None where automation is off,
        which holds for keyset snapshots too.
    :returns: the node's condition.
    :rtype: AutomationCondition | None
    """
    if default is None:
        return None
    keyset = snapshot_keyset_key(dbt_resource_props, data_lake_env)
    return (
        default if keyset is None else keyset_snapshot_upstream_or_code_changes(keyset)
    )
