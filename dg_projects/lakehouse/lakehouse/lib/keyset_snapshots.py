"""Which raw history snapshots start from their keyset's load rather than raw's.

A snapshot whose deletes come from a keyset (``delete_capture: keyset`` in its
``raw_history`` meta, written by ``ol-dbt inventory history``) reads the raw
table and ``<raw_table>__keyset``, which data_loading reloads after each load of
the raw table. In a lake where that keyset runs (the meta's ``keyset_lakes``),
the snapshot depends on the keyset asset and runs after it, once per load, under
``keyset_snapshot_upstream_or_code_changes``. Elsewhere the delete filter reads
no keyset, so the snapshot keeps the dbt condition every model has.

Every raw history snapshot also waits for its raw table's load guard
(``lakehouse.lib.history_load_guard``) to pass against the latest load.

Kept apart from ``assets.lakehouse.dbt`` so it can be tested without a parsed
manifest.
"""

from collections.abc import Mapping
from typing import Any

from dagster import AssetKey, AutomationCondition
from ol_orchestrate.lib.automation_policies import (
    keyset_snapshot_upstream_or_code_changes,
    raw_load_check_passed,
)

from lakehouse.lib.history_load_guard import (
    RAW_DATA_SOURCE,
    is_raw_history_snapshot,
    load_check_key,
)


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
    if not is_raw_history_snapshot(dbt_resource_props):
        return None
    raw_history = dbt_resource_props["config"]["meta"]["raw_history"]
    if raw_history["delete_capture"] != "keyset":
        return None
    if data_lake_env not in raw_history["keyset_lakes"]:
        return None
    return AssetKey([RAW_DATA_SOURCE, raw_history["keyset_table"]])


def dbt_automation_condition(
    dbt_resource_props: Mapping[str, Any],
    data_lake_env: str,
    default: AutomationCondition | None,
) -> AutomationCondition | None:
    """Return a dbt node's condition, with a raw history snapshot's load gate.

    :param dbt_resource_props: the manifest node.
    :param data_lake_env: the lake the build reads raw from.
    :param default: every other node's condition; None where automation is off,
        which holds for raw history snapshots too.
    :returns: the keyset condition for a keyset snapshot in a keyset lake, the
        default with the load gate appended for any other raw history
        snapshot, and the default for every other node.
    :rtype: AutomationCondition | None
    """
    if default is None or not is_raw_history_snapshot(dbt_resource_props):
        return default
    load_check = load_check_key(dbt_resource_props)
    keyset = snapshot_keyset_key(dbt_resource_props, data_lake_env)
    if keyset is not None:
        return keyset_snapshot_upstream_or_code_changes(keyset, load_check)
    return default & raw_load_check_passed(load_check)
