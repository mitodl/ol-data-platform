"""A keyset-filtered history snapshot starts from its keyset, where the keyset runs."""

from typing import Any

import pytest
from dagster import AssetKey, AutomationCondition
from lakehouse.lib.keyset_snapshots import (
    dbt_automation_condition,
    snapshot_keyset_key,
)
from ol_orchestrate.lib.automation_policies import (
    dbt_upstream_or_code_changes,
    keyset_snapshot_upstream_or_code_changes,
)

KEYSET_TABLE = "raw__mitxonline__app__postgres__courses_course__keyset"
KEYSET_KEY = AssetKey(["ol_warehouse_raw_data", KEYSET_TABLE])


def _node(
    resource_type: str = "snapshot", raw_history: dict[str, Any] | None = None
) -> dict[str, Any]:
    meta = {} if raw_history is None else {"raw_history": raw_history}
    return {"resource_type": resource_type, "config": {"meta": meta}}


KEYSET_SNAPSHOT = _node(
    raw_history={
        "unit": "mitxonline/app_postgres",
        "absence": "deleted",
        "delete_capture": "keyset",
        "keyset_table": KEYSET_TABLE,
        "keyset_lakes": ["qa"],
    }
)


def test_keyset_snapshot_in_a_keyset_lake_names_its_keyset() -> None:
    assert snapshot_keyset_key(KEYSET_SNAPSHOT, "qa") == KEYSET_KEY


def test_keyset_snapshot_outside_its_keyset_lakes_has_none() -> None:
    """Production reads no keyset until the unit's keyset_lakes names it."""
    assert snapshot_keyset_key(KEYSET_SNAPSHOT, "production") is None


@pytest.mark.parametrize(
    "node",
    [
        _node(
            raw_history={"unit": "x/y", "absence": "deleted", "delete_capture": "none"}
        ),
        _node(),
        _node(resource_type="model"),
    ],
    ids=["other-delete-capture", "hand-written-snapshot", "model"],
)
def test_other_nodes_have_no_keyset(node: dict[str, Any]) -> None:
    assert snapshot_keyset_key(node, "qa") is None


def test_keyset_snapshot_takes_the_keyset_condition() -> None:
    condition = dbt_automation_condition(
        KEYSET_SNAPSHOT, "qa", dbt_upstream_or_code_changes()
    )
    assert isinstance(condition, AutomationCondition)
    expected = keyset_snapshot_upstream_or_code_changes(KEYSET_KEY)
    assert condition.get_unique_id() == expected.get_unique_id()


def test_other_nodes_keep_the_default_condition() -> None:
    default = dbt_upstream_or_code_changes()
    assert dbt_automation_condition(KEYSET_SNAPSHOT, "production", default) is default


def test_no_condition_where_automation_is_off() -> None:
    assert dbt_automation_condition(KEYSET_SNAPSHOT, "qa", None) is None
