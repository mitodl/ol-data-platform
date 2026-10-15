"""Tests for the Superset dataset assets, built one step per database."""

from typing import Any

import pytest
from dagster import AssetKey, materialize
from lakehouse.assets.superset import build_superset_dataset_assets

DBT_KEYS = [
    AssetKey(["mart", "Refreshed_Model"]),
    AssetKey(["reporting", "missing_model"]),
    AssetKey(["dimensional", "broken_model"]),
]


class FakeSupersetClient:
    def __init__(self) -> None:
        self.lookups: list[tuple[str, str, int]] = []
        self.refreshed: list[int] = []
        self.updated: list[tuple[int, dict[str, Any]]] = []

    def get_or_create_dataset(
        self, schema_suffix: str, table_name: str, database_id: int, schema_base: str
    ) -> int | None:
        assert schema_base == "ol_warehouse_production"
        self.lookups.append((schema_suffix, table_name, database_id))
        if table_name.lower() == "missing_model":
            return None
        if table_name == "broken_model":
            msg = "Failed to create dataset"
            raise RuntimeError(msg)
        return 42

    def refresh_dataset(self, dataset_id: int) -> dict[str, Any]:
        self.refreshed.append(dataset_id)
        return {}

    def update_dataset(
        self, dataset_id: int, payload: dict[str, Any]
    ) -> dict[str, Any]:
        self.updated.append((dataset_id, payload))
        return {}


class FakeSupersetApi:
    def __init__(self) -> None:
        self.client = FakeSupersetClient()


def _materialized_metadata(result) -> dict[AssetKey, dict[str, Any]]:
    return {
        event.asset_key: {
            key: value.value for key, value in event.materialization.metadata.items()
        }
        for event in result.get_asset_materialization_events()
    }


def test_keys_and_groups_match_the_per_dataset_assets_they_replace():
    trino = build_superset_dataset_assets(DBT_KEYS, database_id=1)
    starrocks = build_superset_dataset_assets(
        DBT_KEYS, database_id=3, database_name="starrocks"
    )

    assert set(trino.keys) == {
        AssetKey(["superset", "dataset", "Refreshed_Model"]),
        AssetKey(["superset", "dataset", "missing_model"]),
        AssetKey(["superset", "dataset", "broken_model"]),
    }
    assert set(starrocks.keys) == {
        AssetKey(["superset", "starrocks", "dataset", "Refreshed_Model"]),
        AssetKey(["superset", "starrocks", "dataset", "missing_model"]),
        AssetKey(["superset", "starrocks", "dataset", "broken_model"]),
    }
    assert set(trino.group_names_by_key.values()) == {"superset_dataset"}
    assert set(starrocks.group_names_by_key.values()) == {"superset_starrocks_dataset"}
    for dbt_key in DBT_KEYS:
        dataset_key = AssetKey(["superset", "dataset", dbt_key.path[1]])
        assert trino.asset_deps[dataset_key] == {dbt_key}


def test_one_step_reports_each_dataset_status():
    superset_api = FakeSupersetApi()
    assets = build_superset_dataset_assets(DBT_KEYS, database_id=1)

    result = materialize([assets], resources={"superset_api": superset_api})

    assert result.success
    metadata = _materialized_metadata(result)
    assert metadata[AssetKey(["superset", "dataset", "Refreshed_Model"])] == {
        "status": "refreshed",
        "superset_dataset_id": 42,
        "dbt_asset_group_name": "mart",
        "dbt_model_name": "Refreshed_Model",
    }
    assert (
        metadata[AssetKey(["superset", "dataset", "missing_model"])]["status"]
        == "skipped"
    )
    assert metadata[AssetKey(["superset", "dataset", "broken_model"])] == {
        "status": "error",
        "error": "Failed to create dataset",
        "dbt_asset_group_name": "dimensional",
        "dbt_model_name": "broken_model",
    }
    assert superset_api.client.refreshed == [42]
    assert [dataset_id for dataset_id, _ in superset_api.client.updated] == [42]


def test_a_bad_response_body_costs_only_its_own_dataset():
    class BadBodyClient(FakeSupersetClient):
        def refresh_dataset(self, dataset_id: int) -> dict[str, Any]:
            if len(self.refreshed) == 0:
                self.refreshed.append(dataset_id)
                msg = "Expecting value"
                raise ValueError(msg)
            return super().refresh_dataset(dataset_id)

    superset_api = FakeSupersetApi()
    superset_api.client = BadBodyClient()
    keys = [AssetKey(["mart", "first_model"]), AssetKey(["mart", "second_model"])]
    assets = build_superset_dataset_assets(keys, database_id=1)

    result = materialize([assets], resources={"superset_api": superset_api})

    assert result.success
    statuses = {
        key.path[-1]: metadata["status"]
        for key, metadata in _materialized_metadata(result).items()
    }
    assert statuses == {"first_model": "error", "second_model": "refreshed"}


def test_the_step_fails_when_every_dataset_failed():
    class NoTokenClient(FakeSupersetClient):
        def refresh_dataset(self, dataset_id: int) -> dict[str, Any]:
            self.refreshed.append(dataset_id)
            msg = "Failed to fetch access token"
            raise RuntimeError(msg)

    superset_api = FakeSupersetApi()
    superset_api.client = NoTokenClient()
    keys = [AssetKey(["mart", "first_model"]), AssetKey(["mart", "second_model"])]
    assets = build_superset_dataset_assets(keys, database_id=1)

    result = materialize(
        [assets], resources={"superset_api": superset_api}, raise_on_error=False
    )

    assert not result.success
    failure = result.get_step_failure_events()[0].step_failure_data.error
    assert failure is not None
    assert "Every selected trino Superset dataset failed (2)" in failure.message
    # Both datasets were tried before the step gave up.
    assert superset_api.client.refreshed == [42, 42]
    statuses = {m["status"] for m in _materialized_metadata(result).values()}
    assert statuses == {"error"}


def test_one_selected_dataset_failing_does_not_fail_the_step():
    superset_api = FakeSupersetApi()
    assets = build_superset_dataset_assets(DBT_KEYS, database_id=1)
    selected = AssetKey(["superset", "dataset", "broken_model"])

    result = materialize(
        [assets], resources={"superset_api": superset_api}, selection=[selected]
    )

    assert result.success
    assert _materialized_metadata(result)[selected]["status"] == "error"


def test_a_subset_touches_only_the_selected_datasets():
    superset_api = FakeSupersetApi()
    assets = build_superset_dataset_assets(
        DBT_KEYS, database_id=3, database_name="starrocks"
    )
    selected = AssetKey(["superset", "starrocks", "dataset", "Refreshed_Model"])

    result = materialize(
        [assets], resources={"superset_api": superset_api}, selection=[selected]
    )

    assert result.success
    assert set(_materialized_metadata(result)) == {selected}
    # StarRocks folds identifiers to lowercase.
    assert superset_api.client.lookups == [("mart", "refreshed_model", 3)]


def test_non_trino_database_needs_its_own_id():
    with pytest.raises(ValueError, match="requires an explicit database_id"):
        build_superset_dataset_assets(DBT_KEYS, database_name="starrocks")
