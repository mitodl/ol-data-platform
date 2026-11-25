"""Tests for the Superset dataset assets, built one step per database."""

from typing import Any

import pytest
from dagster import AssetKey, materialize
from dagster_dbt import DagsterDbtTranslator
from lakehouse.assets.superset import (
    build_superset_dataset_assets,
    superset_dataset_keys,
)

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


def _model(
    name: str, layer: str, meta: dict[str, Any], resource_type: str = "model"
) -> dict[str, Any]:
    return {
        "resource_type": resource_type,
        "name": name,
        "unique_id": f"{resource_type}.open_learning.{name}",
        "fqn": ["open_learning", layer, name],
        "config": {"meta": meta},
        "meta": meta,
    }


class LayerKeyTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props: Any) -> AssetKey:
        return AssetKey([dbt_resource_props["fqn"][1], dbt_resource_props["name"]])


def test_superset_dataset_keys_skip_models_that_opt_out():
    manifest = {
        "nodes": {
            "model.open_learning.dim_a": _model("dim_a", "dimensional", {}),
            "model.open_learning.tfact_text": _model(
                "tfact_text", "dimensional", {"superset_dataset": False}
            ),
            "model.open_learning.int_b": _model("int_b", "intermediate", {}),
        }
    }
    keys = [
        AssetKey(["dimensional", "dim_a"]),
        AssetKey(["dimensional", "tfact_text"]),
        AssetKey(["intermediate", "int_b"]),
    ]

    selected = superset_dataset_keys(
        keys, manifest, LayerKeyTranslator(), layers={"dimensional"}
    )

    assert selected == [AssetKey(["dimensional", "dim_a"])]


def test_superset_dataset_keys_publish_opt_in_layers_only_when_asked():
    manifest = {
        "nodes": {
            "model.open_learning.dim_a": _model("dim_a", "dimensional", {}),
            "model.open_learning.dim_b": _model(
                "dim_b", "dimensional", {"superset_dataset": True}
            ),
            "snapshot.open_learning.snapshot_c": _model(
                "snapshot_c", "dimensional", {}, resource_type="snapshot"
            ),
            "snapshot.open_learning.snapshot_d": _model(
                "snapshot_d",
                "dimensional",
                {"superset_dataset": True},
                resource_type="snapshot",
            ),
            "model.open_learning.report_e": _model("report_e", "reporting", {}),
            "snapshot.open_learning.snapshot_f": _model(
                "snapshot_f",
                "reporting",
                {"superset_dataset": False},
                resource_type="snapshot",
            ),
        }
    }
    keys = [
        AssetKey(["dimensional", "dim_a"]),
        AssetKey(["dimensional", "dim_b"]),
        AssetKey(["dimensional", "snapshot_c"]),
        AssetKey(["dimensional", "snapshot_d"]),
        AssetKey(["reporting", "report_e"]),
        AssetKey(["reporting", "snapshot_f"]),
    ]

    selected = superset_dataset_keys(
        keys,
        manifest,
        LayerKeyTranslator(),
        layers={"dimensional", "reporting"},
        opt_in_layers={"dimensional"},
    )

    assert selected == [
        AssetKey(["dimensional", "dim_b"]),
        AssetKey(["dimensional", "snapshot_d"]),
        AssetKey(["reporting", "report_e"]),
    ]


def test_superset_dataset_keys_reject_a_quoted_boolean():
    manifest = {
        "nodes": {
            "model.open_learning.dim_a": _model(
                "dim_a", "dimensional", {"superset_dataset": "true"}
            ),
        }
    }

    with pytest.raises(ValueError, match="dimensional/dim_a"):
        superset_dataset_keys(
            [AssetKey(["dimensional", "dim_a"])],
            manifest,
            LayerKeyTranslator(),
            layers={"dimensional"},
            opt_in_layers={"dimensional"},
        )


def test_superset_dataset_keys_ignore_a_quoted_boolean_outside_the_layers():
    manifest = {
        "nodes": {
            "model.open_learning.stg_a": _model(
                "stg_a", "staging", {"superset_dataset": "true"}
            ),
        }
    }

    selected = superset_dataset_keys(
        [AssetKey(["staging", "stg_a"])],
        manifest,
        LayerKeyTranslator(),
        layers={"dimensional"},
        opt_in_layers={"dimensional"},
    )

    assert selected == []
