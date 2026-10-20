"""Freshness checks bounded by data contract SLAs, with the layer default elsewhere."""

from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
import yaml
from dagster import AssetKey
from dagster_dbt import dbt_assets
from lakehouse.lib import contract_freshness
from lakehouse.lib.contract_freshness import (
    CONTRACTS_FALLBACK,
    build_freshness_checks,
    contract_freshness_bounds,
    dbt_model_key_lookup,
    find_contracts_dir,
)
from ol_dbt_cli.lib.data_contracts import load_contracts

DEFAULT = timedelta(hours=26)
DIM_USER = AssetKey(["dimensional", "dim_user"])
DIM_COURSE = AssetKey(["dimensional", "dim_course"])
STG_USER = AssetKey(["staging", "stg_user"])


def _bounds_by_key(checks) -> dict[AssetKey, list[float]]:
    found: dict[AssetKey, list[float]] = {}
    for check in checks:
        for spec in check.check_specs:
            params = spec.metadata["dagster/freshness_params"].value
            found.setdefault(spec.asset_key, []).append(
                params["lower_bound_delta_seconds"]
            )
    return found


def _write_contract(
    directory: Path, name: str, entity: dict[str, str], sla: Any
) -> None:
    contract: dict[str, Any] = {"name": name}
    if sla is not None:
        contract["sla"] = sla
    (directory / f"{name}.yaml").write_text(
        yaml.safe_dump({"entity": entity, "contract": contract})
    )


class TestBuildFreshnessChecks:
    def test_contract_bound_replaces_the_default(self) -> None:
        checks = build_freshness_checks(
            default_keys=[DIM_USER, DIM_COURSE],
            default_bound=DEFAULT,
            contract_bounds={DIM_USER: timedelta(hours=6)},
        )

        assert _bounds_by_key(checks) == {
            DIM_USER: [timedelta(hours=6).total_seconds()],
            DIM_COURSE: [DEFAULT.total_seconds()],
        }

    def test_contracted_asset_outside_the_default_groups_is_checked(self) -> None:
        checks = build_freshness_checks(
            default_keys=[DIM_COURSE],
            default_bound=DEFAULT,
            contract_bounds={STG_USER: timedelta(days=7)},
        )

        assert _bounds_by_key(checks)[STG_USER] == [timedelta(days=7).total_seconds()]

    def test_no_asset_gets_two_freshness_checks(self) -> None:
        # A contract bound equal to the default must not add a second check on
        # top of the default one either.
        checks = build_freshness_checks(
            default_keys=[DIM_USER, DIM_COURSE],
            default_bound=DEFAULT,
            contract_bounds={DIM_USER: DEFAULT, STG_USER: timedelta(hours=1)},
        )

        check_keys = Counter(
            (spec.asset_key, spec.name) for c in checks for spec in c.check_specs
        )
        assert set(check_keys.values()) == {1}
        assert {key for key, _ in check_keys} == {DIM_USER, DIM_COURSE, STG_USER}

    def test_no_contracts_keeps_one_default_check(self) -> None:
        checks = build_freshness_checks(
            default_keys=[DIM_USER, DIM_COURSE],
            default_bound=DEFAULT,
            contract_bounds={},
        )

        assert len(checks) == 1
        assert _bounds_by_key(checks) == {
            DIM_USER: [DEFAULT.total_seconds()],
            DIM_COURSE: [DEFAULT.total_seconds()],
        }


class TestContractFreshnessBounds:
    def test_only_dbt_model_contracts_with_a_refresh_sla_count(
        self, tmp_path: Path
    ) -> None:
        daily = {"refreshFrequency": {"interval": 1, "unit": "day"}}
        _write_contract(
            tmp_path, "dim_user", {"type": "table", "dbt_model": "dim_user"}, daily
        )
        _write_contract(
            tmp_path, "dim_course", {"type": "table", "dbt_model": "dim_course"}, None
        )
        _write_contract(
            tmp_path, "raw", {"type": "table", "dbt_source": "raw.users_user"}, daily
        )
        _write_contract(
            tmp_path, "dataset", {"type": "table", "fqn": "svc.db.s.t"}, daily
        )
        keys = {"dim_user": DIM_USER, "dim_course": DIM_COURSE}

        bounds = contract_freshness_bounds(load_contracts(tmp_path), keys.get)

        assert bounds == {DIM_USER: timedelta(days=1)}

    def test_model_without_an_asset_warns_and_is_skipped(self, tmp_path: Path) -> None:
        _write_contract(
            tmp_path,
            "b2b",
            {"type": "table", "dbt_model": "starrocks_only"},
            {"refreshFrequency": {"interval": 1, "unit": "hour"}},
        )

        with pytest.warns(UserWarning, match="starrocks_only"):
            bounds = contract_freshness_bounds(
                load_contracts(tmp_path), lambda _name: None
            )

        assert bounds == {}


def _manifest_node(name: str) -> dict[str, Any]:
    return {
        "unique_id": f"model.pkg.{name}",
        "name": name,
        "resource_type": "model",
        "package_name": "pkg",
        "original_file_path": f"models/{name}.sql",
        "path": f"{name}.sql",
        "fqn": ["pkg", name],
        "schema": "s",
        "database": "db",
        "alias": name,
        "config": {"materialized": "table", "meta": {}, "tags": [], "enabled": True},
        "meta": {},
        "tags": [],
        "depends_on": {"nodes": [], "macros": []},
        "description": "",
        "columns": {},
        "checksum": {"name": "sha256", "checksum": "x"},
        "raw_code": "select 1",
    }


def test_dbt_model_key_lookup_only_resolves_selected_models() -> None:
    names = ["dim_user", "stg_user"]
    manifest = {
        "metadata": {"project_name": "pkg"},
        "nodes": {f"model.pkg.{n}": _manifest_node(n) for n in names},
        "sources": {},
        "exposures": {},
        "metrics": {},
        "semantic_models": {},
        "saved_queries": {},
        "selectors": {},
        "unit_tests": {},
        "child_map": {f"model.pkg.{n}": [] for n in names},
        "parent_map": {f"model.pkg.{n}": [] for n in names},
        "group_map": {},
        "groups": {},
    }

    @dbt_assets(manifest=manifest, exclude="stg_user")
    def project(): ...

    lookup = dbt_model_key_lookup(project)

    assert lookup("dim_user") == AssetKey(["dim_user"])
    # In the manifest but selected out of this definition: no asset to check.
    assert lookup("stg_user") is None
    assert lookup("missing") is None


class TestContractsDir:
    def test_resolves_the_repo_contracts_from_the_source_tree(self) -> None:
        found = find_contracts_dir()
        assert (found / "dim_user.yaml").is_file()

    def test_repo_dim_user_keeps_the_26_hour_bound(self) -> None:
        bounds = contract_freshness_bounds(
            load_contracts(find_contracts_dir()), {"dim_user": DIM_USER}.get
        )
        assert bounds[DIM_USER] == DEFAULT

    def test_finds_contracts_two_levels_up_as_in_the_image(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module = tmp_path / "app" / "lakehouse" / "lib" / "contract_freshness.py"
        module.parent.mkdir(parents=True)
        module.touch()
        expected = tmp_path / "app" / "contracts"
        expected.mkdir()
        monkeypatch.setattr(contract_freshness, "__file__", str(module))

        assert find_contracts_dir() == expected

    def test_missing_contracts_warns_and_returns_the_fallback(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        module = tmp_path / "nowhere" / "contract_freshness.py"
        module.parent.mkdir(parents=True)
        module.touch()
        monkeypatch.setattr(contract_freshness, "__file__", str(module))

        with pytest.warns(UserWarning, match="default freshness bound"):
            assert find_contracts_dir() == CONTRACTS_FALLBACK
