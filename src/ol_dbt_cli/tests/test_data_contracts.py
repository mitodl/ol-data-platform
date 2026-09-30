"""Tests for OpenMetadata data contracts as code (the data_contract validate check)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.commands.validate import _check_data_contract
from ol_dbt_cli.lib.data_contracts import (
    DATA_CONTRACT_CHECK,
    check_data_contracts,
    create_request,
    load_contracts,
    om_data_type,
    om_fqn,
    types_compatible,
)
from ol_dbt_cli.lib.manifest import ManifestColumn, ManifestModel, ManifestRegistry
from ol_dbt_cli.lib.sql_parser import ParsedModel
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def _write(directory: Path, name: str, data: dict[str, Any]) -> None:
    (directory / f"{name}.yaml").write_text(yaml.safe_dump(data))


def _contract(entity: dict[str, str], schema: list[dict[str, str]] | None = None) -> dict[str, Any]:
    return {
        "entity": entity,
        "contract": {"name": "c", "schema": schema or [{"name": "user_pk", "dataType": "VARCHAR"}]},
    }


def _node(name: str, columns: dict[str, str], resource_type: str = "model", identifier: str = "") -> ManifestModel:
    return ManifestModel(
        unique_id=f"{resource_type}.pkg.{name}",
        name=name,
        resource_type=resource_type,
        original_file_path=f"models/{name}.sql",
        schema="ol_warehouse_production_dimensional",
        database="ol_data_lake_production",
        identifier=identifier,
        columns={c: ManifestColumn(name=c, data_type=t) for c, t in columns.items()},
    )


def _registry(model: ManifestModel | None = None, source: ManifestModel | None = None) -> ManifestRegistry:
    registry = ManifestRegistry()
    if model is not None:
        registry.nodes[model.unique_id] = model
        registry.by_name[model.name] = model
    if source is not None:
        registry.nodes[source.unique_id] = source
        registry.sources["raw.users_user"] = source
    return registry


def _run(tmp_path: Path, contract: dict[str, Any], registry: ManifestRegistry, sql_columns: set[str] | None = None):
    _write(tmp_path, "c", contract)
    parsed = {"dim_user": ParsedModel(name="dim_user", output_columns=sql_columns)} if sql_columns is not None else {}
    report = ValidationReport()
    check_data_contracts(load_contracts(tmp_path), registry, parsed, report)
    return report


DIM_USER = {"type": "table", "dbt_model": "dim_user"}


def test_repo_contracts_load() -> None:
    contracts = load_contracts(REPO_CONTRACTS_DIR)
    assert [c.body["name"] for c in contracts] == ["dim_user"]
    assert contracts[0].entity.kind == "dbt_model"


def test_intact_model_passes(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry(_node("dim_user", {"user_pk": "varchar"})), {"user_pk"})
    assert report.issues == []


def test_dropped_yaml_column_errors(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry(_node("dim_user", {"email": "varchar"})))
    assert [i.message for i in report.errors] == ["Contracted column 'user_pk' is not declared in the YAML"]


def test_column_missing_from_sql_errors(tmp_path: Path) -> None:
    registry = _registry(_node("dim_user", {"user_pk": "varchar"}))
    report = _run(tmp_path, _contract(DIM_USER), registry, {"email"})
    assert [i.message for i in report.errors] == ["Contracted column 'user_pk' is not selected by the model SQL"]


def test_retyped_column_errors(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry(_node("dim_user", {"user_pk": "bigint"})))
    assert [i.message for i in report.errors] == ["Contracted column 'user_pk' is bigint, contract says VARCHAR"]


def test_same_family_type_change_passes(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry(_node("dim_user", {"user_pk": "string"})))
    assert report.issues == []


def test_missing_data_type_errors(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry(_node("dim_user", {"user_pk": ""})))
    assert [i.message for i in report.errors] == ["Contracted column 'user_pk' has no data_type in the YAML"]


def test_missing_model_errors(tmp_path: Path) -> None:
    report = _run(tmp_path, _contract(DIM_USER), _registry())
    assert [i.message for i in report.errors] == ["Contract c.yaml names a dbt_model that does not exist"]
    assert report.errors[0].check == DATA_CONTRACT_CHECK


def test_dbt_source_binding_checks_source_columns(tmp_path: Path) -> None:
    source = _node("users_user", {"id": "integer"}, resource_type="source")
    contract = _contract({"type": "table", "dbt_source": "raw.users_user"}, [{"name": "id", "dataType": "BIGINT"}])
    assert _run(tmp_path, contract, _registry(source=source)).issues == []

    source.columns.clear()
    assert len(_run(tmp_path, contract, _registry(source=source)).errors) == 1


def test_fqn_binding_is_not_checked_locally(tmp_path: Path) -> None:
    contract = _contract({"type": "dashboardDataModel", "fqn": "Superset.model.41"})
    assert _run(tmp_path, contract, _registry()).issues == []


@pytest.mark.parametrize(
    ("entity", "message"),
    [
        ({"type": "table"}, "exactly one of"),
        ({"type": "table", "dbt_model": "a", "fqn": "b"}, "exactly one of"),
        ({"type": "topic", "dbt_model": "a"}, "always a table"),
        ({"type": "table", "dbt_source": "no_dot"}, "<source_name>.<table_name>"),
    ],
)
def test_malformed_binding_raises(tmp_path: Path, entity: dict[str, str], message: str) -> None:
    _write(tmp_path, "c", _contract(entity))
    with pytest.raises(ValueError, match=message):
        load_contracts(tmp_path)


def test_contract_entity_is_rejected(tmp_path: Path) -> None:
    data = _contract(DIM_USER)
    data["contract"]["entity"] = {"id": "x", "type": "table"}
    _write(tmp_path, "c", data)
    with pytest.raises(ValueError, match="resolved by `ol-dbt contracts sync`"):
        load_contracts(tmp_path)


def test_om_fqn_and_request(tmp_path: Path) -> None:
    _write(tmp_path, "c", _contract(DIM_USER))
    [contract] = load_contracts(tmp_path)
    registry = _registry(_node("dim_user", {}, identifier="dim_user"))
    assert (
        om_fqn(contract.entity, registry, "Starburst Galaxy")
        == "Starburst Galaxy.ol_data_lake_production.ol_warehouse_production_dimensional.dim_user"
    )
    body = create_request(contract, "abc", [{"id": "t1", "type": "team"}])
    assert body["entity"] == {"id": "abc", "type": "table"}
    assert body["owners"] == [{"id": "t1", "type": "team"}]
    assert "entity" not in contract.body


@pytest.mark.parametrize(
    ("warehouse", "om"),
    [
        ("varchar", "VARCHAR"),
        ("array(bigint)", "ARRAY"),
        ("timestamp(6) with time zone", "TIMESTAMP"),
        ("integer", "INT"),
    ],
)
def test_om_data_type(warehouse: str, om: str) -> None:
    assert om_data_type(warehouse) == om


def test_types_compatible() -> None:
    assert types_compatible("BIGINT", "INT")
    assert not types_compatible("BIGINT", "VARCHAR")


def test_no_manifest_warns(tmp_path: Path) -> None:
    _write(tmp_path, "c", _contract(DIM_USER))
    report = ValidationReport()
    _check_data_contract(None, {}, tmp_path, report)
    assert [i.severity for i in report.issues] == [Severity.WARNING]


def test_no_contracts_dir_is_silent(tmp_path: Path) -> None:
    report = ValidationReport()
    _check_data_contract(None, {}, tmp_path / "missing", report)
    assert report.issues == []
