"""Tests for TrinoMaintenanceResource.base_tables_among."""

import pytest
from ol_orchestrate.resources.trino_maintenance import TrinoMaintenanceResource

SCHEMA = "ol_warehouse_production_mart"


def test_only_listed_candidates_are_returned(monkeypatch: pytest.MonkeyPatch) -> None:
    asked: list[set[str]] = []

    def listing(
        _self: TrinoMaintenanceResource, schemas: set[str]
    ) -> set[tuple[str, str]]:
        asked.append(schemas)
        return {(SCHEMA, "marts__a"), (SCHEMA, "marts__a__dbt_tmp")}

    monkeypatch.setattr(TrinoMaintenanceResource, "base_tables_in_schemas", listing)
    trino = TrinoMaintenanceResource.model_construct(host="trino")
    candidates = {(SCHEMA, "marts__a"), (SCHEMA, "marts__absent")}
    assert trino.base_tables_among(candidates) == {(SCHEMA, "marts__a")}
    assert asked == [{SCHEMA}]
