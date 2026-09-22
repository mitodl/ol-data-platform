"""Tests for selecting the staging models no sync_and_stage job builds."""

from pathlib import Path
from typing import Any

from lakehouse.lib.inventory import INVENTORY_DIR
from lakehouse.lib.non_airbyte_staging import (
    non_airbyte_raw_tables,
    staging_models_reading,
)
from ol_dbt_cli.lib.inventory import Unit, load_units


def _unit(loader: str, *raw_tables: str) -> Unit:
    return Unit(
        path=Path(f"{loader}.yml"),
        data={
            "loader": loader,
            "tables": [{"name": t, "raw_table": t} for t in raw_tables],
        },
    )


def _source(name: str) -> dict[str, Any]:
    return {"name": name}


def _model(name: str, schema: str, *parents: str) -> dict[str, Any]:
    return {
        "name": name,
        "resource_type": "model",
        "config": {"schema": schema},
        "depends_on": {"nodes": list(parents)},
    }


SRC = "source.open_learning.ol_warehouse_raw_data"

MANIFEST = {
    "sources": {
        f"{SRC}.raw__dlt_table": _source("raw__dlt_table"),
        f"{SRC}.raw__airbyte_table": _source("raw__airbyte_table"),
    },
    "nodes": {
        "model.open_learning.stg__dlt": _model(
            "stg__dlt", "staging", f"{SRC}.raw__dlt_table"
        ),
        "model.open_learning.stg__airbyte": _model(
            "stg__airbyte", "staging", f"{SRC}.raw__airbyte_table"
        ),
        # Reads the dlt table directly but is not staging, so the automation
        # sensor already covers it.
        "model.open_learning.int__reads_raw": _model(
            "int__reads_raw", "intermediate", f"{SRC}.raw__dlt_table"
        ),
        # Downstream of the dlt staging model, not of the raw table.
        "model.open_learning.stg__second_hop": _model(
            "stg__second_hop", "staging", "model.open_learning.stg__dlt"
        ),
        "test.open_learning.not_null_stg__dlt_id": {
            "name": "not_null_stg__dlt_id",
            "resource_type": "test",
            "config": {"schema": "staging"},
            "depends_on": {"nodes": [f"{SRC}.raw__dlt_table"]},
        },
    },
}


def test_raw_tables_exclude_airbyte_units() -> None:
    units = [
        _unit("airbyte", "raw__airbyte_table"),
        _unit("dlt", "raw__dlt_table"),
        _unit("dagster", "raw__dagster_table"),
    ]

    assert non_airbyte_raw_tables(units) == {"raw__dlt_table", "raw__dagster_table"}


def test_selects_only_staging_models_directly_on_those_sources() -> None:
    assert staging_models_reading(MANIFEST, {"raw__dlt_table"}) == {"stg__dlt"}


def test_unknown_raw_table_selects_nothing() -> None:
    assert staging_models_reading(MANIFEST, {"raw__not_a_source"}) == set()


def test_real_inventory_includes_the_edxorg_database_tables() -> None:
    # The unit whose migration to dlt left seven staging models unbuilt.
    raw_tables = non_airbyte_raw_tables(load_units(INVENTORY_DIR))

    assert "raw__edxorg__s3__tables__student_courseenrollment" in raw_tables
    assert "raw__edxorg__s3__tables__auth_user" in raw_tables
