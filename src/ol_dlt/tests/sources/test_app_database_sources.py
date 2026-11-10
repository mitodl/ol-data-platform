"""Spec-vs-inventory tests for the app-database sources that load QA before cutover."""

from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
import yaml

from ol_dlt.database import DatabaseSourceSpec
from ol_dlt.sources import learn_ai_app, mitlearn_app, ocw_studio_app, xpro_app

# src/ol_dlt/tests/sources/ -> repo root.
UNITS_DIR = Path(__file__).parents[4] / "ingestion" / "inventory" / "units"

SOURCES = [
    pytest.param(
        mitlearn_app,
        mitlearn_app.MITLEARN_APP_SPEC,
        "mitlearn__app_postgres",
        id="mitlearn_app",
    ),
    pytest.param(
        learn_ai_app,
        learn_ai_app.LEARN_AI_APP_SPEC,
        "learn_ai__app_postgres",
        id="learn_ai_app",
    ),
    pytest.param(
        xpro_app,
        xpro_app.XPRO_APP_SPEC,
        "xpro__app_postgres",
        id="xpro_app",
    ),
    pytest.param(
        ocw_studio_app,
        ocw_studio_app.OCW_STUDIO_APP_SPEC,
        "ocw__app_postgres",
        id="ocw_studio_app",
    ),
]


def _unit(stem: str) -> dict[str, Any]:
    return yaml.safe_load((UNITS_DIR / f"{stem}.yml").read_text())


@pytest.mark.parametrize(("module", "spec", "unit_stem"), SOURCES)
def test_spec_matches_the_modeled_tables_of_the_inventory_unit(
    module: ModuleType, spec: DatabaseSourceSpec, unit_stem: str
) -> None:
    """A model added against an unloaded table fails here, not as an empty QA build."""
    modeled = {
        table["name"] for table in _unit(unit_stem)["tables"] if table["modeled"]
    }
    assert {table.name for table in spec.tables} == modeled


@pytest.mark.parametrize(("module", "spec", "unit_stem"), SOURCES)
def test_primary_keys_match_the_inventory_unit(
    module: ModuleType, spec: DatabaseSourceSpec, unit_stem: str
) -> None:
    declared = {
        table["name"]: [column for path in table["primary_key"] for column in path]
        for table in _unit(unit_stem)["tables"]
    }
    for table in spec.tables:
        assert [table.primary_key] == declared[table.name], table.name


@pytest.mark.parametrize(("module", "spec", "unit_stem"), SOURCES)
def test_unit_names_this_source(
    module: ModuleType, spec: DatabaseSourceSpec, unit_stem: str
) -> None:
    """The `dlt:` block is what resolves the unit's QA dedup column to dlt's."""
    assert _unit(unit_stem)["dlt"]["source_module"] == module.__name__


@pytest.mark.parametrize(("module", "spec", "unit_stem"), SOURCES)
def test_no_table_declares_a_cursor(
    module: ModuleType, spec: DatabaseSourceSpec, unit_stem: str
) -> None:
    assert not [table.name for table in spec.tables if table.cursor_column]


@pytest.mark.parametrize(("module", "spec", "unit_stem"), SOURCES)
def test_resources_follow_the_raw_naming_convention(
    module: ModuleType, spec: DatabaseSourceSpec, unit_stem: str
) -> None:
    source = module.build_source()
    assert set(source.resources) == {
        spec.raw_table_name(table) for table in spec.tables
    }


@pytest.mark.parametrize(
    "spec",
    [
        mitlearn_app.MITLEARN_APP_SPEC,
        learn_ai_app.LEARN_AI_APP_SPEC,
        xpro_app.XPRO_APP_SPEC,
    ],
)
def test_password_hash_is_excluded(spec: DatabaseSourceSpec) -> None:
    """``users_user.password`` is a Django password hash, not analytical data."""
    users_user = next(table for table in spec.tables if table.name == "users_user")
    assert "password" in users_user.excluded_columns
