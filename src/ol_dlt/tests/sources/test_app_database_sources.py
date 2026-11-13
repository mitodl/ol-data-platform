"""Spec-vs-inventory tests for the app-database sources that load QA before cutover."""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dlt.database import DatabaseSourceSpec
from ol_dlt.sources import (
    edxapp,
    learn_ai_app,
    mitlearn_app,
    ocw_studio_app,
    xpro_app,
)

# src/ol_dlt/tests/sources/ -> repo root.
UNITS_DIR = Path(__file__).parents[4] / "ingestion" / "inventory" / "units"

SOURCES = [
    pytest.param(
        mitlearn_app.build_source,
        mitlearn_app.MITLEARN_APP_SPEC,
        "mitlearn__app_postgres",
        mitlearn_app.__name__,
        id="mitlearn_app",
    ),
    pytest.param(
        learn_ai_app.build_source,
        learn_ai_app.LEARN_AI_APP_SPEC,
        "learn_ai__app_postgres",
        learn_ai_app.__name__,
        id="learn_ai_app",
    ),
    pytest.param(
        xpro_app.build_source,
        xpro_app.XPRO_APP_SPEC,
        "xpro__app_postgres",
        xpro_app.__name__,
        id="xpro_app",
    ),
    pytest.param(
        ocw_studio_app.build_source,
        ocw_studio_app.OCW_STUDIO_APP_SPEC,
        "ocw__app_postgres",
        ocw_studio_app.__name__,
        id="ocw_studio_app",
    ),
    *(
        pytest.param(
            edxapp.source_builder(deployment),
            spec,
            f"{deployment}__mysql",
            edxapp.__name__,
            id=f"{deployment}_edxapp",
        )
        for deployment, spec in edxapp.EDXAPP_SPECS.items()
    ),
]
PARAMS = ("build_source", "spec", "unit_stem", "module_name")


def _unit(stem: str) -> dict[str, Any]:
    return yaml.safe_load((UNITS_DIR / f"{stem}.yml").read_text())


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_spec_matches_the_modeled_tables_of_the_inventory_unit(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    """A model added against an unloaded table fails here, not as an empty QA build."""
    modeled = {
        table["name"] for table in _unit(unit_stem)["tables"] if table["modeled"]
    }
    assert {table.name for table in spec.tables} == modeled


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_primary_keys_and_schemas_match_the_inventory_unit(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    declared = {table["name"]: table for table in _unit(unit_stem)["tables"]}
    for table in spec.tables:
        entry = declared[table.name]
        primary_key = [column for path in entry["primary_key"] for column in path]
        assert [table.primary_key] == primary_key, table.name
        # The inventory's `namespace` is the database (MySQL) or schema the
        # table lives in; the source reads it from the same place.
        namespace = entry.get("namespace")
        if namespace and spec.db_schema is None:
            assert (table.db_schema or spec.database) == namespace, table.name


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_unit_names_this_source(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    """The `dlt:` block is what resolves the unit's QA dedup column to dlt's."""
    assert _unit(unit_stem)["dlt"]["source_module"] == module_name


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_no_table_declares_a_cursor(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    assert not [table.name for table in spec.tables if table.cursor_column]


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_resources_follow_the_raw_naming_convention(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    assert set(build_source().resources) == {
        spec.raw_table_name(table) for table in spec.tables
    }


@pytest.mark.parametrize(
    ("spec", "table_name"),
    [
        (mitlearn_app.MITLEARN_APP_SPEC, "users_user"),
        (learn_ai_app.LEARN_AI_APP_SPEC, "users_user"),
        (xpro_app.XPRO_APP_SPEC, "users_user"),
        *((spec, "auth_user") for spec in edxapp.EDXAPP_SPECS.values()),
    ],
)
def test_password_hash_is_excluded(spec: DatabaseSourceSpec, table_name: str) -> None:
    """The Django password hash is credential material, not analytical data."""
    users = next(table for table in spec.tables if table.name == table_name)
    assert "password" in users.excluded_columns


@pytest.mark.parametrize(PARAMS, SOURCES)
def test_only_the_retired_ai_grading_tables_are_optional(
    build_source: Callable[[], Any],
    spec: DatabaseSourceSpec,
    unit_stem: str,
    module_name: str,
) -> None:
    """A skipped table loads nothing silently, so each one has to be named here."""
    optional = {table.name for table in spec.tables if table.optional}
    expected = (
        {
            "assessment_aiclassifier",
            "assessment_aiclassifierset",
            "assessment_aigradingworkflow",
            "assessment_aitrainingworkflow",
            "assessment_aitrainingworkflow_training_examples",
        }
        if unit_stem == "mitx__mysql"
        else set()
    )
    assert optional == expected
