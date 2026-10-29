"""dbt_automation_sensor and ocw_learn_automation_sensor must not share an asset.

Resolved against a synthetic manifest, because the real one is only parsed in
the image and nothing in CI imports definitions.py. For the same reason the
sensors' use of these targets is read from definitions.py statically.
"""

import ast
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import lakehouse
import pytest
from dagster import AssetKey, AssetSpec, Definitions, multi_asset
from dagster_dbt import DagsterDbtTranslator, dbt_assets
from lakehouse.lib.automation_targets import (
    OCW_LEARN_MODELS,
    dbt_automation_target,
    ocw_learn_target,
)
from lakehouse.lib.repo_paths import find_upward

# Model name -> config.schema, which DbtAutomationTranslator uses as the group.
# Each OCW model sits beside one that dbt_automation_sensor should keep, so an
# overlap in either layer shows up.
SCHEMAS = {
    "stg__ocw__s3__course_content": "staging",
    "int__ocw__live_courses": "intermediate",
    "int__ocw__content_files": "intermediate",
    "integrations__learn__ocw_courses": "integrations",
    "integrations__learn__ocw_content_files": "integrations",
    "stg__mitlearn__app__postgres__users": "staging",
    "int__mitlearn__users": "intermediate",
    "integrations__learn__programs": "integrations",
    "dim_user": "dimensional",
}
SUPERSET_DATASET = AssetKey(["superset", "dataset", "dim_user"])


def _key(name: str) -> AssetKey:
    return AssetKey([SCHEMAS[name], name])


class _SchemaGroupTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str | None:
        return dbt_resource_props["config"]["schema"]


def _manifest_node(name: str, schema: str) -> dict[str, Any]:
    return {
        "unique_id": f"model.pkg.{name}",
        "name": name,
        "resource_type": "model",
        "package_name": "pkg",
        "original_file_path": f"models/{name}.sql",
        "path": f"{name}.sql",
        "fqn": ["pkg", name],
        "schema": schema,
        "database": "db",
        "alias": name,
        "config": {
            "materialized": "table",
            "schema": schema,
            "meta": {},
            "tags": [],
            "enabled": True,
        },
        "meta": {},
        "tags": [],
        "depends_on": {"nodes": [], "macros": []},
        "description": "",
        "columns": {},
        "checksum": {"name": "sha256", "checksum": "x"},
        "raw_code": "select 1",
    }


def _manifest() -> dict[str, Any]:
    ids = [f"model.pkg.{name}" for name in SCHEMAS]
    return {
        "metadata": {"project_name": "pkg"},
        "nodes": {
            f"model.pkg.{name}": _manifest_node(name, schema)
            for name, schema in SCHEMAS.items()
        },
        "sources": {},
        "exposures": {},
        "metrics": {},
        "semantic_models": {},
        "saved_queries": {},
        "selectors": {},
        "unit_tests": {},
        "child_map": {node_id: [] for node_id in ids},
        "parent_map": {node_id: [] for node_id in ids},
        "group_map": {},
        "groups": {},
    }


@dbt_assets(manifest=_manifest(), dagster_dbt_translator=_SchemaGroupTranslator())
def project(): ...


@multi_asset(specs=[AssetSpec(SUPERSET_DATASET, group_name="superset_dataset")])
def superset_datasets(): ...


ASSET_GRAPH = Definitions(assets=[project, superset_datasets]).resolve_asset_graph()


def test_ocw_learn_target_is_the_ocw_models() -> None:
    # Pins the target to something, so disjointness below is not vacuous.
    assert ocw_learn_target(project).resolve(ASSET_GRAPH) == {
        _key(name) for name in OCW_LEARN_MODELS
    }


def test_the_two_targets_share_no_asset() -> None:
    dbt_target = dbt_automation_target(project).resolve(ASSET_GRAPH)
    ocw_target = ocw_learn_target(project).resolve(ASSET_GRAPH)

    assert dbt_target & ocw_target == set()


def test_dbt_automation_target_keeps_the_rest() -> None:
    assert dbt_automation_target(project).resolve(ASSET_GRAPH) == {
        _key("int__mitlearn__users"),
        _key("integrations__learn__programs"),
        _key("dim_user"),
        SUPERSET_DATASET,
    }


def test_every_ocw_learn_model_exists() -> None:
    # A renamed model drops out of the OCW selection without an error, and
    # dbt_automation_sensor quietly takes it back.
    models_dir = find_upward(
        Path(lakehouse.__file__), Path("src/ol_dbt/models"), Path("/nonexistent")
    )
    model_names = {path.stem for path in models_dir.rglob("*.sql")}

    assert set(OCW_LEARN_MODELS) <= model_names


def _sensor_targets() -> dict[str, str]:
    source = Path(lakehouse.__file__).parent.joinpath("definitions.py").read_text()
    return {
        str(node.args[0].value): ast.unparse(keyword.value)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "AutomationConditionSensorDefinition"
        and isinstance(node.args[0], ast.Constant)
        for keyword in node.keywords
        if keyword.arg == "target"
    }


@pytest.mark.parametrize(
    ("sensor_name", "target"),
    [
        ("dbt_automation_sensor", "dbt_automation_target(full_dbt_project)"),
        ("ocw_learn_automation_sensor", "ocw_learn_target(full_dbt_project)"),
    ],
)
def test_the_sensors_use_these_targets(sensor_name: str, target: str) -> None:
    # The tests above only hold for the sensors while they build their targets
    # here; a selection written inline in definitions.py would bypass them.
    assert _sensor_targets()[sensor_name] == target
