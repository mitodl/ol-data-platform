"""Tests for lib/manifest.py — macro dependency mapping."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ol_dbt_cli.lib.manifest import load_manifest


def _write_manifest(tmp_path: Path, nodes: dict[str, Any], macros: dict[str, Any]) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes, "sources": {}, "macros": macros}))
    return path


def _model_node(name: str, macros: list[str]) -> dict[str, Any]:
    return {
        "unique_id": f"model.open_learning.{name}",
        "name": name,
        "resource_type": "model",
        "original_file_path": f"models/{name}.sql",
        "schema": "s",
        "database": "d",
        "columns": {},
        "depends_on": {"nodes": [], "macros": macros},
    }


def _macro_node(name: str, file: str, macros: list[str]) -> tuple[str, dict[str, Any]]:
    uid = f"macro.open_learning.{name}"
    return uid, {
        "unique_id": uid,
        "name": name,
        "package_name": "open_learning",
        "original_file_path": file,
        "depends_on": {"macros": macros},
    }


def _node(
    unique_id: str,
    name: str,
    resource_type: str,
    *,
    depends_on: list[str] | None = None,
    columns: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    node = {
        "unique_id": unique_id,
        "name": name,
        "resource_type": resource_type,
        "original_file_path": f"models/{name}.sql",
        "schema": "s",
        "database": "d",
        "columns": columns or {},
        "depends_on": {"nodes": depends_on or [], "macros": []},
    }
    if extra:
        node.update(extra)
    return node


def _write_full_manifest(
    tmp_path: Path,
    nodes: dict[str, Any],
    sources: dict[str, Any] | None = None,
    macros: dict[str, Any] | None = None,
) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes, "sources": sources or {}, "macros": macros or {}}))
    return path


class TestNodeLoadingAndFiltering:
    """Realistic manifest with a schema test, a seed, and a source dependency."""

    def _registry(self, tmp_path: Path) -> Any:
        # A model that depends on a source, a seed (which lives under "nodes"),
        # and a schema test attached to the model (also under "nodes").
        source_uid = "source.open_learning.raw.users"
        seed_uid = "seed.open_learning.country_codes"
        model_uid = "model.open_learning.stg_users"
        test_uid = "test.open_learning.not_null_stg_users_user_id.abc123"
        nodes = {
            seed_uid: _node(seed_uid, "country_codes", "seed", columns={"code": {"data_type": "varchar"}}),
            model_uid: _node(
                model_uid,
                "stg_users",
                "model",
                depends_on=[source_uid, seed_uid],
                columns={"user_id": {"data_type": "integer"}},
            ),
            test_uid: _node(test_uid, "not_null_stg_users_user_id", "test", depends_on=[model_uid]),
        }
        sources = {
            source_uid: {
                "unique_id": source_uid,
                "name": "users",
                "source_name": "raw",
                "resource_type": "source",
                "original_file_path": "models/_sources.yml",
                "schema": "raw",
                "database": "d",
                "columns": {"user_id": {"data_type": "integer"}},
                "depends_on": {"nodes": [], "macros": []},
            }
        }
        return load_manifest(_write_full_manifest(tmp_path, nodes=nodes, sources=sources))

    def test_seed_indexed_by_name(self, tmp_path: Path) -> None:
        registry = self._registry(tmp_path)
        seed = registry.get_model("country_codes")
        assert seed is not None
        assert seed.resource_type == "seed"

    def test_source_indexed_by_source_key(self, tmp_path: Path) -> None:
        registry = self._registry(tmp_path)
        assert registry.get_source("raw.users") is not None

    def test_test_node_loaded_but_not_a_model(self, tmp_path: Path) -> None:
        registry = self._registry(tmp_path)
        test_node = registry.get_node("test.open_learning.not_null_stg_users_user_id.abc123")
        assert test_node is not None
        assert not test_node.is_model

    def test_get_model_children_excludes_test_nodes(self, tmp_path: Path) -> None:
        registry = self._registry(tmp_path)
        # The model has a schema test as a child; it must not appear among model children.
        children = registry.get_model_children("model.open_learning.stg_users")
        assert children == []
        # But the raw get_children still surfaces it (test node is a real child).
        raw_children = {c.name for c in registry.get_children("model.open_learning.stg_users")}
        assert "not_null_stg_users_user_id" in raw_children

    def test_model_child_of_source_is_included(self, tmp_path: Path) -> None:
        registry = self._registry(tmp_path)
        children = registry.get_model_children("source.open_learning.raw.users")
        assert [c.name for c in children] == ["stg_users"]


class TestModelsForChangedMacros:
    def test_direct_dependency_flags_model(self, tmp_path: Path) -> None:
        uid, macro = _macro_node("extract_course_id", "macros/extract_course_id.sql", [])
        manifest = load_manifest(
            _write_manifest(
                tmp_path,
                nodes={"model.open_learning.stg_a": _model_node("stg_a", [uid])},
                macros={uid: macro},
            )
        )
        assert manifest.models_for_changed_macros({"macros/extract_course_id.sql"}) == {"stg_a"}

    def test_transitive_macro_chain_flags_model(self, tmp_path: Path) -> None:
        # helper is called only by wrapper; model depends only on wrapper directly.
        helper_uid, helper = _macro_node("helper", "macros/helper.sql", [])
        wrapper_uid, wrapper = _macro_node("wrapper", "macros/wrapper.sql", [helper_uid])
        manifest = load_manifest(
            _write_manifest(
                tmp_path,
                nodes={"model.open_learning.stg_a": _model_node("stg_a", [wrapper_uid])},
                macros={helper_uid: helper, wrapper_uid: wrapper},
            )
        )
        # Changing the low-level helper must still flag stg_a via the wrapper.
        assert manifest.models_for_changed_macros({"macros/helper.sql"}) == {"stg_a"}

    def test_unrelated_macro_change_flags_nothing(self, tmp_path: Path) -> None:
        used_uid, used = _macro_node("used", "macros/used.sql", [])
        unused_uid, unused = _macro_node("unused", "macros/unused.sql", [])
        manifest = load_manifest(
            _write_manifest(
                tmp_path,
                nodes={"model.open_learning.stg_a": _model_node("stg_a", [used_uid])},
                macros={used_uid: used, unused_uid: unused},
            )
        )
        assert manifest.models_for_changed_macros({"macros/unused.sql"}) == set()

    def test_empty_changed_set(self, tmp_path: Path) -> None:
        uid, macro = _macro_node("m", "macros/m.sql", [])
        manifest = load_manifest(
            _write_manifest(
                tmp_path,
                nodes={"model.open_learning.stg_a": _model_node("stg_a", [uid])},
                macros={uid: macro},
            )
        )
        assert manifest.models_for_changed_macros(set()) == set()

    def test_multiple_macros_one_file(self, tmp_path: Path) -> None:
        # Two macros defined in one .sql file, each used by a different model.
        a_uid, a = _macro_node("a", "macros/shared.sql", [])
        b_uid, b = _macro_node("b", "macros/shared.sql", [])
        manifest = load_manifest(
            _write_manifest(
                tmp_path,
                nodes={
                    "model.open_learning.m_a": _model_node("m_a", [a_uid]),
                    "model.open_learning.m_b": _model_node("m_b", [b_uid]),
                },
                macros={a_uid: a, b_uid: b},
            )
        )
        assert manifest.models_for_changed_macros({"macros/shared.sql"}) == {"m_a", "m_b"}
