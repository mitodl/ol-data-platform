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
