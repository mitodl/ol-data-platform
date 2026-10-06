"""Tests for the tests a subset dbt build skips (#2571).

The selection half exercises `lakehouse.lib.stale_descendant_tests` directly.
The wiring half reads `assets/lakehouse/dbt.py` statically, for the reason
test_surrogate_key_drift.py gives.
"""

import ast
from pathlib import Path

import lakehouse
from lakehouse.lib.stale_descendant_tests import stale_descendant_test_names

DIM_USER = "model.pkg.dim_user"
BRIDGE = "model.pkg.bridge_user_role"
DIM_COURSE_RUN = "model.pkg.dim_course_run"
DIM_DATE = "model.pkg.dim_date"


def _test_node(
    name: str, attached_node: str | None, parents: list[str]
) -> dict[str, object]:
    return {
        "name": name,
        "resource_type": "test",
        "attached_node": attached_node,
        "depends_on": {"nodes": parents},
    }


# bridge_user_role is built from dim_user and holds its key; dim_course_run and
# dim_date are siblings that neither read the other.
MANIFEST = {
    "nodes": {
        DIM_USER: {"name": "dim_user", "resource_type": "model"},
        BRIDGE: {"name": "bridge_user_role", "resource_type": "model"},
        DIM_COURSE_RUN: {"name": "dim_course_run", "resource_type": "model"},
        DIM_DATE: {"name": "dim_date", "resource_type": "model"},
        "test.pkg.not_null_dim_user": _test_node(
            "not_null_dim_user", DIM_USER, [DIM_USER]
        ),
        "test.pkg.relationships_bridge_user_fk": _test_node(
            "relationships_bridge_user_fk", BRIDGE, [BRIDGE, DIM_USER]
        ),
        "test.pkg.not_null_bridge_user_fk": _test_node(
            "not_null_bridge_user_fk", BRIDGE, [BRIDGE]
        ),
        "test.pkg.relationships_course_run_date": _test_node(
            "relationships_course_run_date", DIM_COURSE_RUN, [DIM_COURSE_RUN, DIM_DATE]
        ),
        "test.pkg.assert_users_have_roles": _test_node(
            "assert_users_have_roles", None, [BRIDGE, DIM_USER]
        ),
    },
    "child_map": {
        DIM_USER: [
            BRIDGE,
            "test.pkg.not_null_dim_user",
            "test.pkg.relationships_bridge_user_fk",
            "test.pkg.assert_users_have_roles",
        ],
        BRIDGE: [
            "test.pkg.relationships_bridge_user_fk",
            "test.pkg.not_null_bridge_user_fk",
            "test.pkg.assert_users_have_roles",
        ],
        DIM_COURSE_RUN: ["test.pkg.relationships_course_run_date"],
        DIM_DATE: ["test.pkg.relationships_course_run_date"],
    },
}


def test_a_dimension_only_run_skips_the_fk_test_on_its_stale_descendant():
    assert stale_descendant_test_names(MANIFEST, {DIM_USER}) == [
        "relationships_bridge_user_fk"
    ]


def test_the_fk_test_runs_when_its_holder_is_rebuilt():
    assert stale_descendant_test_names(MANIFEST, {BRIDGE}) == []
    assert stale_descendant_test_names(MANIFEST, {DIM_USER, BRIDGE}) == []


def test_a_test_between_siblings_still_runs_when_either_is_selected():
    """Neither side was built from the other, so neither is stale."""
    assert stale_descendant_test_names(MANIFEST, {DIM_COURSE_RUN}) == []
    assert stale_descendant_test_names(MANIFEST, {DIM_DATE}) == []


def test_the_exclusion_reaches_the_build_only_for_a_subset_run():
    source = Path(lakehouse.__file__).parent / "assets" / "lakehouse" / "dbt.py"
    functions = {
        node.name: node
        for node in ast.walk(ast.parse(source.read_text()))
        if isinstance(node, ast.FunctionDef)
    }
    build_calls = [
        node
        for node in ast.walk(functions["full_dbt_project"])
        if isinstance(node, ast.Call) and getattr(node.func, "attr", "") == "cli"
    ]
    assert "_stale_descendant_test_args" in ast.unparse(build_calls[0])
    assert "context.is_subset" in ast.unparse(functions["_stale_descendant_test_args"])
