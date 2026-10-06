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


STG_USERS = "model.pkg.stg_users"
STG_COURSES = "model.pkg.stg_courses"

FK = "relationships_bridge_user_fk"
SINGULAR = "assert_users_have_roles"

# stg_users -> dim_user -> bridge_user_role, which holds dim_user's key.
# stg_courses -> dim_course_run. dim_course_run and dim_date are siblings:
# neither reads the other.
MANIFEST = {
    "nodes": {
        "test.pkg.not_null_dim_user": _test_node(
            "not_null_dim_user", DIM_USER, [DIM_USER]
        ),
        f"test.pkg.{FK}": _test_node(FK, BRIDGE, [BRIDGE, DIM_USER]),
        "test.pkg.relationships_bridge_stg_user": _test_node(
            "relationships_bridge_stg_user", BRIDGE, [BRIDGE, STG_USERS]
        ),
        "test.pkg.not_null_bridge_user_fk": _test_node(
            "not_null_bridge_user_fk", BRIDGE, [BRIDGE]
        ),
        "test.pkg.relationships_course_run_date": _test_node(
            "relationships_course_run_date", DIM_COURSE_RUN, [DIM_COURSE_RUN, DIM_DATE]
        ),
        f"test.pkg.{SINGULAR}": _test_node(SINGULAR, None, [BRIDGE, DIM_USER]),
        "test.pkg.accepted_values_no_attached_key": {
            "name": "accepted_values_no_attached_key",
            "resource_type": "test",
            "depends_on": {"nodes": [DIM_USER]},
        },
    }
    | {
        uid: {"name": uid.rsplit(".", 1)[-1], "resource_type": "model"}
        for uid in (STG_USERS, STG_COURSES, DIM_USER, BRIDGE, DIM_COURSE_RUN, DIM_DATE)
    },
    "child_map": {
        STG_USERS: [DIM_USER, "test.pkg.relationships_bridge_stg_user"],
        STG_COURSES: [DIM_COURSE_RUN],
        DIM_USER: [BRIDGE, "test.pkg.not_null_dim_user", f"test.pkg.{FK}"],
        BRIDGE: [f"test.pkg.{FK}", "test.pkg.not_null_bridge_user_fk"],
        DIM_COURSE_RUN: ["test.pkg.relationships_course_run_date"],
        DIM_DATE: ["test.pkg.relationships_course_run_date"],
    },
}


def test_a_dimension_only_run_skips_the_tests_on_its_stale_descendant():
    """The singular test has no attached model but reads the same stale bridge."""
    assert stale_descendant_test_names(MANIFEST, {DIM_USER}) == [SINGULAR, FK]


def test_the_fk_test_runs_when_its_holder_is_rebuilt():
    assert stale_descendant_test_names(MANIFEST, {BRIDGE}) == []
    assert stale_descendant_test_names(MANIFEST, {DIM_USER, BRIDGE}) == []


def test_a_descendant_more_than_one_hop_away_is_stale():
    assert stale_descendant_test_names(MANIFEST, {STG_USERS}) == [
        "relationships_bridge_stg_user"
    ]


def test_a_test_between_siblings_still_runs_when_either_is_selected():
    """Neither side was built from the other, so neither is stale."""
    assert stale_descendant_test_names(MANIFEST, {DIM_COURSE_RUN}) == []
    assert stale_descendant_test_names(MANIFEST, {DIM_DATE}) == []


def test_staleness_is_judged_against_the_tests_own_selected_parent():
    """dim_course_run is stale here, but not relative to dim_date."""
    assert stale_descendant_test_names(MANIFEST, {STG_COURSES, DIM_DATE}) == []


def test_a_test_the_run_selected_by_name_is_kept():
    assert stale_descendant_test_names(MANIFEST, {DIM_USER}, keep={FK}) == [SINGULAR]


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
