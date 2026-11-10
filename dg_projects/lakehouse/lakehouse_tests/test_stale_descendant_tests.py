"""Tests for the tests a subset dbt build skips (#2571).

The selection half exercises `lakehouse.lib.stale_descendant_tests` directly.
The wiring half reads `assets/lakehouse/dbt.py` statically, for the reason
test_surrogate_key_drift.py gives.
"""

import ast
from pathlib import Path

import lakehouse
import pytest
from dagster import (
    AssetCheckKey,
    AssetCheckResult,
    AssetKey,
    Definitions,
    asset_check,
)
from dagster_dbt import build_dbt_asset_selection, dbt_assets
from lakehouse.lib.automation_targets import (
    dbt_job_target,
    learn_integrations_qa_target,
)
from lakehouse.lib.stale_descendant_tests import (
    stale_descendant_test_names,
    unselected_model_test_names,
)

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


def test_without_automation_a_test_on_an_unselected_sibling_is_skipped():
    """The QA case: dim_course_run is not built, so its table may not exist."""
    assert unselected_model_test_names(MANIFEST, {DIM_DATE}) == [
        "relationships_course_run_date"
    ]
    assert unselected_model_test_names(MANIFEST, {DIM_COURSE_RUN}) == []


def test_without_automation_the_stale_descendant_tests_are_still_skipped():
    assert unselected_model_test_names(MANIFEST, {DIM_USER}) == [SINGULAR, FK]
    assert unselected_model_test_names(MANIFEST, {DIM_USER}, keep={FK}) == [SINGULAR]
    assert unselected_model_test_names(MANIFEST, {DIM_USER, BRIDGE}) == []


def test_without_automation_a_selected_seed_counts_as_a_selected_parent():
    """Seeds and snapshots are assets too, so a run can select one alone."""
    seed, model, source = "seed.pkg.codes", "model.pkg.stg_codes", "source.pkg.raw.c"
    manifest = {
        "nodes": {
            "test.pkg.fk": _test_node("fk", model, [model, seed]),
            "test.pkg.not_null": _test_node("not_null", model, [model, source]),
        },
        "child_map": {seed: [model, "test.pkg.fk"], model: ["test.pkg.fk"]},
    }
    assert stale_descendant_test_names(manifest, {seed}) == ["fk"]
    assert unselected_model_test_names(manifest, {seed}) == ["fk"]


def test_without_automation_a_test_with_no_selected_parent_is_not_named():
    """Excluding a test dbt would not select only lengthens the command."""
    assert unselected_model_test_names(MANIFEST, {STG_COURSES}) == []


def _dbt_node(name: str, resource_type: str, **fields: object) -> dict[str, object]:
    return {
        "unique_id": f"{resource_type}.pkg.{name}",
        "name": name,
        "resource_type": resource_type,
        "package_name": "pkg",
        "original_file_path": f"models/{name}.sql",
        "path": f"{name}.sql",
        "fqn": ["pkg", name],
        "schema": "main",
        "database": "db",
        "alias": name,
        "config": {
            "materialized": "table",
            "meta": {},
            "tags": fields.get("tags", []),
            "enabled": True,
            "severity": "ERROR",
        },
        "meta": {},
        "tags": fields.get("tags", []),
        "depends_on": {"nodes": fields.get("parents", []), "macros": []},
        "description": "",
        "columns": {},
        "checksum": {"name": "sha256", "checksum": "x"},
        "raw_code": "select 1",
        **({"attached_node": fields["attached"]} if "attached" in fields else {}),
    }


# The QA shape: qa_view is tagged and reads stg_programs; int_requirements is
# built from stg_programs but nothing tagged reads it, and it holds an FK test
# against stg_programs.
QA_VIEW, STG_PROGRAMS, UNBUILT = (
    "model.pkg.qa_view",
    "model.pkg.stg_programs",
    "model.pkg.int_requirements",
)
QA_FK = "relationships_int_requirements_program_id"
QA_NOT_NULL = "not_null_qa_view_id"
QA_MANIFEST = {
    "metadata": {"project_name": "pkg"},
    "nodes": {
        QA_VIEW: _dbt_node(
            "qa_view", "model", tags=["qa_scheduled"], parents=[STG_PROGRAMS]
        ),
        STG_PROGRAMS: _dbt_node("stg_programs", "model"),
        UNBUILT: _dbt_node("int_requirements", "model", parents=[STG_PROGRAMS]),
        f"test.pkg.{QA_FK}": _dbt_node(
            QA_FK, "test", parents=[STG_PROGRAMS, UNBUILT], attached=UNBUILT
        ),
        f"test.pkg.{QA_NOT_NULL}": _dbt_node(
            QA_NOT_NULL, "test", parents=[QA_VIEW], attached=QA_VIEW
        ),
    },
    "sources": {},
    "exposures": {},
    "metrics": {},
    "semantic_models": {},
    "saved_queries": {},
    "selectors": {},
    "unit_tests": {},
    "child_map": {
        QA_VIEW: [f"test.pkg.{QA_NOT_NULL}"],
        STG_PROGRAMS: [QA_VIEW, UNBUILT, f"test.pkg.{QA_FK}"],
        UNBUILT: [f"test.pkg.{QA_FK}"],
        f"test.pkg.{QA_FK}": [],
        f"test.pkg.{QA_NOT_NULL}": [],
    },
    "parent_map": {
        QA_VIEW: [STG_PROGRAMS],
        STG_PROGRAMS: [],
        UNBUILT: [STG_PROGRAMS],
        f"test.pkg.{QA_FK}": [STG_PROGRAMS, UNBUILT],
        f"test.pkg.{QA_NOT_NULL}": [QA_VIEW],
    },
    "group_map": {},
    "groups": {},
}


@dbt_assets(manifest=QA_MANIFEST)
def qa_project(): ...


@asset_check(asset=AssetKey("qa_view"))
def qa_view_is_fresh() -> AssetCheckResult:
    return AssetCheckResult(passed=True)


QA_GRAPH = Definitions(
    assets=[qa_project], asset_checks=[qa_view_is_fresh]
).resolve_asset_graph()


def test_a_bare_dbt_selection_selects_a_check_on_an_unselected_model():
    """The premise: if this stops holding, the QA target can go back to a bare one."""
    selection = build_dbt_asset_selection([qa_project], dbt_select="+tag:qa_scheduled")

    assert AssetCheckKey(AssetKey("int_requirements"), QA_FK) in (
        selection.resolve_checks(QA_GRAPH)
    )


def test_the_qa_job_selects_only_the_dbt_tests_on_the_models_it_builds():
    """No check on an unbuilt model, none lost on a built one, none from outside dbt."""
    selection = learn_integrations_qa_target(qa_project)

    assert selection.resolve(QA_GRAPH) == {
        AssetKey("qa_view"),
        AssetKey("stg_programs"),
    }
    assert selection.resolve_checks(QA_GRAPH) == {
        AssetCheckKey(AssetKey("qa_view"), QA_NOT_NULL)
    }


def test_the_qa_job_excludes_the_test_on_the_model_it_does_not_build():
    """Dbt still selects it through stg_programs, so the exclusion has to name it."""
    keep = {
        key.name
        for key in learn_integrations_qa_target(qa_project).resolve_checks(QA_GRAPH)
    }

    assert unselected_model_test_names(
        QA_MANIFEST, {QA_VIEW, STG_PROGRAMS}, keep=keep
    ) == [QA_FK]


def test_the_staging_job_excludes_the_test_on_the_descendant_it_does_not_build():
    """The production shape: staging rebuilt, its descendants left to automation.

    A bare selection of stg_programs selects the FK test attached to
    int_requirements, so the exclusion keeps it and it compares the new
    stg_programs with the int_requirements the previous tick built.
    """
    selection = dbt_job_target(qa_project, "stg_programs")
    keep = {key.name for key in selection.resolve_checks(QA_GRAPH)}

    assert selection.resolve(QA_GRAPH) == {AssetKey("stg_programs")}
    assert keep == set()
    assert stale_descendant_test_names(QA_MANIFEST, {STG_PROGRAMS}, keep=keep) == [
        QA_FK
    ]


@pytest.mark.parametrize(
    ("job_name", "selection"),
    [
        (
            "learn_integrations_qa_job",
            "learn_integrations_qa_target(full_dbt_project)",
        ),
        (
            "non_airbyte_staging_job",
            "dbt_job_target(full_dbt_project, ' '.join(non_airbyte_staging_models))",
        ),
    ],
)
def test_the_scheduled_dbt_jobs_use_the_target(job_name, selection):
    source = Path(lakehouse.__file__).parent.joinpath("definitions.py").read_text()
    selections = [
        ast.unparse(keyword.value)
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", "") == "define_asset_job"
        and any(
            keyword.arg == "name" and getattr(keyword.value, "value", "") == job_name
            for keyword in node.keywords
        )
        for keyword in node.keywords
        if keyword.arg == "selection"
    ]
    assert selections == [selection]


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
    exclusion = ast.unparse(functions["_stale_descendant_test_args"])
    assert "context.is_subset" in exclusion
    assert (
        "stale_descendant_test_names if DBT_AUTOMATION_ENABLED "
        "else unselected_model_test_names"
    ) in exclusion
