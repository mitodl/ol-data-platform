"""Tests for the surrogate-key drift check and its escalation in full_dbt_project.

The behavioural half exercises `lakehouse.lib.surrogate_key_drift` directly.
The ordering half reads `assets/lakehouse/dbt.py` statically, for the reason
test_definitions_schedule_ids.py gives: importing that module evaluates a
`@dbt_assets` decorator that needs /opt/dbt and a parsed manifest, which only
the container has. The invariants it checks are ones a wrong edit would
otherwise only break in production — a repair that runs before the build it is
repairing, or a state file written when the repair failed.
"""

import ast
from pathlib import Path

import lakehouse
import pytest
from lakehouse.lib.surrogate_key_drift import (
    SURROGATE_KEY_STATE_ARTIFACT,
    detect_drift,
    full_refresh_build_args,
)

# One re-keyed dimension (dim_thing.thing_pk) and one incremental fact that
# stores it, declared the way dbt records a `relationships` test.
MANIFEST = {
    "nodes": {
        "model.pkg.dim_thing": {
            "unique_id": "model.pkg.dim_thing",
            "name": "dim_thing",
            "resource_type": "model",
            "original_file_path": "models/dim_thing.sql",
            "config": {"materialized": "table"},
            "raw_code": (
                "{{ dbt_utils.generate_surrogate_key(['a', 'b']) }} as thing_pk"
            ),
            "depends_on": {"nodes": []},
            "columns": {},
        },
        "model.pkg.fact_thing": {
            "unique_id": "model.pkg.fact_thing",
            "name": "fact_thing",
            "resource_type": "model",
            "original_file_path": "models/fact_thing.sql",
            "config": {"materialized": "incremental"},
            "raw_code": "select thing_pk as thing_fk from {{ ref('dim_thing') }}",
            "depends_on": {"nodes": ["model.pkg.dim_thing"]},
            "columns": {"thing_fk": {}},
        },
        "test.pkg.relationships_fact_thing": {
            "unique_id": "test.pkg.relationships_fact_thing",
            "name": "relationships_fact_thing",
            "resource_type": "test",
            "original_file_path": "models/_schema.yml",
            "attached_node": "model.pkg.fact_thing",
            "column_name": "thing_fk",
            "config": {"materialized": "test"},
            "test_metadata": {
                "name": "relationships",
                "kwargs": {
                    "column_name": "thing_fk",
                    "to": "ref('dim_thing')",
                    "field": "thing_pk",
                },
            },
            "depends_on": {"nodes": []},
        },
    }
}

MATCHING_STATE = {"dim_thing": {"thing_pk": [["a", "b"]]}}
PRIOR_STATE = {"dim_thing": {"thing_pk": [["a"]]}}


def test_no_baseline_records_state_without_escalating():
    """The first run has nothing to compare against and must not rebuild the world."""
    drift = detect_drift(MANIFEST, None)
    assert drift.models == []
    assert drift.current_state == MATCHING_STATE


def test_unchanged_keys_escalate_nothing():
    assert detect_drift(MANIFEST, MATCHING_STATE).models == []


def test_a_re_key_escalates_the_incremental_models_holding_it():
    drift = detect_drift(MANIFEST, PRIOR_STATE)
    assert drift.models == ["fact_thing"]
    assert drift.current_state == MATCHING_STATE
    assert "dim_thing.thing_pk re-keyed" in drift.describe()
    assert "fact_thing" in drift.describe()


def test_state_recorded_after_a_repair_stops_it_repeating():
    """The escalation fires once: the state it writes matches on the next run."""
    first = detect_drift(MANIFEST, PRIOR_STATE)
    assert first.models == ["fact_thing"]
    assert detect_drift(MANIFEST, first.current_state).models == []


BOTH_BUILT = {"dim_thing", "fact_thing"}


def test_a_run_that_built_both_sides_repairs_and_records():
    models, complete = detect_drift(MANIFEST, PRIOR_STATE).resolved_against(BOTH_BUILT)
    assert models == ["fact_thing"]
    assert complete is True


def test_a_run_that_skipped_the_re_keyed_dimension_repairs_nothing():
    """Refreshing the fact now would just re-copy the old keys and call it fixed."""
    models, complete = detect_drift(MANIFEST, PRIOR_STATE).resolved_against(
        {"fact_thing"}
    )
    assert models == []
    assert complete is False


def test_a_run_that_skipped_an_affected_fact_withholds_the_state():
    models, complete = detect_drift(MANIFEST, PRIOR_STATE).resolved_against(
        {"dim_thing"}
    )
    assert models == []
    assert complete is False


def test_a_clean_run_records_the_baseline_whatever_it_built():
    models, complete = detect_drift(MANIFEST, MATCHING_STATE).resolved_against(set())
    assert models == []
    assert complete is True


def test_full_refresh_selector_is_one_space_separated_argument():
    assert full_refresh_build_args(["fact_a", "fact_b"]) == [
        "build",
        "--full-refresh",
        "--select",
        "fact_a fact_b",
    ]


def test_full_refresh_carries_the_build_vars_through():
    """A repair against a different schema rebuilds the wrong relations."""
    args = full_refresh_build_args(["fact_a"], ["--vars", "schema_suffix: dev"])
    assert args[-2:] == ["--vars", "schema_suffix: dev"]


# ---------------------------------------------------------------------------
# Ordering invariants in the asset
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def dbt_asset_module() -> ast.Module:
    source = Path(lakehouse.__file__).parent / "assets" / "lakehouse" / "dbt.py"
    return ast.parse(source.read_text())


def _function(module: ast.Module, name: str) -> ast.FunctionDef:
    found = [
        n for n in ast.walk(module) if isinstance(n, ast.FunctionDef) and n.name == name
    ]
    assert found, f"{name} is no longer defined in dbt.py"
    return found[0]


def _called_names(node: ast.AST) -> list[tuple[int, str]]:
    """``(line, callee)`` for every call under *node*, in source order."""
    calls: list[tuple[int, str]] = []
    for child in ast.walk(node):
        if not isinstance(child, ast.Call):
            continue
        func = child.func
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", "")
        calls.append((child.lineno, name))
    return sorted(calls)


def _first_call_lines(node: ast.AST) -> dict[str, int]:
    """``{callee: first line it is called on}``."""
    lines: dict[str, int] = {}
    for line, name in _called_names(node):
        lines.setdefault(name, line)
    return lines


def test_the_repair_runs_after_the_build_it_repairs(dbt_asset_module):
    """The dimension has to carry its new keys before the facts are rebuilt.

    Repairing first would rebuild them against the previous keys and orphan
    every FK again — a green run that fixed nothing.
    """
    lines = _first_call_lines(_function(dbt_asset_module, "full_dbt_project"))
    assert lines["_surrogate_key_drift"] < lines["stream"]
    assert lines["stream"] < lines["_repair_surrogate_key_drift"]


def test_state_is_written_only_after_a_complete_repair(dbt_asset_module):
    """A failed or partial repair must leave the drift pending, not marked handled."""
    asset = _function(dbt_asset_module, "full_dbt_project")
    lines = _first_call_lines(asset)
    assert lines["_repair_surrogate_key_drift"] < lines["write_json_artifact"]

    writes = [
        node
        for node in ast.walk(asset)
        if isinstance(node, ast.If)
        and "write_json_artifact" in {name for _, name in _called_names(node)}
    ]
    assert writes, "the key state write must stay conditional"
    # Gated on the repair's own return value, so a repair that raised or
    # reported an incomplete run can never reach the write.
    assert "_repair_surrogate_key_drift" in {
        name for _, name in _called_names(writes[0].test)
    }

    repair = _function(dbt_asset_module, "_repair_surrogate_key_drift")
    guarded = [node for node in repair.body if isinstance(node, ast.If)]
    assert guarded, (
        "the full-refresh build must stay conditional on the resolved models"
    )
    assert "full_refresh_build_args" in {name for _, name in _called_names(guarded[0])}


def test_the_repair_is_scoped_to_what_this_run_actually_built(dbt_asset_module):
    """A subset build must not mark drift handled for models it never touched."""
    repair = _function(dbt_asset_module, "_repair_surrogate_key_drift")
    assert "resolved_against" in {name for _, name in _called_names(repair)}
    assert "built" in {arg.arg for arg in repair.args.args}

    asset = _function(dbt_asset_module, "full_dbt_project")
    assert "_models_built_by" in {name for _, name in _called_names(asset)}


def test_the_state_artifact_name_is_stable():
    """Renaming it silently orphans the baseline and re-fires every escalation."""
    assert SURROGATE_KEY_STATE_ARTIFACT == "surrogate-key-state.json"
