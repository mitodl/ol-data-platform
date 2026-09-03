"""Tests for surrogate-key hash-input extraction and drift detection."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ol_dbt_cli.lib.manifest import (
    ForeignKeyRef,
    ManifestColumn,
    ManifestModel,
    ManifestRegistry,
    registry_from_manifest,
)
from ol_dbt_cli.lib.surrogate_keys import (
    affected_incremental_descendants,
    changed_keys_from_state,
    changed_surrogate_keys,
    detect_key_regen,
    extract_surrogate_keys,
    surrogate_key_inputs,
    surrogate_key_state,
)

FIXTURES = Path(__file__).parent / "fixtures" / "surrogate_keys"


@pytest.fixture(scope="module")
def dimensional_registry() -> ManifestRegistry:
    """Real lineage: every dimensional model plus its relationships tests.

    Extracted mechanically from a `dbt parse` of the project (all models under
    models/dimensional/, all relationships tests attached to one), carrying
    only the fields the detector reads. Nothing in it was selected per
    incident, so the regression tests below are not being handed their answer.
    """
    return registry_from_manifest(json.loads((FIXTURES / "dimensional_manifest.json").read_text()))


def fixture_sql(name: str) -> str:
    return (FIXTURES / name).read_text()


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def test_extracts_inputs_and_alias():
    keys = extract_surrogate_keys(
        "select {{ dbt_utils.generate_surrogate_key(['order_id', 'platform']) }} as order_key"
    )
    assert [(k.column, k.inputs) for k in keys] == [("order_key", ("order_id", "platform"))]


def test_extracts_unqualified_macro_call():
    keys = extract_surrogate_keys("select {{ generate_surrogate_key(['a']) }} as pk")
    assert keys[0].column == "pk"


def test_input_order_is_preserved():
    """Order is part of the hash, so a reordered list is a different key."""
    forward = surrogate_key_inputs("{{ generate_surrogate_key(['a', 'b']) }} as pk")
    reversed_ = surrogate_key_inputs("{{ generate_surrogate_key(['b', 'a']) }} as pk")
    assert forward != reversed_


def test_parenthesised_and_multiline_expressions():
    sql = """
        {{ dbt_utils.generate_surrogate_key([
            'cast(source_discount_id as varchar)',
            'platform_code'
        ]) }} as discount_pk
    """
    assert extract_surrogate_keys(sql)[0].inputs == (
        "cast(source_discount_id as varchar)",
        "platform_code",
    )


def test_reindenting_and_recasing_do_not_count_as_a_re_key():
    """Reformatting a dimension must not full-refresh its fact tables."""
    tidy = "{{ generate_surrogate_key(['cast(id as varchar)', 'platform']) }} as pk"
    reformatted = "{{ generate_surrogate_key([\n    'CAST(id AS varchar)',\n    'Platform'\n]) }} as pk"
    assert changed_surrogate_keys(tidy, reformatted) == []


def test_a_changed_sql_string_literal_is_a_re_key():
    """`concat(kind, 'A')` and `concat(kind, 'a')` hash differently.

    Folding case across the whole expression would equate them and miss the
    exact re-key this detector exists to catch.
    """
    change = changed_surrogate_keys(
        "{{ generate_surrogate_key([\"concat(kind, 'A')\"]) }} as pk",
        "{{ generate_surrogate_key([\"concat(kind, 'a')\"]) }} as pk",
    )
    assert [c.column for c in change] == ["pk"]


def test_whitespace_inside_a_sql_literal_is_a_re_key():
    change = changed_surrogate_keys(
        "{{ generate_surrogate_key([\"concat(a, ', ', b)\"]) }} as pk",
        "{{ generate_surrogate_key([\"concat(a, ',', b)\"]) }} as pk",
    )
    assert [c.column for c in change] == ["pk"]


def test_a_quoted_identifier_keeps_its_case():
    """Trino folds a bare identifier and preserves a quoted one."""
    change = changed_surrogate_keys(
        "{{ generate_surrogate_key(['cast(\"Col\" as varchar)']) }} as pk",
        "{{ generate_surrogate_key(['cast(\"col\" as varchar)']) }} as pk",
    )
    assert [c.column for c in change] == ["pk"]


def test_recasing_outside_a_literal_is_still_not_a_re_key():
    """The fold that keeps a reindent quiet must survive the literal-awareness."""
    change = changed_surrogate_keys(
        "{{ generate_surrogate_key([\"CONCAT(KIND, 'A')\"]) }} as pk",
        "{{ generate_surrogate_key([\"concat(kind, 'A')\"]) }} as pk",
    )
    assert change == []


def test_a_doubled_quote_stays_inside_its_literal():
    """`''` is SQL's escape for a quote, not a close followed by a reopen."""
    keys = extract_surrogate_keys("{{ generate_surrogate_key([\"concat(a, 'it''s')\"]) }} as pk")
    assert keys[0].inputs == ("concat(a, 'it''s')",)


def test_an_unterminated_quote_normalizes_without_raising():
    keys = extract_surrogate_keys('{{ generate_surrogate_key(["concat(a, \'x)"]) }} as pk')
    assert keys[0].column == "pk"


def test_unaliased_calls_are_dropped():
    """A key minted inside a join predicate defines no column to go stale."""
    sql = """
        select 1
        from a join b
          on a.k = {{ dbt_utils.generate_surrogate_key(['b.id', 'b.platform']) }}
    """
    assert extract_surrogate_keys(sql)[0].column == ""
    assert surrogate_key_inputs(sql) == {}


def test_repeated_alias_keeps_every_call():
    """A key minted per union branch has to compare branch by branch."""
    sql = "select {{ generate_surrogate_key(['a']) }} as pk union all select {{ generate_surrogate_key(['b']) }} as pk"
    assert surrogate_key_inputs(sql) == {"pk": (("a",), ("b",))}


def test_adjacent_literals_are_one_argument():
    """Jinja concatenates them, so `['O' 'Brien']` reaches dbt as one value.

    Reading them as two would make it compare equal to `['O', 'Brien']`, which
    hashes differently -- one input versus two -- and a re-key between the two
    spellings would go unreported.
    """
    joined = extract_surrogate_keys("{{ generate_surrogate_key(['O' 'Brien']) }} as pk")
    assert joined[0].inputs == ("obrien",)

    separate = extract_surrogate_keys("{{ generate_surrogate_key(['O', 'Brien']) }} as pk")
    assert separate[0].inputs == ("o", "brien")

    assert changed_surrogate_keys(
        "{{ generate_surrogate_key(['O' 'Brien']) }} as pk",
        "{{ generate_surrogate_key(['O', 'Brien']) }} as pk",
    )


def test_doubled_quotes_at_the_argument_level_concatenate():
    """`['O''Brien']` is two adjacent literals to Jinja, not a SQL escape."""
    keys = extract_surrogate_keys("{{ generate_surrogate_key(['O''Brien']) }} as pk")
    assert keys[0].inputs == ("obrien",)


def test_unbalanced_call_is_skipped_not_raised():
    assert extract_surrogate_keys("{{ generate_surrogate_key(['a'] as pk") == []


# ---------------------------------------------------------------------------
# Diffing
# ---------------------------------------------------------------------------


def test_added_and_removed_key_columns_are_not_reported():
    """Both are ordinary column changes with their own, louder failure mode."""
    assert changed_surrogate_keys("select 1", "{{ generate_surrogate_key(['a']) }} as pk") == []
    assert changed_surrogate_keys("{{ generate_surrogate_key(['a']) }} as pk", "select 1") == []


def test_changed_inputs_are_reported():
    change = changed_surrogate_keys(
        "{{ generate_surrogate_key(['a', 'b']) }} as pk",
        "{{ generate_surrogate_key(['a']) }} as pk",
    )
    assert [(c.column, c.base_inputs, c.current_inputs) for c in change] == [("pk", (("a", "b"),), (("a",),))]


# ---------------------------------------------------------------------------
# Downstream tracing
# ---------------------------------------------------------------------------


def _model(name: str, materialized: str, parents: list[str] | None = None) -> ManifestModel:
    return ManifestModel(
        unique_id=f"model.pkg.{name}",
        name=name,
        resource_type="model",
        original_file_path=f"models/{name}.sql",
        schema="dim",
        database="db",
        materialized=materialized,
        columns={},
        depends_on=[f"model.pkg.{p}" for p in parents or []],
    )


def _registry(*models: ManifestModel) -> ManifestRegistry:
    registry = ManifestRegistry()
    for model in models:
        registry.nodes[model.unique_id] = model
        registry.by_name[model.name] = model
    for model in models:
        for parent in model.depends_on:
            registry.children.setdefault(parent, []).append(model.unique_id)
    return registry


def test_only_incremental_descendants_are_flagged():
    dim = _model("dim_thing", "table")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    bridge = _model("bridge_thing", "table", ["dim_thing"])
    registry = _registry(dim, fact, bridge)
    reads = {"fact_thing": {"thing_pk"}, "bridge_thing": {"thing_pk"}}

    affected = affected_incremental_descendants(registry, dim, "thing_pk", lambda child, _parent: reads.get(child))
    assert [m.model_name for m in affected] == ["fact_thing"]


def test_a_descendant_that_never_reads_the_key_is_not_flagged():
    dim = _model("dim_thing", "table")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    registry = _registry(dim, fact)

    affected = affected_incremental_descendants(registry, dim, "thing_pk", lambda _child, _parent: {"thing_name"})
    assert affected == []


def test_the_key_propagates_through_a_full_refresh_intermediate():
    """A table in the middle carries the stale value on to the fact below it."""
    dim = _model("dim_thing", "table")
    mid = _model("int_thing", "table", ["dim_thing"])
    fact = _model("fact_thing", "incremental", ["int_thing"])
    registry = _registry(dim, mid, fact)

    affected = affected_incremental_descendants(registry, dim, "thing_pk", lambda _child, _parent: {"thing_pk"})
    assert [(m.model_name, m.depth) for m in affected] == [("fact_thing", 2)]


def test_tracing_continues_past_the_first_incremental_model():
    """Full-refreshing one fact does not fix the stale copy in the next."""
    dim = _model("dim_thing", "table")
    first = _model("fact_a", "incremental", ["dim_thing"])
    second = _model("fact_b", "incremental", ["fact_a"])
    registry = _registry(dim, first, second)

    affected = affected_incremental_descendants(registry, dim, "thing_pk", lambda _child, _parent: {"thing_pk"})
    assert [m.model_name for m in affected] == ["fact_a", "fact_b"]


def test_a_declared_fk_is_not_double_counted_with_the_read_behind_it():
    """Reading dim.thing_pk is how fact.thing_fk gets its value, not a second FK."""
    dim = _model("dim_thing", "table")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    registry = _registry(dim, fact)
    registry.foreign_keys[fact.unique_id] = [
        ForeignKeyRef(
            child_unique_id=fact.unique_id,
            child_column="thing_fk",
            parent_name="dim_thing",
            parent_column="thing_pk",
        )
    ]

    affected = affected_incremental_descendants(registry, dim, "thing_pk", lambda _child, _parent: {"thing_pk"})
    assert [(m.model_name, m.fk_column, m.evidence) for m in affected] == [
        ("fact_thing", "thing_fk", "relationships_test")
    ]


def test_unparseable_sql_falls_back_to_documented_columns():
    dim = _model("dim_thing", "table")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    fact.columns = {"thing_pk": ManifestColumn(name="thing_pk")}
    registry = _registry(dim, fact)

    affected = affected_incremental_descendants(registry, dim, "thing_pk")
    assert [(m.model_name, m.evidence) for m in affected] == [("fact_thing", "column_metadata")]


def test_an_incremental_ancestor_is_not_this_failure_mode():
    """It keeps the keys it already minted; only a full-refresh model re-derives them."""
    dim = _model("dim_thing", "incremental")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    registry = _registry(dim, fact)
    changed = changed_surrogate_keys(
        "{{ generate_surrogate_key(['a', 'b']) }} as thing_pk",
        "{{ generate_surrogate_key(['a']) }} as thing_pk",
    )

    assert detect_key_regen({"dim_thing": changed}, registry, lambda *_: {"thing_pk"}) == []


def test_no_finding_when_nothing_incremental_stores_the_key():
    dim = _model("dim_thing", "table")
    other = _model("dim_other", "table", ["dim_thing"])
    registry = _registry(dim, other)
    changed = changed_surrogate_keys(
        "{{ generate_surrogate_key(['a', 'b']) }} as thing_pk",
        "{{ generate_surrogate_key(['a']) }} as thing_pk",
    )

    assert detect_key_regen({"dim_thing": changed}, registry, lambda *_: {"thing_pk"}) == []


def test_a_diamond_does_not_lose_the_path_that_carries_the_key():
    """Marking a node seen on first sight makes the result traversal-ordered.

    Here `dim_thing` reaches `fact_thing` two ways: through `int_bare`, which
    carries nothing, and through `int_carrier`, which carries the key. Whichever
    the walk reaches first, the fact must still be flagged.
    """
    dim = _model("dim_thing", "table")
    bare = _model("int_bare", "table", ["dim_thing"])
    carrier = _model("int_carrier", "table", ["dim_thing"])
    fact = _model("fact_thing", "incremental", ["int_bare", "int_carrier"])
    reads = {
        ("int_bare", "dim_thing"): set(),
        ("int_carrier", "dim_thing"): {"thing_pk"},
        ("fact_thing", "int_bare"): set(),
        ("fact_thing", "int_carrier"): {"thing_pk"},
    }

    for order in ([bare, carrier], [carrier, bare]):
        registry = _registry(dim, *order, fact)
        affected = affected_incremental_descendants(
            registry,
            dim,
            "thing_pk",
            lambda child, parent: reads.get((child, parent), set()),
        )
        assert [m.model_name for m in affected] == ["fact_thing"], order


def test_a_second_path_carrying_a_new_column_reopens_the_node():
    """A node already reached under one column must be re-walked for another."""
    dim = _model("dim_thing", "table")
    fact = _model("fact_thing", "incremental", ["dim_thing"])
    registry = _registry(dim, fact)
    registry.foreign_keys[fact.unique_id] = [
        ForeignKeyRef(
            child_unique_id=fact.unique_id,
            child_column="thing_fk",
            parent_name="dim_thing",
            parent_column="thing_pk",
        )
    ]

    affected = affected_incremental_descendants(registry, dim, "thing_pk")
    assert [(m.model_name, m.fk_column) for m in affected] == [("fact_thing", "thing_fk")]


# ---------------------------------------------------------------------------
# Regression: the two incidents this check exists for
# ---------------------------------------------------------------------------


def test_flags_the_dim_discount_re_key_of_2411(dimensional_registry):
    """#2411 narrowed discount_pk's hash from three columns to two.

    tfact_order is incremental and stores it as discount_fk, which is why that
    PR needed a hand-run `dbt run --select tfact_order --full-refresh`.
    """
    changed = changed_surrogate_keys(fixture_sql("dim_discount_before.sql"), fixture_sql("dim_discount_after.sql"))
    findings = detect_key_regen({"dim_discount": changed}, dimensional_registry)

    assert len(findings) == 1
    assert findings[0].changed_key_column == "discount_pk"
    assert findings[0].affected_model_names == ["tfact_order"]
    assert [(m.fk_column, m.evidence) for m in findings[0].affected_models] == [("discount_fk", "relationships_test")]


def test_flags_the_dim_user_re_key_of_2497(dimensional_registry):
    """#2497 re-keyed user_pk off durable ids instead of email.

    The orphaned FKs surfaced later as #2618, which repaired six fact tables by
    hand. The detector finds those six plus tfact_problem_events and
    tfact_studentmodule_problems, which are incremental, declare the same
    user_fk -> dim_user.user_pk relationship, and were not repaired.
    """
    changed = changed_surrogate_keys(fixture_sql("dim_user_before.sql"), fixture_sql("dim_user_after.sql"))
    findings = detect_key_regen({"dim_user": changed}, dimensional_registry)

    assert len(findings) == 1
    assert findings[0].changed_key_column == "user_pk"
    repaired_by_2618 = {
        "tfact_certificate",
        "tfact_enrollment",
        "tfact_feedback",
        "tfact_grade",
        "tfact_order",
        "tfact_payment",
    }
    assert repaired_by_2618 <= set(findings[0].affected_model_names)
    assert set(findings[0].affected_model_names) - repaired_by_2618 == {
        "tfact_problem_events",
        "tfact_studentmodule_problems",
    }


def test_an_unchanged_dimension_produces_no_finding(dimensional_registry):
    unchanged = fixture_sql("dim_user_after.sql")
    assert changed_surrogate_keys(unchanged, unchanged) == []
    assert detect_key_regen({}, dimensional_registry) == []


# ---------------------------------------------------------------------------
# Manifest-level state
# ---------------------------------------------------------------------------


def test_state_covers_only_full_refresh_models():
    manifest = {
        "nodes": {
            "model.pkg.dim_thing": {
                "name": "dim_thing",
                "resource_type": "model",
                "config": {"materialized": "table"},
                "raw_code": "{{ generate_surrogate_key(['a', 'b']) }} as thing_pk",
            },
            "model.pkg.fact_thing": {
                "name": "fact_thing",
                "resource_type": "model",
                "config": {"materialized": "incremental"},
                "raw_code": "{{ generate_surrogate_key(['c']) }} as fact_key",
            },
            "model.pkg.dim_plain": {
                "name": "dim_plain",
                "resource_type": "model",
                "config": {"materialized": "table"},
                "raw_code": "select 1",
            },
        }
    }
    assert surrogate_key_state(manifest) == {"dim_thing": {"thing_pk": [["a", "b"]]}}


def test_state_diff_reports_changed_inputs_only():
    previous = {"dim_thing": {"thing_pk": [["a", "b"]]}, "dim_other": {"other_pk": [["x"]]}}
    current = {"dim_thing": {"thing_pk": [["a"]]}, "dim_other": {"other_pk": [["x"]]}}

    changed = changed_keys_from_state(previous, current)
    assert list(changed) == ["dim_thing"]
    assert changed["dim_thing"][0].current_inputs == (("a",),)


def test_a_model_missing_from_the_baseline_is_not_treated_as_changed():
    """A snapshot predating this check must not full-refresh everything below."""
    assert changed_keys_from_state({}, {"dim_thing": {"thing_pk": [["a"]]}}) == {}
