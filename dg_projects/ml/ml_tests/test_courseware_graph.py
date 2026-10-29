import json

import networkit as nk
import polars as pl
import pytest
from ml.lib.courseware.graph import (
    PrerequisiteCycleError,
    compute_graph,
    prerequisite_closure,
    previous_membership,
)


def concepts(*names):
    return pl.DataFrame(
        {
            "concept_pk": list(names),
            "preferred_label": list(names),
            "lifecycle_status": ["active"] * len(names),
            "label_count": [1] * len(names),
        }
    )


def relation(source, target, kind, **evidence):
    return {
        "from_concept_fk": source,
        "to_concept_fk": target,
        "relation": kind,
        "courserun_support": evidence.get("support", 2),
        "passes_support_rule": evidence.get("passes", True),
        "co_taught_lift": evidence.get("lift"),
        "is_adjudicated": evidence.get("adjudicated", False),
        "has_co_assessed_evidence": False,
        "review_status": evidence.get("review", "proposed"),
    }


def relations(*rows):
    return pl.DataFrame(
        list(rows),
        schema={
            "from_concept_fk": pl.String,
            "to_concept_fk": pl.String,
            "relation": pl.String,
            "courserun_support": pl.Int64,
            "passes_support_rule": pl.Boolean,
            "co_taught_lift": pl.Float64,
            "is_adjudicated": pl.Boolean,
            "has_co_assessed_evidence": pl.Boolean,
            "review_status": pl.String,
        },
    )


def by_concept(result):
    return {row["concept_id"]: row for row in result.nodes.iter_rows(named=True)}


def test_sequence_levels_foundations_and_cycles():
    result = compute_graph(
        concepts("mean", "variance", "regression", "a", "b", "c"),
        relations(
            relation("variance", "mean", "builds_on"),
            relation("regression", "variance", "builds_on"),
            relation("a", "b", "broader", adjudicated=True),
            relation("b", "a", "broader", adjudicated=True),
            # the weakest link of a broader cycle, which fails dbt's support rule
            relation("a", "c", "broader", adjudicated=True),
            relation("c", "a", "broader", adjudicated=True, passes=False),
            # fails bridge_concept_relation's support rule, so it cannot close a cycle
            relation("mean", "regression", "builds_on", passes=False),
        ),
    )
    nodes = by_concept(result)
    assert nodes["regression"]["sequence_level"] == 2
    assert nodes["mean"]["sequence_level"] == 0
    assert nodes["mean"]["foundation_rank"] > nodes["regression"]["foundation_rank"]
    assert nodes["a"]["in_hierarchy_cycle"]
    assert not nodes["c"]["in_hierarchy_cycle"]
    assert not nodes["mean"]["in_sequence_cycle"]
    assert result.run["hierarchy_cycle_node_count"][0] == 2


def test_communities_keep_their_ids_across_runs():
    names = ("pandas", "dataframe", "csv", "kmeans", "centroid", "cluster")
    edges = relations(
        relation("pandas", "dataframe", "complements", lift=5.0),
        relation("dataframe", "csv", "complements", lift=5.0),
        relation("pandas", "csv", "complements", lift=5.0),
        relation("kmeans", "centroid", "complements", lift=5.0),
        relation("centroid", "cluster", "complements", lift=5.0),
        relation("kmeans", "cluster", "complements", lift=5.0),
        relation("csv", "kmeans", "related", adjudicated=True),
    )
    first = compute_graph(concepts(*names), edges)
    second = compute_graph(
        concepts(*names, "silhouette"),
        pl.concat(
            [
                edges,
                relations(relation("silhouette", "cluster", "complements", lift=5.0)),
            ]
        ),
        previous_nodes=previous_membership(
            first.nodes, first.communities, first.run["graph_run_id"][0]
        ),
    )
    before, after = by_concept(first), by_concept(second)
    assert before["pandas"]["community_id"] != before["kmeans"]["community_id"]
    for name in names:
        assert after[name]["community_id"] == before[name]["community_id"]
    assert after["silhouette"]["community_id"] == after["kmeans"]["community_id"]
    assert 0 < second.run["density"][0] < 1


def test_a_relabelled_concept_changes_the_input_checksum():
    edges = relations(relation("pandas", "numpy", "complements", lift=5.0))
    before = compute_graph(concepts("pandas", "numpy"), edges)
    same = compute_graph(concepts("pandas", "numpy"), edges)
    relabelled = compute_graph(
        concepts("pandas", "numpy").with_columns(
            preferred_label=pl.when(pl.col("concept_pk") == "numpy")
            .then(pl.lit("NumPy"))
            .otherwise(pl.col("preferred_label"))
        ),
        edges,
    )
    assert same.run["input_checksum"][0] == before.run["input_checksum"][0]
    assert relabelled.run["input_checksum"][0] != before.run["input_checksum"][0]
    assert "NumPy" in relabelled.communities["top_concepts_json"].str.join("")[0]


def test_a_graph_with_no_active_concepts_is_empty():
    no_concepts = pl.DataFrame(
        schema={
            "concept_pk": pl.String,
            "preferred_label": pl.String,
            "lifecycle_status": pl.String,
            "label_count": pl.Int64,
        }
    )
    result = compute_graph(no_concepts, relations())
    assert result.nodes.height == 0
    assert result.edges.height == 0
    assert result.run["node_count"][0] == 0


def test_a_concept_with_no_current_labels_is_not_a_node():
    kept = concepts("pandas", "numpy")
    dropped = concepts("bluebikes").with_columns(label_count=pl.lit(0, dtype=pl.Int64))
    result = compute_graph(pl.concat([kept, dropped]), relations())
    assert sorted(result.nodes["concept_id"]) == ["numpy", "pandas"]


def directed(node_count, *edges):
    graph = nk.Graph(node_count, directed=True)
    for source, target in edges:
        graph.addEdge(source, target)
    return graph


def test_prerequisite_closure_follows_chains_at_their_shortest_depth():
    # 0 -> 1 -> 2 -> 3, and a diamond 0 -> 4 -> 3 that reaches 3 in two hops
    closure = prerequisite_closure(directed(5, (0, 1), (1, 2), (2, 3), (0, 4), (4, 3)))
    assert closure == {
        (0, 1): 1,
        (0, 2): 2,
        (0, 3): 2,
        (0, 4): 1,
        (1, 2): 1,
        (1, 3): 2,
        (2, 3): 1,
        (4, 3): 1,
    }


def test_prerequisite_closure_stops_at_the_depth_cap():
    closure = prerequisite_closure(directed(4, (0, 1), (1, 2), (2, 3)), max_depth=2)
    assert (0, 3) not in closure
    assert closure[(0, 2)] == 2
    assert closure[(1, 3)] == 2


def test_prerequisite_closure_rejects_a_cycle():
    with pytest.raises(PrerequisiteCycleError):
        prerequisite_closure(directed(3, (0, 1), (1, 2), (2, 0)))


def test_the_closure_reads_only_approved_builds_on():
    result = compute_graph(
        concepts("mean", "variance", "regression", "plot", "median"),
        relations(
            relation("variance", "mean", "builds_on", review="accepted"),
            relation("regression", "variance", "builds_on", review="accepted"),
            # observations that pass the support rule but nobody has approved
            relation("median", "mean", "builds_on"),
            relation("regression", "median", "builds_on"),
            relation("regression", "plot", "practiced_with"),
        ),
    )
    closure = {
        (row["from_concept_id"], row["to_concept_id"]): row["min_depth"]
        for row in result.prerequisite_closure.iter_rows(named=True)
    }
    assert closure == {
        ("variance", "mean"): 1,
        ("regression", "variance"): 1,
        ("regression", "mean"): 2,
    }
    assert json.loads(result.run["prerequisite_closure_excluded_json"][0]) == []
    assert set(result.prerequisite_closure["graph_run_id"]) == {
        result.run["graph_run_id"][0]
    }


def test_the_closure_is_empty_until_an_edge_is_approved():
    result = compute_graph(
        concepts("mean", "variance", "regression"),
        relations(
            relation("variance", "mean", "builds_on"),
            relation("regression", "variance", "builds_on"),
        ),
    )
    assert result.prerequisite_closure.is_empty()
    assert by_concept(result)["regression"]["sequence_level"] == 2


def test_a_cycle_of_observations_is_a_finding_not_a_failure():
    result = compute_graph(
        concepts("mean", "variance"),
        relations(
            relation("variance", "mean", "builds_on"),
            relation("mean", "variance", "builds_on"),
        ),
    )
    assert by_concept(result)["mean"]["in_sequence_cycle"]
    assert result.prerequisite_closure.is_empty()


def test_an_approved_builds_on_cycle_fails_the_graph_run():
    with pytest.raises(PrerequisiteCycleError, match="mean"):
        compute_graph(
            concepts("mean", "variance", "regression"),
            relations(
                relation("variance", "mean", "builds_on", review="accepted"),
                relation("regression", "variance", "builds_on", review="accepted"),
                relation("mean", "regression", "builds_on", review="accepted"),
            ),
        )


def test_a_pair_approved_in_both_directions_fails_the_graph_run():
    # direction_consistency lets at most one direction of a pair pass the support
    # rule, so the other approval must not drop out before the cycle check
    with pytest.raises(PrerequisiteCycleError, match="mean"):
        compute_graph(
            concepts("mean", "variance"),
            relations(
                relation("variance", "mean", "builds_on", review="accepted"),
                relation(
                    "mean", "variance", "builds_on", passes=False, review="accepted"
                ),
            ),
        )


def test_an_approved_builds_on_self_loop_fails_the_graph_run():
    with pytest.raises(PrerequisiteCycleError, match="mean"):
        compute_graph(
            concepts("mean"),
            relations(relation("mean", "mean", "builds_on", review="accepted")),
        )


def test_an_approved_edge_failing_the_support_rule_is_in_the_closure():
    result = compute_graph(
        concepts("mean", "variance", "regression"),
        relations(
            relation("variance", "mean", "builds_on", review="accepted"),
            relation(
                "regression", "variance", "builds_on", passes=False, review="accepted"
            ),
        ),
    )
    closure = {
        (row["from_concept_id"], row["to_concept_id"]): row["min_depth"]
        for row in result.prerequisite_closure.iter_rows(named=True)
    }
    assert closure == {
        ("variance", "mean"): 1,
        ("regression", "variance"): 1,
        ("regression", "mean"): 2,
    }
    # the observation view still applies the rule
    assert by_concept(result)["regression"]["sequence_level"] == 0


def test_an_approved_edge_to_an_inactive_concept_is_recorded_on_the_run():
    concept_rows = concepts("mean", "variance", "regression").with_columns(
        pl.when(pl.col("concept_pk") == "regression")
        .then(pl.lit("merged"))
        .otherwise(pl.col("lifecycle_status"))
        .alias("lifecycle_status")
    )
    result = compute_graph(
        concept_rows,
        relations(
            relation("variance", "mean", "builds_on", review="accepted"),
            relation("regression", "variance", "builds_on", review="accepted"),
        ),
    )
    assert result.prerequisite_closure["from_concept_id"].to_list() == ["variance"]
    assert json.loads(result.run["prerequisite_closure_excluded_json"][0]) == [
        {
            "from_concept_id": "regression",
            "to_concept_id": "variance",
            "reason": "inactive_concept",
        }
    ]
