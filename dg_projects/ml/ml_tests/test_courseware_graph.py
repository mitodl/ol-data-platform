import polars as pl
from ml.lib.courseware.graph import compute_graph, previous_membership


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
        "direction_consistency": evidence.get("consistency"),
        "co_taught_lift": evidence.get("lift"),
        "is_adjudicated": evidence.get("adjudicated", False),
        "has_co_assessed_evidence": False,
    }


def relations(*rows):
    return pl.DataFrame(
        list(rows),
        schema={
            "from_concept_fk": pl.String,
            "to_concept_fk": pl.String,
            "relation": pl.String,
            "courserun_support": pl.Int64,
            "direction_consistency": pl.Float64,
            "co_taught_lift": pl.Float64,
            "is_adjudicated": pl.Boolean,
            "has_co_assessed_evidence": pl.Boolean,
        },
    )


def by_concept(result):
    return {row["concept_id"]: row for row in result.nodes.iter_rows(named=True)}


def test_sequence_levels_foundations_and_cycles():
    result = compute_graph(
        concepts("mean", "variance", "regression", "a", "b"),
        relations(
            relation("variance", "mean", "builds_on", consistency=1.0),
            relation("regression", "variance", "builds_on", consistency=1.0),
            relation("a", "b", "broader", adjudicated=True),
            relation("b", "a", "broader", adjudicated=True),
            # inconsistent direction across courses: dropped by the threshold
            relation("mean", "regression", "builds_on", consistency=0.3),
        ),
    )
    nodes = by_concept(result)
    assert nodes["regression"]["sequence_level"] == 2
    assert nodes["mean"]["sequence_level"] == 0
    assert nodes["mean"]["foundation_rank"] > nodes["regression"]["foundation_rank"]
    assert nodes["a"]["in_hierarchy_cycle"]
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
