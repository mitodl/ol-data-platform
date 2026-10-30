"""Topology of the concept graph: communities, sequencing, hierarchy and sparsity.

Reads bridge_concept_relation (edges with their evidence) and dim_concept (active
concepts), keeps the edges that pass dbt's support rules (for builds_on and broader)
and clear the support thresholds here, and computes on three views of the graph with
NetworKit:

- association: undirected and weighted, every relation family. Leiden communities,
  connected components, degree, density and sparsity, and a local-degree backbone
  score per edge for drawing a sparsified graph.
- sequence: builds_on, directed from a concept to what it builds on. These edges are
  observed instructional order (units teaching one concept relied on the other), not
  reviewed prerequisites, so this view is exploratory. PageRank on it is a foundation
  rank (rank flows to the concepts many others build on); strongly connected
  components find cycles, which are findings to review; sequence_level is the longest
  chain of builds_on edges beneath a concept.
- prerequisite closure: every concept each concept builds on, directly or through
  others, at its shortest depth, over approved builds_on edges only (review_status
  accepted). It is an experimental projection that keeps the meaning of its input
  edges and validates none of them. No review workflow exists yet, so nothing is
  approved and the closure is empty. The support rule does not gate an approved
  edge: it filters noise from observations, and approval is the decision it stands
  in for. A cycle among approved edges (an approved pair in both directions
  included) fails the run rather than writing a closure that puts a concept before
  itself. An approved edge whose concept is no longer active is left out and listed
  in the run's prerequisite_closure_excluded_json.
- hierarchy: broader, directed from narrower to broader. hierarchy_depth is the
  longest chain of broader concepts above a concept; cycles are findings too.

Community ids are minted once and carried forward between runs by member overlap,
the same way concept ids are, so a community can be followed over time.
"""

import hashlib
import json
import math
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime

import networkit as nk
import polars as pl

GRAPH_VERSION = "courseware_concept_graph_v1"
# A co-taught complements edge needs at least this lift over chance.
MIN_CO_TAUGHT_LIFT = 2.0
MIN_COURSERUN_SUPPORT = 1
# Edge weight by relation, before the support multiplier. Equivalence and
# hierarchy say the most about what a concept is; association the least.
RELATION_WEIGHTS = {
    "close": 1.0,
    "broader": 0.8,
    "builds_on": 0.6,
    "practiced_with": 0.3,
    "complements": 0.5,
    "related": 0.2,
}
# The closure stops at this many builds_on hops. A concept further back than this
# is not useful as "take this first", and the cap keeps the closure from growing
# with the square of the corpus along long chains. Only a concept whose
# sequence_level exceeds it can have prerequisites the closure leaves out, since
# sequence_level is the longest chain and the closure keeps the shortest.
MAX_PREREQUISITE_DEPTH = 8
COMMUNITY_CARRY_FORWARD_MIN_JACCARD = 0.3
LEIDEN_GAMMA = 1.0
SEED = 20261016

NODE_SCHEMA = {
    "graph_run_id": pl.String,
    "concept_id": pl.String,
    "community_id": pl.String,
    "component_index": pl.Int64,
    "degree": pl.Int64,
    "weighted_degree": pl.Float64,
    "foundation_rank": pl.Float64,
    "sequence_level": pl.Int64,
    "hierarchy_depth": pl.Int64,
    "in_sequence_cycle": pl.Boolean,
    "in_hierarchy_cycle": pl.Boolean,
}
EDGE_SCHEMA = {
    "graph_run_id": pl.String,
    "from_concept_id": pl.String,
    "to_concept_id": pl.String,
    "relation": pl.String,
    "weight": pl.Float64,
    "backbone_score": pl.Float64,
}
PREREQUISITE_CLOSURE_SCHEMA = {
    "graph_run_id": pl.String,
    "from_concept_id": pl.String,
    "to_concept_id": pl.String,
    "min_depth": pl.Int64,
}
COMMUNITY_SCHEMA = {
    "graph_run_id": pl.String,
    "community_id": pl.String,
    "concept_count": pl.Int64,
    "top_concepts_json": pl.String,
    "first_seen_run_id": pl.String,
}
RUN_SCHEMA = {
    "graph_run_id": pl.String,
    "graph_version": pl.String,
    "computed_at": pl.Datetime(time_zone="UTC"),
    "input_checksum": pl.String,
    "parameters_json": pl.String,
    "node_count": pl.Int64,
    "edge_count": pl.Int64,
    "edges_by_relation_json": pl.String,
    "density": pl.Float64,
    "sparsity": pl.Float64,
    "average_degree": pl.Float64,
    "component_count": pl.Int64,
    "largest_component_share": pl.Float64,
    "isolate_count": pl.Int64,
    "community_count": pl.Int64,
    "modularity": pl.Float64,
    "sequence_cycle_node_count": pl.Int64,
    "hierarchy_cycle_node_count": pl.Int64,
    "prerequisite_closure_excluded_json": pl.String,
}


@dataclass(frozen=True)
class GraphResult:
    nodes: pl.DataFrame
    edges: pl.DataFrame
    communities: pl.DataFrame
    prerequisite_closure: pl.DataFrame
    run: pl.DataFrame


class PrerequisiteCycleError(ValueError):
    """Approved builds_on edges form a cycle, so learning order is undefined."""


def select_edges(relations: pl.DataFrame) -> pl.DataFrame:
    """Keep the edges whose evidence clears the thresholds, with a weight each.

    :param relations: bridge_concept_relation rows
    :returns: from_concept_fk, to_concept_fk, relation, weight
    :rtype: pl.DataFrame
    """
    support = pl.col("courserun_support")
    # bridge_concept_relation owns the builds_on support and direction rule and
    # broader acyclicity, through passes_support_rule
    keep = pl.col("passes_support_rule") & (
        pl.when(pl.col("relation") == "practiced_with")
        .then(support >= MIN_COURSERUN_SUPPORT)
        .when(pl.col("relation") == "complements")
        .then(
            (support >= MIN_COURSERUN_SUPPORT)
            & (
                (pl.col("co_taught_lift") >= MIN_CO_TAUGHT_LIFT)
                | pl.col("has_co_assessed_evidence")
            )
        )
        # builds_on needs nothing beyond passes_support_rule; the rest are adjudicated
        .otherwise(pl.col("is_adjudicated") | (pl.col("relation") == "builds_on"))
    )
    return (
        relations.filter(keep & (pl.col("from_concept_fk") != pl.col("to_concept_fk")))
        .with_columns(
            (
                pl.col("relation").replace_strict(RELATION_WEIGHTS, default=0.1)
                * (1 + support.cast(pl.Float64).log1p())
            ).alias("weight")
        )
        .select("from_concept_fk", "to_concept_fk", "relation", "weight")
    )


def approved_prerequisite_edges(relations: pl.DataFrame) -> pl.DataFrame:
    """Select the builds_on edges a reviewer accepted, the closure's only input.

    passes_support_rule is not applied. It filters noise from observations and lets
    at most one direction of an observed pair through, so applying it would drop an
    approval without a trace, and would hide a pair approved in both directions,
    which is a cycle.

    :param relations: bridge_concept_relation rows
    :returns: from_concept_fk, to_concept_fk
    :rtype: pl.DataFrame
    """
    return (
        relations.filter(
            (pl.col("relation") == "builds_on")
            & (pl.col("review_status") == "accepted")
        )
        .select("from_concept_fk", "to_concept_fk")
        .unique()
        .sort(pl.all())
    )


def _cycle_nodes(graph: nk.Graph) -> set[int]:
    components = nk.components.StronglyConnectedComponents(graph)
    components.run()
    return {
        node
        for component in components.getComponents()
        if len(component) > 1
        for node in component
    }


def _longest_chain(graph: nk.Graph, blocked: set[int]) -> dict[int, int]:
    """Longest path length from each node along out-edges, ignoring cyclic nodes.

    For builds_on (concept -> what it builds on) this is the depth of the
    observed instructional order beneath a concept; for broader it is the depth of
    the hierarchy above it.
    """
    if not graph.numberOfNodes():
        # topologicalSort rejects a graph with no nodes
        return {}
    acyclic = nk.Graph(graph.numberOfNodes(), directed=True)
    for source, target in graph.iterEdges():
        if source not in blocked and target not in blocked:
            acyclic.addEdge(source, target)
    order = nk.graphtools.topologicalSort(acyclic)
    depth = dict.fromkeys(range(graph.numberOfNodes()), 0)
    # reverse topological order visits every target before its sources
    for node in reversed(order):
        for target in acyclic.iterNeighbors(node):
            depth[node] = max(depth[node], depth[target] + 1)
    return depth


def prerequisite_closure(
    graph: nk.Graph,
    max_depth: int = MAX_PREREQUISITE_DEPTH,
    labels: list[str] | None = None,
) -> dict[tuple[int, int], int]:
    """Every node each node builds on, within max_depth hops, at its shortest depth.

    :param graph: builds_on, directed from a concept to what it builds on
    :param max_depth: the most hops a pair may be apart
    :param labels: each node's display label by node index, to name a cycle's
        concepts in the error
    :returns: (node, prerequisite node) -> fewest builds_on hops between them
    :rtype: dict[tuple[int, int], int]
    :raises PrerequisiteCycleError: when the graph has a directed cycle
    """
    self_loops = {source for source, target in graph.iterEdges() if source == target}
    if cycle := _cycle_nodes(graph) | self_loops:
        names = sorted(labels[node] for node in cycle) if labels else sorted(cycle)
        msg = f"builds_on has a cycle through {names}"
        raise PrerequisiteCycleError(msg)
    if not graph.numberOfNodes():
        return {}
    reachable: dict[int, dict[int, int]] = {}
    # reverse topological order closes every target before its sources, and a
    # shortest path's tail is a shortest path, so one pass gives min depths
    for node in reversed(nk.graphtools.topologicalSort(graph)):
        depths: dict[int, int] = {}
        for target in graph.iterNeighbors(node):
            depths[target] = 1
            for prerequisite, depth in reachable[target].items():
                if depth + 1 <= min(max_depth, depths.get(prerequisite, max_depth)):
                    depths[prerequisite] = depth + 1
        reachable[node] = depths
    return {
        (node, prerequisite): depth
        for node, depths in reachable.items()
        for prerequisite, depth in depths.items()
    }


def _carry_forward_communities(
    members: dict[int, set[str]], previous: pl.DataFrame | None
) -> tuple[dict[int, str], dict[str, str]]:
    """Give each new community the id of the previous one it overlaps most.

    :returns: community index -> community id, and community id -> first-seen run
    """
    old: dict[str, set[str]] = defaultdict(set)
    first_seen: dict[str, str] = {}
    if previous is not None:
        for row in previous.iter_rows(named=True):
            if row["community_id"]:
                old[row["community_id"]].add(row["concept_id"])
                first_seen.setdefault(row["community_id"], row["first_seen_run_id"])
    ids: dict[int, str] = {}
    taken: set[str] = set()
    for index, concepts in sorted(members.items(), key=lambda item: -len(item[1])):
        score, best = max(
            (
                (len(concepts & old_members) / len(concepts | old_members), old_id)
                for old_id, old_members in old.items()
                if old_id not in taken
            ),
            default=(0.0, None),
        )
        if best and score >= COMMUNITY_CARRY_FORWARD_MIN_JACCARD:
            ids[index] = best
            taken.add(best)
        else:
            ids[index] = str(uuid.uuid4())
    return ids, first_seen


def compute_graph(  # noqa: PLR0915
    concepts: pl.DataFrame,
    relations: pl.DataFrame,
    *,
    previous_nodes: pl.DataFrame | None = None,
) -> GraphResult:
    """Compute the concept graph's topology.

    :param concepts: dim_concept rows; active concepts that some current claim names
        (label_count above 0) are nodes
    :param relations: bridge_concept_relation rows
    :param previous_nodes: the previous run's node rows joined to its community
        rows (concept_id, community_id, first_seen_run_id), to carry community ids
    :returns: node, edge, community and run tables
    :rtype: GraphResult
    """
    nk.setSeed(SEED, useThreadId=False)
    # Leiden's result depends on scheduling when it runs in parallel; one thread
    # keeps a re-run on the same input identical
    nk.setNumberOfThreads(1)
    run_id = str(uuid.uuid4())
    computed_at = datetime.now(tz=UTC)

    # resolution keeps a concept whose labels have dropped out active until a rebuild
    active = concepts.filter(
        (pl.col("lifecycle_status") == "active") & (pl.col("label_count") > 0)
    )
    concept_ids = sorted(active["concept_pk"])
    index = {concept_id: position for position, concept_id in enumerate(concept_ids)}
    labels = dict(zip(active["concept_pk"], active["preferred_label"], strict=True))
    edges = select_edges(relations).filter(
        pl.col("from_concept_fk").is_in(concept_ids)
        & pl.col("to_concept_fk").is_in(concept_ids)
    )
    approved = approved_prerequisite_edges(relations)
    approved_is_active = pl.col("from_concept_fk").is_in(concept_ids) & pl.col(
        "to_concept_fk"
    ).is_in(concept_ids)
    excluded_approved = [
        {
            "from_concept_id": source,
            "to_concept_id": target,
            "reason": "inactive_concept",
        }
        for source, target in approved.filter(~approved_is_active).iter_rows()
    ]

    node_count = len(concept_ids)
    association = nk.Graph(node_count, weighted=True, directed=False)
    sequence = nk.Graph(node_count, directed=True)
    approved_sequence = nk.Graph(node_count, directed=True)
    hierarchy = nk.Graph(node_count, directed=True)
    for row in edges.iter_rows(named=True):
        source, target = index[row["from_concept_fk"]], index[row["to_concept_fk"]]
        if association.hasEdge(source, target):
            association.increaseWeight(source, target, row["weight"])
        else:
            association.addEdge(source, target, row["weight"])
        if row["relation"] == "builds_on" and not sequence.hasEdge(source, target):
            sequence.addEdge(source, target)
        if row["relation"] == "broader" and not hierarchy.hasEdge(source, target):
            hierarchy.addEdge(source, target)
    for source, target in approved.filter(approved_is_active).iter_rows():
        approved_sequence.addEdge(index[source], index[target])

    leiden = nk.community.ParallelLeiden(
        association, randomize=False, gamma=LEIDEN_GAMMA
    )
    leiden.run()
    partition = leiden.getPartition()
    modularity = (
        nk.community.Modularity().getQuality(partition, association)
        if association.numberOfEdges()
        else 0.0
    )
    members: dict[int, set[str]] = defaultdict(set)
    for node in range(node_count):
        members[partition.subsetOf(node)].add(concept_ids[node])
    community_ids, first_seen = _carry_forward_communities(members, previous_nodes)

    components = nk.components.ConnectedComponents(association)
    components.run()
    component_sizes = components.getComponentSizes()

    # NetworKit's PageRank crashes the process on a graph with no nodes
    foundation: list[float] = []
    if node_count:
        pagerank = nk.centrality.PageRank(sequence, damp=0.85, normalized=True)
        pagerank.run()
        foundation = pagerank.scores()

    sequence_cycles = _cycle_nodes(sequence)
    closure = prerequisite_closure(
        approved_sequence, labels=[labels[concept] for concept in concept_ids]
    )
    hierarchy_cycles = _cycle_nodes(hierarchy)
    sequence_level = _longest_chain(sequence, sequence_cycles)
    hierarchy_depth = _longest_chain(hierarchy, hierarchy_cycles)

    association.indexEdges()
    backbone = nk.sparsification.LocalDegreeScore(association)
    backbone.run()
    backbone_scores = backbone.scores()

    nodes = pl.DataFrame(
        [
            {
                "graph_run_id": run_id,
                "concept_id": concept_ids[node],
                "community_id": community_ids[partition.subsetOf(node)],
                "component_index": components.componentOfNode(node),
                "degree": association.degree(node),
                "weighted_degree": association.weightedDegree(node),
                "foundation_rank": foundation[node],
                "sequence_level": sequence_level[node],
                "hierarchy_depth": hierarchy_depth[node],
                "in_sequence_cycle": node in sequence_cycles,
                "in_hierarchy_cycle": node in hierarchy_cycles,
            }
            for node in range(node_count)
        ],
        schema=NODE_SCHEMA,
    )

    edge_rows = []
    for row in edges.iter_rows(named=True):
        source, target = index[row["from_concept_fk"]], index[row["to_concept_fk"]]
        edge_rows.append(
            {
                "graph_run_id": run_id,
                "from_concept_id": row["from_concept_fk"],
                "to_concept_id": row["to_concept_fk"],
                "relation": row["relation"],
                "weight": row["weight"],
                "backbone_score": backbone_scores[association.edgeId(source, target)],
            }
        )

    community_rows = []
    for community_index, concept_set in members.items():
        ranked = sorted(
            concept_set,
            key=lambda concept_id: -association.weightedDegree(index[concept_id]),
        )
        community_id = community_ids[community_index]
        community_rows.append(
            {
                "graph_run_id": run_id,
                "community_id": community_id,
                "concept_count": len(concept_set),
                "top_concepts_json": json.dumps([labels[c] for c in ranked[:5]]),
                "first_seen_run_id": first_seen.get(community_id, run_id),
            }
        )

    closure_rows = [
        {
            "graph_run_id": run_id,
            "from_concept_id": concept_ids[source],
            "to_concept_id": concept_ids[target],
            "min_depth": depth,
        }
        for (source, target), depth in sorted(closure.items())
    ]

    edge_count = association.numberOfEdges()
    possible = node_count * (node_count - 1) / 2
    density = edge_count / possible if possible else 0.0
    parameters = {
        "min_co_taught_lift": MIN_CO_TAUGHT_LIFT,
        "min_courserun_support": MIN_COURSERUN_SUPPORT,
        "relation_weights": RELATION_WEIGHTS,
        "leiden_gamma": LEIDEN_GAMMA,
        "max_prerequisite_depth": MAX_PREREQUISITE_DEPTH,
        "prerequisite_closure_input": (
            "builds_on with review_status accepted, support rule not applied"
        ),
        "seed": SEED,
    }
    # The labels are input too: top_concepts_json shows them, so a relabelled
    # concept with unchanged topology must still write a run.
    input_checksum = hashlib.sha256(
        json.dumps(
            [
                [(concept_id, labels[concept_id]) for concept_id in concept_ids],
                edges.sort(pl.all()).rows(),
                approved.rows(),
                parameters,
            ],
            default=str,
        ).encode()
    ).hexdigest()
    run = pl.DataFrame(
        [
            {
                "graph_run_id": run_id,
                "graph_version": GRAPH_VERSION,
                "computed_at": computed_at,
                "input_checksum": input_checksum,
                "parameters_json": json.dumps(parameters),
                "node_count": node_count,
                "edge_count": edge_count,
                "edges_by_relation_json": json.dumps(dict(Counter(edges["relation"]))),
                "density": density,
                "sparsity": 1 - density,
                "average_degree": 2 * edge_count / node_count if node_count else 0.0,
                "component_count": components.numberOfComponents(),
                "largest_component_share": (
                    max(component_sizes.values()) / node_count if node_count else 0.0
                ),
                "isolate_count": sum(
                    1 for node in range(node_count) if association.degree(node) == 0
                ),
                "community_count": len(members),
                "modularity": modularity if not math.isnan(modularity) else 0.0,
                "sequence_cycle_node_count": len(sequence_cycles),
                "hierarchy_cycle_node_count": len(hierarchy_cycles),
                "prerequisite_closure_excluded_json": json.dumps(excluded_approved),
            }
        ],
        schema=RUN_SCHEMA,
    )
    return GraphResult(
        nodes=nodes,
        edges=pl.DataFrame(edge_rows, schema=EDGE_SCHEMA),
        communities=pl.DataFrame(community_rows, schema=COMMUNITY_SCHEMA),
        prerequisite_closure=pl.DataFrame(
            closure_rows, schema=PREREQUISITE_CLOSURE_SCHEMA
        ),
        run=run,
    )


def previous_membership(
    nodes: pl.DataFrame | None, communities: pl.DataFrame | None, run_id: str | None
) -> pl.DataFrame | None:
    """Return the previous run's concept -> community membership and first-seen runs."""
    if nodes is None or communities is None or run_id is None:
        return None
    return nodes.filter(pl.col("graph_run_id") == run_id).join(
        communities.filter(pl.col("graph_run_id") == run_id).select(
            "community_id", "first_seen_run_id"
        ),
        on="community_id",
    )
