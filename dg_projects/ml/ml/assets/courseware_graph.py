"""Topology of the concept graph, recomputed whenever its nodes or edges change."""

from typing import Any

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    MetadataValue,
    Output,
    multi_asset,
)
from ml.lib.courseware.graph import (
    GRAPH_VERSION,
    PrerequisiteCycleError,
    compute_graph,
    previous_membership,
)
from ml.lib.courseware.warehouse import latest_run_rows
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import PermanentFailure
from pydantic import Field

CONCEPTS_KEY = AssetKey(["dimensional", "dim_concept"])
RELATIONS_KEY = AssetKey(["dimensional", "bridge_concept_relation"])
RUN_KEY = AssetKey(["intermediate", "courseware_concept_graph_run"])
# Written in this order; the run row goes last, because dbt and the next run read
# the newest run in courseware_concept_graph_run.
GRAPH_TABLES = (
    "courseware_concept_graph_node",
    "courseware_concept_graph_edge",
    "courseware_concept_graph_community",
    "courseware_concept_graph_prerequisite_closure",
    RUN_KEY.path[-1],
)
# How many cycle members the check lists, for review.
CYCLE_MEMBERS_SHOWN = 25
_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "schema_update_mode": "update",
}


class CoursewareGraphConfig(Config):
    max_sequence_cycle_nodes: int = Field(
        default=0,
        ge=0,
        description=(
            "The cycle check warns when more concepts than this sit on a builds_on "
            "cycle; learning order should be acyclic."
        ),
    )
    max_hierarchy_cycle_nodes: int = Field(
        default=0,
        ge=0,
        description="The same, for cycles in the broader hierarchy.",
    )


class ApprovedPrerequisiteCycle(PermanentFailure):
    """Approved builds_on edges form a cycle, so the closure has no learning order.

    Permanent because recomputing the same input fails the same way; a reviewer has
    to withdraw one of the approvals.
    """


def cycle_check(
    run: dict[str, Any], nodes: pl.DataFrame, config: CoursewareGraphConfig
) -> AssetCheckResult:
    """Pass when the sequence and hierarchy cycles are within the configured limits.

    :param run: the graph run row
    :param nodes: that run's node rows, to list the concepts on cycles
    :param config: the limits
    """

    def members(column: str) -> list[str]:
        return (
            nodes.filter(pl.col(column))["concept_id"].sort().head(CYCLE_MEMBERS_SHOWN)
        ).to_list()

    sequence = run["sequence_cycle_node_count"]
    hierarchy = run["hierarchy_cycle_node_count"]
    return AssetCheckResult(
        asset_key=RUN_KEY,
        check_name="graph_cycles",
        passed=sequence <= config.max_sequence_cycle_nodes
        and hierarchy <= config.max_hierarchy_cycle_nodes,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "graph_run_id": MetadataValue.text(run["graph_run_id"]),
            "sequence_cycle_node_count": MetadataValue.int(sequence),
            "hierarchy_cycle_node_count": MetadataValue.int(hierarchy),
            "sequence_cycle_concepts": MetadataValue.json(members("in_sequence_cycle")),
            "hierarchy_cycle_concepts": MetadataValue.json(
                members("in_hierarchy_cycle")
            ),
        },
    )


@multi_asset(
    group_name="courseware",
    deps=[CONCEPTS_KEY, RELATIONS_KEY],
    outs={
        table: AssetOut(
            key=AssetKey(["intermediate", table]),
            io_manager_key="io_manager",
            metadata=_OUTPUT_METADATA,
            code_version=GRAPH_VERSION,
            automation_condition=upstream_or_code_changes(),
            # No new run is written when the graph's input is unchanged.
            is_required=False,
        )
        for table in GRAPH_TABLES
    },
    check_specs=[
        AssetCheckSpec(
            "graph_cycles",
            asset=RUN_KEY,
            description=(
                "Concepts on builds_on and broader cycles in the newest graph run."
            ),
        )
    ],
)
def courseware_concept_graph(
    context: AssetExecutionContext, config: CoursewareGraphConfig
):
    """Compute the concept graph's communities, sequencing, hierarchy, sparsity and
    experimental prerequisite closure.

    Calls ml.lib.courseware.graph.compute_graph on dim_concept and every
    bridge_concept_relation column, so review_status reaches select_edges and only
    approved builds_on edges enter the closure. Community ids carry from the newest
    run. A run whose input checksum and graph version match the newest run's is not
    written again. A cycle among approved edges fails the run without writing
    anything, so the last run's closure stays the newest.
    """
    runs = read_table("intermediate", RUN_KEY.path[-1])
    last_run = (
        runs.sort("computed_at").collect().row(-1, named=True)
        if runs is not None
        else None
    )
    last_run_id = last_run["graph_run_id"] if last_run else None
    try:
        result = compute_graph(
            require_table("dimensional", CONCEPTS_KEY.path[-1]).collect(),
            require_table("dimensional", RELATIONS_KEY.path[-1]).collect(),
            previous_nodes=previous_membership(
                latest_run_rows(
                    "intermediate", GRAPH_TABLES[0], "graph_run_id", last_run_id
                ),
                latest_run_rows(
                    "intermediate",
                    "courseware_concept_graph_community",
                    "graph_run_id",
                    last_run_id,
                ),
                last_run_id,
            ),
        )
    except PrerequisiteCycleError as error:
        raise ApprovedPrerequisiteCycle(
            description=str(error),
            metadata={"retryable": False},
            allow_retries=False,
        ) from error
    run = result.run.row(0, named=True)
    # The checksum covers the input and parameters, not the algorithm, so a
    # GRAPH_VERSION bump (the asset's code version) must still write a run.
    if (
        last_run
        and last_run["input_checksum"] == run["input_checksum"]
        and last_run["graph_version"] == run["graph_version"]
    ):
        context.log.info(
            "Graph input and version are unchanged since run %s; nothing written",
            last_run_id,
        )
        last_nodes = (
            require_table("intermediate", GRAPH_TABLES[0])
            .filter(pl.col("graph_run_id") == last_run_id)
            .collect()
        )
        yield cycle_check(last_run, last_nodes, config)
        return

    context.log.info("Graph run %s", run)
    for table, frame in zip(
        GRAPH_TABLES,
        (
            result.nodes,
            result.edges,
            result.communities,
            result.prerequisite_closure,
            result.run,
        ),
        strict=True,
    ):
        yield Output(
            frame,
            output_name=table,
            metadata={"row_count": MetadataValue.int(frame.height)},
        )
    yield cycle_check(run, result.nodes, config)
