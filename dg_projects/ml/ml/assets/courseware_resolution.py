"""Corpus-wide concept resolution over every extracted concept label."""

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
from ml.lib.courseware.resolve import (
    ADJUDICATION_MODEL,
    ADJUDICATION_PROMPT,
    BEDROCK_ADJUDICATION_MODEL,
    EMBEDDING_DIM,
    PROMPT_VERSION,
    RESOLVER_ID,
    ResolutionState,
    resolve_concepts,
)
from ml.lib.courseware.warehouse import (
    anthropic_client,
    latest_run_rows,
    read_table,
    require_table,
    warehouse_database,
)
from ml.lib.embed import build_embedding_client
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import publish_prompt_version
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import PermanentFailure
from pydantic import Field

LABELS_KEY = AssetKey(["intermediate", "int__courseware__concept_labels"])
RUN_KEY = AssetKey(["intermediate", "courseware_resolution_run"])
ADJUDICATION_PROMPT_NAME = "courseware-concept-adjudication"
# The pilot's full rebuild kept 636 of 650 assignments (2.2% changed).
DEFAULT_MAX_ASSIGNMENT_CHURN = 0.05
# Written in this order; the run row goes last, because the next run reads the
# newest row in courseware_resolution_run and then that run's snapshots.
SNAPSHOT_TABLES = {
    "courseware_concept_registry": "registry",
    "courseware_concept_assignment": "assignments",
    "courseware_concept_relation": "relations",
}
APPEND_TABLES = (
    *SNAPSHOT_TABLES,
    "courseware_resolution_decision",
    "courseware_concept_label_embedding",
    RUN_KEY.path[-1],
)
_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "schema_update_mode": "update",
}


class CoursewareResolutionConfig(Config):
    model_id: str | None = Field(
        default=None,
        description=(
            "Adjudication model, in the llm resource's namespace. Unset uses "
            f"{BEDROCK_ADJUDICATION_MODEL} on Bedrock and {ADJUDICATION_MODEL} on "
            "the Anthropic API."
        ),
    )
    rebuild: bool = Field(
        default=False,
        description=(
            "Re-resolve every label, carrying ids forward by member overlap. A "
            "rebuild changes some assignments, so run it as a reviewed event."
        ),
    )
    max_assignment_churn: float = Field(
        default=DEFAULT_MAX_ASSIGNMENT_CHURN,
        ge=0,
        le=1,
        description=(
            "The churn check fails when more than this share of the labels the "
            "previous run assigned now belong to a different concept."
        ),
    )


class AdjudicationFailure(PermanentFailure):
    """Some adjudication batches failed; the run's other results are stored.

    Permanent so stop_run_retries keeps run_retries from re-running the whole
    resolution; see ScopeExtractionFailure for the same reasoning.
    """


def assignment_churn(previous: pl.DataFrame, current: pl.DataFrame) -> tuple[int, int]:
    """Count labels in both runs, and those whose concept id changed between them.

    :returns: (labels in both runs, labels whose concept_id differs)
    :rtype: tuple[int, int]
    """
    joined = previous.select("concept_label_key", "concept_id").join(
        current.select("concept_label_key", "concept_id"),
        on="concept_label_key",
        suffix="_current",
    )
    changed = joined.filter(pl.col("concept_id") != pl.col("concept_id_current"))
    return joined.height, changed.height


def churn_check(
    previous: pl.DataFrame, current: pl.DataFrame, max_churn: float, **metadata: int
) -> AssetCheckResult:
    """Pass when few enough carried labels moved to a different concept id."""
    common, changed = assignment_churn(previous, current)
    share = changed / common if common else 0.0
    return AssetCheckResult(
        asset_key=RUN_KEY,
        check_name="resolution_churn",
        passed=share <= max_churn,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "labels_in_both_runs": MetadataValue.int(common),
            "labels_with_changed_id": MetadataValue.int(changed),
            "changed_share": MetadataValue.float(share),
            "max_assignment_churn": MetadataValue.float(max_churn),
            **{name: MetadataValue.int(value) for name, value in metadata.items()},
        },
    )


@multi_asset(
    group_name="courseware",
    deps=[LABELS_KEY],
    outs={
        table: AssetOut(
            key=AssetKey(["intermediate", table]),
            io_manager_key="io_manager",
            metadata=_OUTPUT_METADATA,
            code_version=f"{RESOLVER_ID}-{PROMPT_VERSION}",
            automation_condition=upstream_or_code_changes(),
            # No new run is written when every label already has an assignment.
            is_required=False,
        )
        for table in APPEND_TABLES
    },
    check_specs=[
        AssetCheckSpec(
            "resolution_churn",
            asset=RUN_KEY,
            description=(
                "Share of labels assigned in the previous run whose concept id "
                "changed in this one."
            ),
        )
    ],
    pool="courseware_resolution",
)
def courseware_resolution(
    context: AssetExecutionContext,
    config: CoursewareResolutionConfig,
    llm: LLMClientFactory,
    embedding_llm: LLMClientFactory,
):
    """Assign every concept label to a stable concept id, resolving only new labels.

    Calls ml.lib.courseware.resolve.resolve_concepts with the newest run's
    registry, assignment and relation snapshots and the label embedding cache,
    then appends this run's snapshots, decisions, new embeddings and run row.
    """
    labels = require_table("intermediate", LABELS_KEY.path[-1]).collect()

    runs = read_table("intermediate", RUN_KEY.path[-1])
    last_run = (
        runs.sort("started_at")
        .select("resolution_run_id")
        .collect()["resolution_run_id"][-1]
        if runs is not None
        else None
    )
    state = ResolutionState()
    for table, attribute in SNAPSHOT_TABLES.items():
        snapshot = latest_run_rows("intermediate", table, "resolution_run_id", last_run)
        if snapshot is not None:
            setattr(
                state, attribute, snapshot.select(getattr(state, attribute).columns)
            )
    cached = read_table("intermediate", "courseware_concept_label_embedding")
    if cached is not None:
        state.embeddings = cached.select(state.embeddings.columns).collect()

    assigned = set(state.assignments["concept_label_key"])
    if not config.rebuild and set(labels["concept_label_key"]) == assigned:
        context.log.info(
            "All %d labels already have an assignment in run %s; nothing to resolve",
            labels.height,
            last_run,
        )
        yield churn_check(
            state.assignments, state.assignments, config.max_assignment_churn
        )
        return

    client, model_id = anthropic_client(
        llm,
        config.model_id,
        default_model=ADJUDICATION_MODEL,
        default_bedrock_model=BEDROCK_ADJUDICATION_MODEL,
    )
    # Before any model call, so nothing Opik does can fail the run after outputs
    # start being written.
    opik_prompt_version = publish_prompt_version(
        ADJUDICATION_PROMPT_NAME, ADJUDICATION_PROMPT
    )
    result = resolve_concepts(
        labels,
        state,
        client,
        build_embedding_client(embedding_llm, dim=EMBEDDING_DIM),
        model_id=model_id,
        rebuild=config.rebuild,
    )
    run = result.run.row(0, named=True)
    # Labels in a failed batch that still have no concept. In a rebuild a failed
    # label keeps its previous concept, so only labels new to this run count.
    unassigned = labels.height - result.assignments.height
    context.log.info("Resolution run %s", run)
    for table, frame in (
        ("courseware_concept_registry", result.registry),
        ("courseware_concept_assignment", result.assignments),
        ("courseware_concept_relation", result.relations),
        ("courseware_resolution_decision", result.decisions),
        ("courseware_concept_label_embedding", result.embeddings),
    ):
        yield Output(
            frame,
            output_name=table,
            metadata={"row_count": MetadataValue.int(frame.height)},
        )
    yield Output(
        result.run,
        output_name=RUN_KEY.path[-1],
        metadata={
            "resolution_run_id": MetadataValue.text(run["resolution_run_id"]),
            "mode": MetadataValue.text(run["mode"]),
            "model_id": MetadataValue.text(model_id),
            "resolved_label_count": MetadataValue.int(run["resolved_label_count"]),
            "concepts_minted": MetadataValue.int(run["concepts_minted"]),
            "concepts_merged": MetadataValue.int(run["concepts_merged"]),
            "failed_batch_count": MetadataValue.int(run["failed_batch_count"]),
            "failed_label_count": MetadataValue.int(run["failed_label_count"]),
            "unassigned_label_count": MetadataValue.int(unassigned),
            "prompt_version": MetadataValue.text(PROMPT_VERSION),
            "opik_prompt_version": MetadataValue.text(opik_prompt_version),
        },
    )
    yield churn_check(
        state.assignments,
        result.assignments,
        config.max_assignment_churn,
        concepts_merged=run["concepts_merged"],
        concepts_deprecated=run["concepts_deprecated"],
        concepts_minted=run["concepts_minted"],
    )

    # What succeeded is stored. The failure is permanent for run_retries, which
    # would pay for every batch again; automation re-requests once, and that run
    # adjudicates only the labels still unassigned.
    if run["failed_batch_count"]:
        raise AdjudicationFailure(
            description=(
                f"{run['failed_batch_count']} of {run['adjudication_calls']} "
                f"adjudication batches failed ({run['failed_label_count']} labels, "
                f"{unassigned} left without a concept)"
            ),
            metadata={
                "retryable": False,
                "failed_batch_count": run["failed_batch_count"],
                "failed_label_count": run["failed_label_count"],
                "unassigned_label_count": unassigned,
            },
            allow_retries=False,
        )
