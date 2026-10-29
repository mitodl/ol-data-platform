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
    DagsterInstance,
    DagsterRunStatus,
    Failure,
    MetadataValue,
    Output,
    RunsFilter,
    multi_asset,
)
from ml.lib.courseware.resolve import (
    ADJUDICATION_MODEL,
    ADJUDICATION_PROMPT,
    BEDROCK_ADJUDICATION_MODEL,
    PROMPT_VERSION,
    RESOLVER_ID,
    ResolutionState,
    label_embedder,
    resolve_concepts,
)
from ml.lib.courseware.senses import SENSE_PROMPT, SENSE_PROMPT_VERSION
from ml.lib.courseware.warehouse import anthropic_client, latest_run_rows
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import publish_prompt_version
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import PermanentFailure
from pydantic import Field

LABELS_KEY = AssetKey(["intermediate", "int__courseware__concept_labels"])
SENSE_INPUTS_KEY = AssetKey(["intermediate", "int__courseware__concept_sense_inputs"])
RUN_KEY = AssetKey(["intermediate", "courseware_resolution_run"])
ADJUDICATION_PROMPT_NAME = "courseware-concept-adjudication"
SENSE_PROMPT_NAME = "courseware-concept-sense"
UNIT_COLUMNS = ("concept_label_key", "platform", "courserun_readable_id")
# The pilot's full rebuild kept 636 of 650 assignments (2.2% changed).
DEFAULT_MAX_ASSIGNMENT_CHURN = 0.05
# Written in this order; the run row goes last, because the next run reads the
# newest row in courseware_resolution_run and then that run's snapshots.
SNAPSHOT_TABLES = {
    "courseware_concept_registry": "registry",
    "courseware_concept_assignment": "assignments",
    "courseware_concept_relation": "relations",
    "courseware_concept_alias": "aliases",
    "courseware_concept_sense_assignment": "sense_assignments",
}
APPEND_TABLES = (
    *SNAPSHOT_TABLES,
    "courseware_resolution_decision",
    "courseware_sense_decision",
    "courseware_concept_label_embedding",
    RUN_KEY.path[-1],
)
# Statuses of a run that may still write resolution snapshots.
_WRITING_STATUSES = [
    DagsterRunStatus.STARTING,
    DagsterRunStatus.STARTED,
    DagsterRunStatus.CANCELING,
]
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


def overlapping_resolution_runs(instance: DagsterInstance, run_id: str) -> list[str]:
    """Ids of other runs that are writing resolution snapshots now.

    Each run reads the newest run's snapshots and writes a full new set, so two at
    once would mint different ids for the same new labels and the later snapshot
    would drop the other's concepts. Automation never overlaps them
    (upstream_or_code_changes is gated on ~in_progress), but a launch from the UI
    can, and the courseware_resolution pool has no limit: Dagster sets pool limits
    only in its database, not in code. Both runs are already starting or started
    when either checks, so at worst both refuse.

    :param instance: the Dagster instance
    :param run_id: this run, excluded
    :returns: the overlapping run ids
    :rtype: list[str]
    """
    return [
        record.dagster_run.run_id
        for record in instance.get_run_records(
            filters=RunsFilter(statuses=_WRITING_STATUSES)
        )
        if record.dagster_run.run_id != run_id
        and RUN_KEY in (record.dagster_run.asset_selection or ())
    ]


class AdjudicationFailure(PermanentFailure):
    """Some adjudication batches failed; the run's other results are stored.

    Permanent so stop_run_retries keeps run_retries from re-running the whole
    resolution; see ScopeExtractionFailure for the same reasoning.
    """


def assignment_churn(
    previous: pl.DataFrame, current: pl.DataFrame, label_keys: set[str]
) -> tuple[int, int]:
    """Count current labels in both runs, and those whose concept id changed.

    Incremental runs carry the assignments of labels that are no longer current,
    so both snapshots are restricted to label_keys first; otherwise the carried
    rows pad the count.

    :param label_keys: the run's current concept_label_keys
    :returns: (current labels in both runs, those whose concept_id differs)
    :rtype: tuple[int, int]
    """
    joined = (
        previous.select("concept_label_key", "concept_id")
        .join(
            current.select("concept_label_key", "concept_id"),
            on="concept_label_key",
            suffix="_current",
        )
        .filter(pl.col("concept_label_key").is_in(label_keys))
    )
    changed = joined.filter(pl.col("concept_id") != pl.col("concept_id_current"))
    return joined.height, changed.height


def undecided_sense_units(
    sense_inputs: pl.DataFrame, sense_assignments: pl.DataFrame, label_keys: set[str]
) -> int:
    """Count current (label, course run) units with no sense assignment row.

    A unit in the review queue has a row (needs_review), so it does not make a run
    on its own; any run asks about it again. A unit whose sense batch failed has no
    row, so it does.

    :param sense_inputs: int__courseware__concept_sense_inputs
    :param sense_assignments: the newest run's sense assignment snapshot
    :param label_keys: the run's current concept_label_keys
    """
    return (
        sense_inputs.filter(pl.col("concept_label_key").is_in(label_keys))
        .select(UNIT_COLUMNS)
        .unique()
        .join(sense_assignments.select(UNIT_COLUMNS), on=UNIT_COLUMNS, how="anti")
        .height
    )


def churn_check(
    previous: pl.DataFrame,
    current: pl.DataFrame,
    label_keys: set[str],
    max_churn: float,
    **metadata: int,
) -> AssetCheckResult:
    """Pass when few enough carried labels moved to a different concept id."""
    common, changed = assignment_churn(previous, current, label_keys)
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
    deps=[LABELS_KEY, SENSE_INPUTS_KEY],
    outs={
        table: AssetOut(
            key=AssetKey(["intermediate", table]),
            io_manager_key="io_manager",
            metadata=_OUTPUT_METADATA,
            code_version=f"{RESOLVER_ID}-{PROMPT_VERSION}-{SENSE_PROMPT_VERSION}",
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
):
    """Assign every concept label to a stable concept id, resolving only new labels,
    then each course run's use of a label to one of its senses.

    Calls ml.lib.courseware.resolve.resolve_concepts with the newest run's
    registry, assignment, relation, alias and sense assignment snapshots and the
    label embedding cache, then appends this run's snapshots, decisions, new
    embeddings and run row.
    """
    if overlapping := overlapping_resolution_runs(context.instance, context.run.run_id):
        # retryable: the retry reads the snapshot the other run writes
        msg = f"Resolution runs {overlapping} are in progress; one writer at a time"
        raise Failure(msg)
    labels = require_table("intermediate", LABELS_KEY.path[-1]).collect()
    sense_inputs = require_table("intermediate", SENSE_INPUTS_KEY.path[-1]).collect()

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
            # a column added since the snapshot was written (the registry's
            # split_from_concept_id) is null in its rows
            columns = getattr(state, attribute).schema
            setattr(
                state,
                attribute,
                snapshot.select(
                    pl.col(name)
                    if name in snapshot.columns
                    else pl.lit(None, dtype=dtype).alias(name)
                    for name, dtype in columns.items()
                ),
            )
    cached = read_table("intermediate", "courseware_concept_label_embedding")
    if cached is not None:
        state.embeddings = cached.select(state.embeddings.columns).collect()

    # Membership, not equality: the snapshot also carries the assignments of
    # labels that are no longer current, and those never need resolving.
    label_keys = set(labels["concept_label_key"])
    if (
        not config.rebuild
        and label_keys <= set(state.assignments["concept_label_key"])
        and not undecided_sense_units(sense_inputs, state.sense_assignments, label_keys)
    ):
        context.log.info(
            "All %d labels and their course runs already have assignments in run %s;"
            " nothing to resolve",
            labels.height,
            last_run,
        )
        yield churn_check(
            state.assignments,
            state.assignments,
            label_keys,
            config.max_assignment_churn,
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
    opik_sense_prompt_version = publish_prompt_version(SENSE_PROMPT_NAME, SENSE_PROMPT)
    result = resolve_concepts(
        labels,
        state,
        client,
        label_embedder(run_id=context.run.run_id),
        mentions=sense_inputs,
        model_id=model_id,
        rebuild=config.rebuild,
    )
    run = result.run.row(0, named=True)
    # Current labels in a failed batch that still have no concept. In a rebuild a
    # failed label keeps its previous concept, so only labels new to this run
    # count; the assignments also carry labels that are no longer current, so
    # this is set membership, not a difference of counts.
    unassigned = len(label_keys - set(result.assignments["concept_label_key"]))
    # units of a failed sense batch; the next run asks about them again
    undecided_units = undecided_sense_units(
        sense_inputs, result.sense_assignments, label_keys
    )
    context.log.info("Resolution run %s", run)
    for table, frame in (
        ("courseware_concept_registry", result.registry),
        ("courseware_concept_assignment", result.assignments),
        ("courseware_concept_relation", result.relations),
        ("courseware_concept_alias", result.aliases),
        ("courseware_concept_sense_assignment", result.sense_assignments),
        ("courseware_resolution_decision", result.decisions),
        ("courseware_sense_decision", result.sense_decisions),
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
            "sense_calls": MetadataValue.int(run["sense_calls"]),
            "senses_minted": MetadataValue.int(run["senses_minted"]),
            "sense_review_count": MetadataValue.int(run["sense_review_count"]),
            "undecided_sense_unit_count": MetadataValue.int(undecided_units),
            "sense_prompt_version": MetadataValue.text(SENSE_PROMPT_VERSION),
            "opik_sense_prompt_version": MetadataValue.text(opik_sense_prompt_version),
        },
    )
    yield churn_check(
        state.assignments,
        result.assignments,
        label_keys,
        config.max_assignment_churn,
        concepts_merged=run["concepts_merged"],
        concepts_deprecated=run["concepts_deprecated"],
        concepts_minted=run["concepts_minted"],
    )

    # What succeeded is stored. The failure is permanent for run_retries, which
    # would pay for every batch again; automation re-requests once, and that run
    # adjudicates only the labels and course runs still undecided.
    if run["failed_batch_count"] or undecided_units:
        raise AdjudicationFailure(
            description=(
                f"{run['failed_batch_count']} of {run['adjudication_calls']} label "
                f"adjudication batches failed ({run['failed_label_count']} labels, "
                f"{unassigned} left without a concept); {undecided_units} course "
                "runs' uses of a label are in a failed sense batch"
            ),
            metadata={
                "retryable": False,
                "failed_batch_count": run["failed_batch_count"],
                "failed_label_count": run["failed_label_count"],
                "unassigned_label_count": unassigned,
                "undecided_sense_unit_count": undecided_units,
            },
            allow_retries=False,
        )
