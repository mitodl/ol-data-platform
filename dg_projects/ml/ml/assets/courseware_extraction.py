"""Courseware claim extraction, one partition per (platform, course run).

courseware_extraction_text stands for the text a run's scopes are built from. It
is observed, not materialized: courseware_extraction_text_sensor reports each run's
text checksum (int__courseware__extraction_source_checksums) under the extractor
version as its data version, and courseware_extraction runs for a partition when
that version moves or the partition is new.

Only runs courseware_extraction_allowlist approves for the deployed extractor
version are observed or extracted. A prompt or schema change therefore extracts
nothing until the allowlist is promoted to the new version, run by run or
platform by platform, and the asset carries no code version for the same reason.
"""

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetSpec,
    Config,
    DataVersion,
    MetadataValue,
    ObserveResult,
    Output,
    multi_asset,
    multi_observable_source_asset,
)
from ml.lib.courseware.extract import (
    BEDROCK_DEFAULT_MODEL,
    DEFAULT_EFFORT,
    DEFAULT_MODEL,
    EXTRACTOR_ID,
    PROMPT_VERSION,
    SCHEMA_VERSION,
    TEXT_STAGE,
    Effort,
    extract_scopes,
    plan_scopes,
    scope_inputs,
    stored_readings,
    text_extraction_key,
)
from ml.lib.courseware.prompts import CONTEXT_SYSTEM_PROMPT, TEXT_SYSTEM_PROMPT
from ml.lib.courseware.scopes import build_scopes
from ml.lib.courseware.warehouse import (
    EXTRACTION_PARTITION_EXPR,
    EXTRACTION_PARTITIONS,
    anthropic_client,
    partition_courserun,
)
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import publish_prompt_version
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import PermanentFailure, permanent_failure
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, EqualTo, In
from pyiceberg.table import Table

EXTRACTION_TEXT_KEY = AssetKey(["courseware", "extraction_text"])
SOURCES_KEY = AssetKey(["intermediate", "int__courseware__extraction_sources"])
CHECKSUMS_KEY = AssetKey(
    ["intermediate", "int__courseware__extraction_source_checksums"]
)
TEXT_EXTRACTION_KEY = AssetKey(["intermediate", "courseware_text_extraction"])
EXTRACTION_KEY = AssetKey(["intermediate", "courseware_claim_extraction"])
CLAIM_KEY = AssetKey(["intermediate", "courseware_claim"])
EVIDENCE_KEY = AssetKey(["intermediate", "courseware_claim_evidence"])
SCOPE_INPUT_KEY = AssetKey(["intermediate", "courseware_scope_input"])
EXTRACTION_PROMPT_NAME = "courseware-claim-extraction"
CONTEXT_PROMPT_NAME = "courseware-claim-context"
# The extractor version this code runs, as courseware_extraction_allowlist names it.
EXTRACTOR_VERSION = {
    "extractor_id": EXTRACTOR_ID,
    "prompt_version": PROMPT_VERSION,
    "schema_version": SCHEMA_VERSION,
}
# Tries per delete commit before the run fails on a concurrent writer.
DELETE_COMMIT_ATTEMPTS = 3
# The POC measured 98.9% of 1,949 passages as exact matches, none unverified.
DEFAULT_MIN_EXACT_EVIDENCE_SHARE = 0.95

_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "partition_expr": EXTRACTION_PARTITION_EXPR,
    "schema_update_mode": "update",
}


def text_checksums() -> pl.DataFrame:
    """Every allowlisted run's text checksum and the extractor version it may use.

    From int__courseware__extraction_source_checksums, which leaves out the runs
    courseware_extraction_allowlist does not name.
    """
    return (
        require_table("intermediate", CHECKSUMS_KEY.path[-1])
        .select(
            "platform", "courserun_readable_id", "text_checksum", *EXTRACTOR_VERSION
        )
        .collect()
    )


def approved(rows: pl.DataFrame) -> pl.DataFrame:
    """Keep the runs approved for the extractor version this code runs."""
    return rows.filter(
        *(pl.col(column) == value for column, value in EXTRACTOR_VERSION.items())
    )


def extraction_text_version(text_checksum: str) -> str:
    """Return extraction_text's data version for a run's text checksum.

    The extractor version is part of it so that promoting an unchanged run to a new
    version moves the data version and extracts the run once more.
    """
    return "-".join([text_checksum, *EXTRACTOR_VERSION.values()])


def approved_run(platform: str, courserun: str) -> dict[str, str]:
    """Return a run's checksum row, or fail when it is not approved for this version.

    :raises PermanentFailure: when courseware_extraction_allowlist does not approve
        the run for the extractor version this code runs
    """
    rows = text_checksums().filter(
        (pl.col("platform") == platform)
        & (pl.col("courserun_readable_id") == courserun)
    )
    if not approved(rows).height:
        approved_for = (
            "-".join(rows.row(0, named=True)[column] for column in EXTRACTOR_VERSION)
            if rows.height
            else None
        )
        msg = (
            f"{platform} {courserun} is not approved for extractor version "
            f"{'-'.join(EXTRACTOR_VERSION.values())} (courseware_extraction_allowlist "
            f"approves {approved_for or 'no version'}). Add or promote its row in "
            "src/ol_dbt/seeds/courseware_extraction_allowlist.csv."
        )
        raise permanent_failure(msg)
    return rows.row(0, named=True)


@multi_observable_source_asset(
    specs=[
        AssetSpec(
            EXTRACTION_TEXT_KEY,
            partitions_def=EXTRACTION_PARTITIONS,
            deps=[SOURCES_KEY, CHECKSUMS_KEY],
            group_name="courseware",
            description=(
                "The text each course run's extraction scopes are built from, "
                "versioned by its checksum."
            ),
        )
    ],
)
def courseware_extraction_text(context: AssetExecutionContext) -> ObserveResult:
    """Report one course run's text checksum.

    Routine observation comes from courseware_extraction_text_sensor, which
    reports every run in one tick. This is kept so the asset is observable (only
    observable upstreams take part in the data version comparison) and so a single
    run can be observed on demand.
    """
    platform, courserun = partition_courserun(context.partition_key)
    row = approved_run(platform, courserun)
    return ObserveResult(
        asset_key=EXTRACTION_TEXT_KEY,
        data_version=DataVersion(extraction_text_version(row["text_checksum"])),
    )


class CoursewareExtractionConfig(Config):
    model_id: str | None = Field(
        default=None,
        description=(
            "Model or inference profile id, in the llm resource's namespace. Unset "
            f"uses {BEDROCK_DEFAULT_MODEL} on Bedrock and {DEFAULT_MODEL} on the "
            "Anthropic API. Part of every extraction key, so a new model adds rows "
            "beside the old ones."
        ),
    )
    effort: Effort = Field(
        default=DEFAULT_EFFORT, description="output_config.effort for every call"
    )
    max_workers: int = Field(default=6, ge=1, description="Concurrent model calls")
    reuse_text_across_runs: bool = Field(
        default=False,
        description=(
            "Let this run use another run's stored text reading of identical scope "
            "text (e.g. a B2B copy of a course) instead of calling the text stage. "
            "Off until identical passages in different courses have been compared "
            "(docs/design/courseware_knowledge_extraction.md)."
        ),
    )
    min_exact_evidence_share: float = Field(
        default=DEFAULT_MIN_EXACT_EVIDENCE_SHARE,
        ge=0,
        le=1,
        description=(
            "The evidence check fails when fewer of this run's new passages than "
            "this share match their source exactly."
        ),
    )


class ScopeExtractionFailure(PermanentFailure):
    """Some of a run's scopes got no valid extraction after the library's retry.

    Permanent so stop_run_retries keeps run_retries from re-running the whole
    partition (each attempt pays for every failed scope again).
    upstream_or_code_changes still re-requests a newly failed partition once, and
    that run asks only for the scopes still missing.
    """


def _clear_partial_write(
    table: Table, platform: str, courserun: str, keys: list[str]
) -> None:
    """Delete claim or evidence rows an earlier, failed attempt wrote for these keys.

    The extraction row is written last and is what marks a key as done, so rows
    under a key with no extraction row are leftovers that would be duplicated. A
    commit that loses to a concurrent writer is retried on the refreshed table.
    """
    row_filter = And(
        EqualTo("platform", platform),
        EqualTo("courserun_readable_id", courserun),
        In("extraction_key", keys),
    )
    for attempt in range(1, DELETE_COMMIT_ATTEMPTS + 1):
        try:
            table.delete(row_filter)
        except CommitFailedException:
            if attempt == DELETE_COMMIT_ATTEMPTS:
                raise
            table.refresh()
        else:
            return


def evidence_check(evidence: pl.DataFrame, min_exact_share: float) -> AssetCheckResult:
    """Pass when enough of the new evidence passages match their source exactly."""
    counts = dict(evidence.group_by("verification_status").len().iter_rows())
    total = evidence.height
    exact_share = counts.get("exact", 0) / total if total else 1.0
    verified_share = (
        (counts.get("exact", 0) + counts.get("fuzzy", 0)) / total if total else 1.0
    )
    return AssetCheckResult(
        asset_key=EVIDENCE_KEY,
        check_name="evidence_verification_rate",
        passed=exact_share >= min_exact_share,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "passages": MetadataValue.int(total),
            "exact_share": MetadataValue.float(exact_share),
            "verified_share": MetadataValue.float(verified_share),
            "unverified": MetadataValue.int(counts.get("unverified", 0)),
            "unknown_source": MetadataValue.int(counts.get("unknown_source", 0)),
            "min_exact_share": MetadataValue.float(min_exact_share),
        },
    )


@multi_asset(
    partitions_def=EXTRACTION_PARTITIONS,
    deps=[EXTRACTION_TEXT_KEY],
    group_name="courseware",
    outs={
        key.path[-1]: AssetOut(
            key=key,
            io_manager_key="courseware_io_manager",
            metadata=_OUTPUT_METADATA,
            # No code_version: a version change reaches a run through its
            # allowlist row and extraction_text's data version, never all runs at
            # once (see the module docstring).
            automation_condition=upstream_or_code_changes(),
        )
        for key in (
            TEXT_EXTRACTION_KEY,
            CLAIM_KEY,
            EVIDENCE_KEY,
            EXTRACTION_KEY,
            SCOPE_INPUT_KEY,
        )
    },
    check_specs=[
        AssetCheckSpec(
            "evidence_verification_rate",
            asset=EVIDENCE_KEY,
            description=(
                "Share of this run's new evidence passages found verbatim in the "
                "source text."
            ),
        )
    ],
    pool="courseware_extraction",
)
def courseware_extraction(
    context: AssetExecutionContext,
    config: CoursewareExtractionConfig,
    llm: LLMClientFactory,
):
    """Extract claims for every scope of one course run that is not stored yet.

    Calls ml.lib.courseware.extract.extract_scopes over the run's scopes, skipping
    every extraction_key already in courseware_claim_extraction and reusing the
    run's stored text readings (courseware_text_extraction), so a header edit
    re-runs only the context stage. Every table is append-only. Text readings are
    written first, then claims and evidence, then the extraction rows, which mark a
    key as done. A run courseware_extraction_allowlist does not approve for this
    extractor version fails before anything is read or called, however it was
    launched.
    """
    platform, courserun = partition_courserun(context.partition_key)
    approved_run(platform, courserun)
    rows = (
        require_table("intermediate", SOURCES_KEY.path[-1])
        .filter(
            (pl.col("platform") == platform)
            & (pl.col("courserun_readable_id") == courserun)
        )
        .collect()
    )
    scopes = build_scopes(rows) if rows.height else []

    in_run = (pl.col("platform") == platform) & (
        pl.col("courserun_readable_id") == courserun
    )
    stored = read_table("intermediate", EXTRACTION_KEY.path[-1])
    existing_keys = (
        set(stored.filter(in_run).select("extraction_key").collect()["extraction_key"])
        if stored is not None
        else set()
    )
    client, model_id = anthropic_client(
        llm,
        config.model_id,
        default_model=DEFAULT_MODEL,
        default_bedrock_model=BEDROCK_DEFAULT_MODEL,
    )
    across_runs = config.reuse_text_across_runs
    stored_text = read_table("intermediate", TEXT_EXTRACTION_KEY.path[-1])
    readings = (
        stored_readings(
            stored_text.filter(
                # any run's reading of these scopes' text, or this run's own
                pl.col("text_extraction_key").is_in(
                    [
                        text_extraction_key(scope, model_id, config.effort)
                        for scope in scopes
                    ]
                )
                if across_runs
                else in_run
            ).collect(),
            across_runs=across_runs,
        )
        if stored_text is not None
        else {}
    )
    # Before any model call, so nothing Opik does can fail the run after outputs
    # start being written.
    opik_prompt_version = publish_prompt_version(
        EXTRACTION_PROMPT_NAME, TEXT_SYSTEM_PROMPT
    )
    opik_context_prompt_version = publish_prompt_version(
        CONTEXT_PROMPT_NAME, CONTEXT_SYSTEM_PROMPT
    )
    pending = [
        planned
        for planned in plan_scopes(
            scopes,
            model_id=model_id,
            effort=config.effort,
            readings=readings,
            across_runs=across_runs,
        )
        if planned.extraction_key not in existing_keys
    ]
    # A scope part with no stored text reading gets a new key, which no earlier
    # attempt can have written claims under: its text reading is written first.
    pending_keys = [
        planned.extraction_key
        for planned in pending
        if planned.extraction_key is not None
    ]
    # Before any model call, so a catalog error fails the run before it spends.
    if pending:
        catalog = get_glue_catalog()
        for key in (CLAIM_KEY, EVIDENCE_KEY):
            identifier = f"{_OUTPUT_METADATA['schema']}.{key.path[-1]}"
            if catalog.table_exists(identifier):
                table = catalog.load_table(identifier)
                if pending_keys:
                    _clear_partial_write(table, platform, courserun, pending_keys)

    tables = extract_scopes(
        scopes,
        client,
        model_id=model_id,
        effort=config.effort,
        existing_keys=existing_keys,
        readings=readings,
        max_workers=config.max_workers,
        reuse_text_across_runs=across_runs,
    )
    failed = len(pending) - tables.extractions.height
    context.log.info(
        "%s %s: %d scopes, %d already stored, %d new text readings, %d extracted, "
        "%d failed",
        platform,
        courserun,
        len(scopes),
        len(scopes) - len(pending),
        tables.text_extractions.height,
        tables.extractions.height,
        failed,
    )

    partition_columns = [
        pl.lit(platform).alias("platform"),
        pl.lit(courserun).alias("courserun_readable_id"),
    ]
    yield Output(
        tables.text_extractions,
        output_name=TEXT_EXTRACTION_KEY.path[-1],
        metadata={
            "row_count": MetadataValue.int(tables.text_extractions.height),
            "prompt_version": MetadataValue.text(TEXT_STAGE.prompt_version),
            "opik_prompt_version": MetadataValue.text(opik_prompt_version),
            "input_tokens": MetadataValue.int(
                tables.text_extractions["input_tokens"].sum() or 0
            ),
            "output_tokens": MetadataValue.int(
                tables.text_extractions["output_tokens"].sum() or 0
            ),
        },
    )
    yield Output(
        tables.claims.with_columns(partition_columns),
        output_name=CLAIM_KEY.path[-1],
        metadata={"row_count": MetadataValue.int(tables.claims.height)},
    )
    yield Output(
        tables.evidence.with_columns(partition_columns),
        output_name=EVIDENCE_KEY.path[-1],
        metadata={"row_count": MetadataValue.int(tables.evidence.height)},
    )
    yield Output(
        tables.extractions,
        output_name=EXTRACTION_KEY.path[-1],
        metadata={
            "row_count": MetadataValue.int(tables.extractions.height),
            "scope_count": MetadataValue.int(len(scopes)),
            "failed_scope_count": MetadataValue.int(failed),
            "model_id": MetadataValue.text(model_id),
            "effort": MetadataValue.text(config.effort),
            "prompt_version": MetadataValue.text(PROMPT_VERSION),
            "opik_prompt_version": MetadataValue.text(opik_context_prompt_version),
            "input_tokens": MetadataValue.int(
                tables.extractions["input_tokens"].sum() or 0
            ),
            "output_tokens": MetadataValue.int(
                tables.extractions["output_tokens"].sum() or 0
            ),
        },
    )
    # Every scope part as read now, extracted or skipped, so dbt can tell which
    # stored reading is the current text's.
    observed = scope_inputs(scopes)
    yield Output(
        observed,
        output_name=SCOPE_INPUT_KEY.path[-1],
        metadata={"row_count": MetadataValue.int(observed.height)},
    )
    yield evidence_check(tables.evidence, config.min_exact_evidence_share)

    # The successful scopes are stored. The failure is permanent for run_retries;
    # automation re-requests the partition once (see ScopeExtractionFailure).
    if failed:
        raise ScopeExtractionFailure(
            description=(
                f"{failed} of {len(pending)} scopes failed to extract for "
                f"{platform} {courserun}"
            ),
            metadata={"retryable": False, "failed_scope_count": failed},
            allow_retries=False,
        )
