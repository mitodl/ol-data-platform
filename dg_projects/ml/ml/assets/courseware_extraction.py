"""Courseware claim extraction, one partition per (platform, course run).

courseware_extraction_text stands for the text a run's scopes are built from. It
is observed, not materialized: courseware_extraction_text_sensor reports each run's
text checksum (int__courseware__extraction_source_checksums) as its data version,
and courseware_extraction runs for a partition when that version moves, when the
partition is new, or when the extractor's code version changes.
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
    Failure,
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
    Effort,
    extract_scopes,
    extraction_key,
)
from ml.lib.courseware.prompts import SYSTEM_PROMPT
from ml.lib.courseware.scopes import build_scopes
from ml.lib.courseware.warehouse import (
    EXTRACTION_PARTITION_EXPR,
    EXTRACTION_PARTITIONS,
    anthropic_client,
    partition_courserun,
    read_table,
    require_table,
    warehouse_database,
)
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import publish_prompt_version
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from pydantic import Field
from pyiceberg.expressions import And, EqualTo, In
from pyiceberg.table import Table

EXTRACTION_TEXT_KEY = AssetKey(["courseware", "extraction_text"])
SOURCES_KEY = AssetKey(["intermediate", "int__courseware__extraction_sources"])
CHECKSUMS_KEY = AssetKey(
    ["intermediate", "int__courseware__extraction_source_checksums"]
)
EXTRACTION_KEY = AssetKey(["intermediate", "courseware_claim_extraction"])
CLAIM_KEY = AssetKey(["intermediate", "courseware_claim"])
EVIDENCE_KEY = AssetKey(["intermediate", "courseware_claim_evidence"])
EXTRACTION_PROMPT_NAME = "courseware-claim-extraction"
# The POC measured 98.9% of 1,949 passages as exact matches, none unverified.
DEFAULT_MIN_EXACT_EVIDENCE_SHARE = 0.95

_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "partition_expr": EXTRACTION_PARTITION_EXPR,
    "schema_update_mode": "update",
}


def text_checksums() -> pl.DataFrame:
    """Every run's text checksum, from int__courseware__extraction_source_checksums."""
    return (
        require_table("intermediate", CHECKSUMS_KEY.path[-1])
        .select("platform", "courserun_readable_id", "text_checksum")
        .collect()
    )


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
    row = text_checksums().filter(
        (pl.col("platform") == platform)
        & (pl.col("courserun_readable_id") == courserun)
    )
    return ObserveResult(
        asset_key=EXTRACTION_TEXT_KEY, data_version=DataVersion(row["text_checksum"][0])
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
    min_exact_evidence_share: float = Field(
        default=DEFAULT_MIN_EXACT_EVIDENCE_SHARE,
        ge=0,
        le=1,
        description=(
            "The evidence check fails when fewer of this run's new passages than "
            "this share match their source exactly."
        ),
    )


def _clear_partial_write(
    table: Table, platform: str, courserun: str, keys: list[str]
) -> None:
    """Delete claim or evidence rows an earlier, failed attempt wrote for these keys.

    The extraction row is written last and is what marks a key as done, so rows
    under a key with no extraction row are leftovers that would be duplicated.
    """
    table.delete(
        And(
            EqualTo("platform", platform),
            EqualTo("courserun_readable_id", courserun),
            In("extraction_key", keys),
        )
    )


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
            # The prompt and schema versions are part of every extraction key, so
            # a change to either re-extracts every run (rows are added, not
            # replaced).
            code_version=f"{EXTRACTOR_ID}-{PROMPT_VERSION}-{SCHEMA_VERSION}",
            automation_condition=upstream_or_code_changes(),
        )
        for key in (CLAIM_KEY, EVIDENCE_KEY, EXTRACTION_KEY)
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
    every extraction_key already in courseware_claim_extraction. All three tables
    are append-only. Claims and evidence are written before the extraction rows,
    which mark a key as done.
    """
    platform, courserun = partition_courserun(context.partition_key)
    rows = (
        require_table("intermediate", SOURCES_KEY.path[-1])
        .filter(
            (pl.col("platform") == platform)
            & (pl.col("courserun_readable_id") == courserun)
        )
        .collect()
    )
    scopes = build_scopes(rows) if rows.height else []

    stored = read_table("intermediate", EXTRACTION_KEY.path[-1])
    existing_keys = (
        set(
            stored.filter(
                (pl.col("platform") == platform)
                & (pl.col("courserun_readable_id") == courserun)
            )
            .select("extraction_key")
            .collect()["extraction_key"]
        )
        if stored is not None
        else set()
    )

    client, model_id = anthropic_client(
        llm,
        config.model_id,
        default_model=DEFAULT_MODEL,
        default_bedrock_model=BEDROCK_DEFAULT_MODEL,
    )
    pending = [
        scope
        for scope in scopes
        if extraction_key(scope, model_id, config.effort) not in existing_keys
    ]
    tables = extract_scopes(
        scopes,
        client,
        model_id=model_id,
        effort=config.effort,
        existing_keys=existing_keys,
        max_workers=config.max_workers,
    )
    failed = len(pending) - tables.extractions.height
    context.log.info(
        "%s %s: %d scopes, %d already stored, %d extracted, %d failed",
        platform,
        courserun,
        len(scopes),
        len(scopes) - len(pending),
        tables.extractions.height,
        failed,
    )

    new_keys = tables.extractions["extraction_key"].to_list()
    if new_keys:
        catalog = get_glue_catalog()
        for key in (CLAIM_KEY, EVIDENCE_KEY):
            identifier = f"{_OUTPUT_METADATA['schema']}.{key.path[-1]}"
            if catalog.table_exists(identifier):
                _clear_partial_write(
                    catalog.load_table(identifier), platform, courserun, new_keys
                )

    partition_columns = [
        pl.lit(platform).alias("platform"),
        pl.lit(courserun).alias("courserun_readable_id"),
    ]
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
            "opik_prompt_version": MetadataValue.text(
                publish_prompt_version(EXTRACTION_PROMPT_NAME, SYSTEM_PROMPT)
            ),
            "input_tokens": MetadataValue.int(
                tables.extractions["input_tokens"].sum() or 0
            ),
            "output_tokens": MetadataValue.int(
                tables.extractions["output_tokens"].sum() or 0
            ),
        },
    )
    yield evidence_check(tables.evidence, config.min_exact_evidence_share)

    # The successful scopes are stored; failing the run gets the rest one retry
    # (upstream_or_code_changes re-requests a newly failed partition once).
    if failed:
        msg = f"{failed} of {len(pending)} scopes failed to extract for {courserun}"
        raise Failure(msg)
