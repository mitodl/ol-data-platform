"""Learner-facing descriptions for every resolved concept.

Descriptions are written only under a description prompt that
courseware_description_approval approves. int__courseware__concept_description_inputs
carries the approved prompt version on every row, and a run under any other prompt
describes nothing: a run automation requested logs why and stops, and a run
launched any other way fails before any model call.

A promotion lands in two images, the seed row in lakehouse and the prompt in ml,
and either may deploy first. The inputs' next build after the seed promotion
requests a run, and the asset's code version is DESCRIPTION_PROMPT_VERSION, so the
ml deploy that changes the prompt requests one too. The first of the two runs stops
as unapproved and the second describes every concept under the new prompt. A
prompt edit deployed without a promotion requests one run, which stops.
"""

from collections.abc import Iterator

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    Config,
    MetadataValue,
    Output,
    asset,
)

# Not exported from the top-level dagster namespace; see sensors/courseware.py.
from dagster._core.storage.tags import AUTOMATION_CONDITION_TAG
from ml.lib.courseware.describe import (
    BEDROCK_DESCRIPTION_MODEL,
    DESCRIPTION_MODEL,
    DESCRIPTION_PROMPT,
    DESCRIPTION_PROMPT_VERSION,
    DESCRIPTION_SCHEMA,
    describe_concepts,
    description_version,
    pending_concepts,
)
from ml.lib.courseware.extract import DEFAULT_EFFORT, Effort
from ml.lib.courseware.warehouse import anthropic_client
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ml.resources.llm import LLMClientFactory
from ml.resources.opik_auth import publish_prompt_version
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import PermanentFailure, permanent_failure
from pydantic import Field

INPUTS_KEY = AssetKey(["intermediate", "int__courseware__concept_description_inputs"])
DESCRIPTION_KEY = AssetKey(["intermediate", "courseware_concept_description"])
DESCRIPTION_PROMPT_NAME = "courseware-concept-description"


class DescriptionFailure(PermanentFailure):
    """Some concepts got no description; the others are stored.

    Permanent so stop_run_retries keeps run_retries from re-running every batch.
    """


class CoursewareDescriptionConfig(Config):
    model_id: str | None = Field(
        default=None,
        description=(
            "Model id, in the llm resource's namespace. Unset uses "
            f"{BEDROCK_DESCRIPTION_MODEL} on Bedrock and {DESCRIPTION_MODEL} on the "
            "Anthropic API. Part of description_version, so a new model describes "
            "every concept again."
        ),
    )
    effort: Effort = Field(default=DEFAULT_EFFORT, description="output_config.effort")


def unapproved_prompt(inputs: pl.DataFrame) -> str | None:
    """Say why the inputs do not approve the description prompt this code runs.

    :param inputs: int__courseware__concept_description_inputs rows, each carrying
        approved_prompt_version from courseware_description_approval
    :returns: the reason, or None when DESCRIPTION_PROMPT_VERSION is approved
    :rtype: str | None
    """
    # Absent until lakehouse rebuilds the inputs with the approval seed, which may
    # deploy after this image; that approves nothing.
    approved = (
        set(inputs["approved_prompt_version"])
        if "approved_prompt_version" in inputs.columns
        else {None}
    )
    if approved == {DESCRIPTION_PROMPT_VERSION}:
        return None
    named = sorted(version for version in approved if version is not None)
    return (
        f"The description prompt version {DESCRIPTION_PROMPT_VERSION} is not "
        "approved (courseware_description_approval approves "
        f"{', '.join(named) or 'no version'}). Promote it in "
        "src/ol_dbt/seeds/courseware_description_approval.csv to describe every "
        "concept under it, or deploy the approved prompt."
    )


def unversioned_descriptions(stored: pl.LazyFrame | None, version: str) -> pl.DataFrame:
    """Copy the descriptions written at version before rows carried a prompt_version.

    description_version hashes DESCRIPTION_PROMPT with the model and effort, so a row
    at this run's version was written under DESCRIPTION_PROMPT. pending_concepts
    counts it as described, but a knowledge release selects descriptions by
    prompt_version, so without a copy carrying DESCRIPTION_PROMPT_VERSION the
    concept would never be released. A row already copied is not copied again.

    :param stored: the stored descriptions, or None before the first run
    :param version: description_version for the run's model and effort
    :returns: the copies to append, stamped with DESCRIPTION_PROMPT_VERSION
    :rtype: pl.DataFrame
    """
    columns = [*DESCRIPTION_SCHEMA, "prompt_version"]
    if stored is None:
        return pl.DataFrame(schema={**DESCRIPTION_SCHEMA, "prompt_version": pl.String})
    if "prompt_version" not in stored.collect_schema():
        stored = stored.with_columns(prompt_version=pl.lit(None, dtype=pl.String))
    keys = ["concept_id", "input_checksum"]
    at_version = stored.filter(pl.col("description_version") == version)
    return (
        at_version.filter(pl.col("prompt_version").is_null())
        .join(
            at_version.filter(pl.col("prompt_version").is_not_null()).select(keys),
            on=keys,
            how="anti",
        )
        .sort("described_at")
        .unique(keys, keep="last", maintain_order=True)
        .with_columns(prompt_version=pl.lit(DESCRIPTION_PROMPT_VERSION))
        .select(columns)
        .collect()
    )


@asset(
    key=DESCRIPTION_KEY,
    group_name="courseware",
    deps=[INPUTS_KEY],
    io_manager_key="io_manager",
    metadata={
        "schema": warehouse_database("intermediate"),
        "write_mode": "append",
        "schema_update_mode": "update",
    },
    # So the deploy that changes the prompt requests a run even when its promotion
    # deployed first (see the module docstring).
    code_version=DESCRIPTION_PROMPT_VERSION,
    automation_condition=upstream_or_code_changes(),
    # Nothing is written when every concept is already described, so an upstream
    # rebuild does not commit an empty snapshot.
    output_required=False,
    pool="courseware_description",
)
def courseware_concept_description(
    context: AssetExecutionContext,
    config: CoursewareDescriptionConfig,
    llm: LLMClientFactory,
) -> Iterator[Output[pl.DataFrame]]:
    """Describe every concept whose description inputs have no description yet.

    Calls ml.lib.courseware.describe.describe_concepts against the stored
    descriptions and appends only the new rows, so an unchanged concept is never
    sent again. Rows stored before descriptions carried a prompt_version are
    appended again with it (see unversioned_descriptions). Nothing reaches Opik or
    the model when the deployed prompt is not the approved one.
    """
    inputs = require_table("intermediate", INPUTS_KEY.path[-1]).collect()
    if not inputs.height:
        context.log.info("No concepts to describe")
        return
    if refusal := unapproved_prompt(inputs):
        # Each rebuild of the inputs and each prompt deploy requests a run while the
        # deployed prompt and the approved one differ. Failing that run would add a
        # retry and a Sentry issue per request, where an unapproved extraction is
        # never requested at all, so a run automation requested stops here. A run
        # someone launched fails.
        if context.run.tags.get(AUTOMATION_CONDITION_TAG) == "true":
            context.log.warning(refusal)
            return
        raise permanent_failure(refusal)
    stored = read_table("intermediate", DESCRIPTION_KEY.path[-1])
    existing = (
        stored.select(
            "concept_id", "input_checksum", "description_version", "described_at"
        ).collect()
        if stored is not None
        else None
    )
    client, model_id = anthropic_client(
        llm,
        config.model_id,
        default_model=DESCRIPTION_MODEL,
        default_bedrock_model=BEDROCK_DESCRIPTION_MODEL,
    )
    # Before any model call, so nothing Opik does can fail the run after outputs
    # start being written.
    opik_prompt_version = publish_prompt_version(
        DESCRIPTION_PROMPT_NAME, DESCRIPTION_PROMPT
    )
    version = description_version(model_id, config.effort)
    pending = {
        entry["concept_id"] for entry in pending_concepts(inputs, existing, version)
    }
    backfilled = unversioned_descriptions(stored, version)
    # The approved version each row was written under, which a knowledge release
    # selects descriptions by.
    described = describe_concepts(
        inputs, client, model_id=model_id, effort=config.effort, existing=existing
    ).with_columns(prompt_version=pl.lit(DESCRIPTION_PROMPT_VERSION))
    failed = len(pending - set(described["concept_id"]))
    concept_count = inputs["concept_id"].n_unique()
    context.log.info(
        "Described %d of %d concepts; stamped the prompt version on %d earlier rows",
        described["concept_id"].n_unique(),
        concept_count,
        backfilled.height,
    )
    written = pl.concat([backfilled, described])
    if written.height:
        yield Output(
            written,
            metadata={
                "row_count": MetadataValue.int(written.height),
                "backfilled_row_count": MetadataValue.int(backfilled.height),
                "concept_count": MetadataValue.int(concept_count),
                "model_id": MetadataValue.text(model_id),
                "description_version": MetadataValue.text(version),
                "prompt_version": MetadataValue.text(DESCRIPTION_PROMPT_VERSION),
                "failed_concept_count": MetadataValue.int(failed),
                "opik_prompt_version": MetadataValue.text(opik_prompt_version),
            },
        )

    # What succeeded is stored. Permanent so run_retries does not pay for every
    # batch again; automation re-requests once and describes only what is missing.
    if failed:
        raise DescriptionFailure(
            description=f"{failed} of {len(pending)} concepts got no description",
            metadata={"retryable": False, "failed_concept_count": failed},
            allow_retries=False,
        )
