"""Learner-facing descriptions for every resolved concept.

Descriptions are written only under a description prompt that
courseware_description_approval approves. int__courseware__concept_description_inputs
carries the approved prompt version on every row, so the inputs' next build after
a promotion requests a run under the new prompt, while a prompt edit deployed
without a promotion fails before any model call instead of describing every
concept again. The asset carries no code version for the same reason.
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
from ml.lib.courseware.describe import (
    BEDROCK_DESCRIPTION_MODEL,
    DESCRIPTION_MODEL,
    DESCRIPTION_PROMPT,
    DESCRIPTION_PROMPT_VERSION,
    describe_concepts,
    description_version,
    pending_concepts,
)
from ml.lib.courseware.extract import DEFAULT_EFFORT, Effort
from ml.lib.courseware.warehouse import (
    anthropic_client,
    read_table,
    require_table,
    warehouse_database,
)
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


def require_approved_prompt(inputs: pl.DataFrame) -> None:
    """Fail unless the inputs approve the description prompt this code runs.

    :param inputs: int__courseware__concept_description_inputs rows, each carrying
        approved_prompt_version from courseware_description_approval
    :raises PermanentFailure: when the approved version is not
        DESCRIPTION_PROMPT_VERSION, or no version is approved
    """
    approved = set(inputs["approved_prompt_version"])
    if approved != {DESCRIPTION_PROMPT_VERSION}:
        named = sorted(version for version in approved if version is not None)
        msg = (
            f"The description prompt version {DESCRIPTION_PROMPT_VERSION} is not "
            "approved (courseware_description_approval approves "
            f"{', '.join(named) or 'no version'}). Promote it in "
            "src/ol_dbt/seeds/courseware_description_approval.csv to describe every "
            "concept under it, or deploy the approved prompt."
        )
        raise permanent_failure(msg)


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
    # No code_version: a prompt change reaches the corpus only through
    # courseware_description_approval (see the module docstring).
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
    sent again. Fails before Opik or the model is called when the deployed prompt
    is not the approved one, however the run was launched.
    """
    inputs = require_table("intermediate", INPUTS_KEY.path[-1]).collect()
    if not inputs.height:
        context.log.info("No concepts to describe")
        return
    require_approved_prompt(inputs)
    stored = read_table("intermediate", DESCRIPTION_KEY.path[-1])
    existing = (
        stored.select("concept_id", "input_checksum", "description_version").collect()
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
    described = describe_concepts(
        inputs, client, model_id=model_id, effort=config.effort, existing=existing
    )
    failed = len(pending - set(described["concept_id"]))
    concept_count = inputs["concept_id"].n_unique()
    context.log.info(
        "Described %d of %d concepts", described["concept_id"].n_unique(), concept_count
    )
    if described.height:
        yield Output(
            described,
            metadata={
                "row_count": MetadataValue.int(described.height),
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
