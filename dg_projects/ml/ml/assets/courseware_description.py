"""Learner-facing descriptions for every resolved concept."""

import hashlib
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
from ol_orchestrate.lib.failures import PermanentFailure
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
    code_version=hashlib.sha256(DESCRIPTION_PROMPT.encode()).hexdigest()[:12],
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
    sent again.
    """
    inputs = require_table("intermediate", INPUTS_KEY.path[-1]).collect()
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
