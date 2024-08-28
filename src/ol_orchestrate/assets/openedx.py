# - Query the openedx api to get course structures and course blocks data
# - Model the different asset objects according to their type

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import jsonlines
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    AutoMaterializePolicy,
    AutomationCondition,
    DataVersion,
    Output,
    asset,
    multi_asset,
)
from flatten_dict import flatten
from flatten_dict.reducers import make_reducer

from ol_orchestrate.lib.openedx import un_nest_course_structure
from ol_orchestrate.partitions.openedx import (
    OPENEDX_COURSE_RUN_PARTITION,
)


@asset(
    key=AssetKey(["openedx", "courseware"]),
    partitions_def=OPENEDX_COURSE_RUN_PARTITION,
    required_resource_keys={"openedx"},
    description=("An instance of courseware running in an Open edX environment."),
    automation_condition=AutomationCondition.on_cron(
        cron_schedule="0 * * * *"
    ).missing(),
)
def openedx_live_courseware(context: AssetExecutionContext):
    courserun_id = context.partition_key
    # Retrieve the last published timestamp from
    # /learning_sequences/v1/course_outline/{course_key_str}, using the last published
    # information as the data version
    course_outline = context.resources.openedx.client.get_course_outline(courserun_id)
    return Output(
        course_outline,
        data_version=DataVersion(course_outline["published_version"]),
        metadata={
            "course_key": courserun_id,
            "course_title": course_outline["title"],
            "courseware_published_version": course_outline["published_version"],
            "courseware_published_at": course_outline["published_at"],
        },
    )


@multi_asset(
    outs={
        "course_structure": AssetOut(
            key=AssetKey(("openedx", "raw_data", "course_structure")),
            description=("A json file with the course structure information."),
            auto_materialize_policy=AutoMaterializePolicy.eager(),
            io_manager_key="s3file_io_manager",
        ),
        "course_blocks": AssetOut(
            key=AssetKey(("openedx", "raw_data", "course_blocks")),
            description=(
                "A json file containing the hierarchical representation"
                "of the course structure information with course blocks."
            ),
            auto_materialize_policy=AutoMaterializePolicy.eager(),
            io_manager_key="s3file_io_manager",
        ),
    },
    ins={"courseware": AssetIn(key=AssetKey(["openedx", "courseware"]))},
    partitions_def=OPENEDX_COURSE_RUN_PARTITION,
    group_name="openedx",
    required_resource_keys={"openedx"},
)
def course_structure(context: AssetExecutionContext, courseware):  # noqa: ARG001
    partition_dimensions = context.partition_key.keys_by_dimension()
    source_system = context.resources.openedx.deployment
    course_id = partition_dimensions["course_key"]
    course_structure = context.resources.openedx.client.get_course_structure_document(
        course_id
    )
    course_structure_document = json.load(course_structure.open())
    data_version = hashlib.sha256(
        json.dumps(course_structure_document).encode("utf-8")
    ).hexdigest()
    structures_file = Path(f"course_structures_{data_version}.json")
    blocks_file = Path(f"course_blocks_{data_version}.json")
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()
    with (
        jsonlines.open(structures_file, mode="w") as structures,
        jsonlines.open(blocks_file, mode="w") as blocks,
    ):
        table_row = {
            "content_hash": hashlib.sha256(
                json.dumps(course_structure_document).encode("utf-8")
            ).hexdigest(),
            "course_id": context.partition_key,
            "course_structure": course_structure_document,
            "course_structure_flattened": flatten(
                course_structure_document,
                reducer=make_reducer("__"),
            ),
            "retrieved_at": data_retrieval_timestamp,
        }
        structures.write(table_row)
        for block in un_nest_course_structure(
            course_id, course_structure_document, data_retrieval_timestamp
        ):
            blocks.write(block)
    structure_object_key = f"{source_system}_openedx_extracts/course_structure/{course_id}/course_structures_{data_version}.json"  # noqa: E501
    blocks_object_key = f"{source_system}_openedx_extracts/course_blocks/{course_id}/course_blocks_{data_version}.json"  # noqa: E501
    yield Output(
        (structures_file, structure_object_key),
        output_name="flattened_course_structure",
        data_version=DataVersion(data_version),
        metadata={"course_id": course_id, "object_key": structure_object_key},
    )
    yield Output(
        (blocks_file, blocks_object_key),
        output_name="course_blocks",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_id,
            "object_key": blocks_object_key,
        },
    )
