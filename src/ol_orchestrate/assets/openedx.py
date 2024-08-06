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
    Config,
    DagsterEventType,
    DataVersion,
    EventRecordsFilter,
    Output,
    asset,
    multi_asset,
)
from flatten_dict import flatten
from flatten_dict.reducers import make_reducer
from pydantic import Field
from upath import UPath

from ol_orchestrate.lib.openedx import un_nest_course_structure


@asset(
    key=AssetKey(("openedx", "raw_data", "course_list_config")),
    # partitions_def=,
    group_name="openedx",
)
class CourseListConfig(Config):
    edx_course_api_page_size: int = Field(
        default=100,
        description="The number of records to return per API request. This can be "
        "modified to address issues with rate limiting.",
    )


@asset(
    # partitions_def=,
    key=AssetKey(("openedx", "raw_data", "course_list")),
    group_name="openedx",
    io_manager_key="s3file_io_manager",
    # todo: best practice for passing in the config for page_size?
    ins={
        "course_list_config": AssetIn(
            key=AssetKey(("openedx", "raw_data", "course_list_config"))
        )
    },
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=None
    ),
)
def course_list(
    context: AssetExecutionContext, config: Config
):
    course_ids = []
    course_id_generator = context.resources.openedx.get_edx_course_ids(
        page_size=config.edx_course_api_page_size,
    )
    for result_set in course_id_generator:
        course_ids.extend([course["id"] for course in result_set])
    yield Output(course_ids)

@multi_asset(
    outs={
        "course_structure": AssetOut(
            key=AssetKey(("openedx", "raw_data", "course_structure")),
            io_manager_key="s3file_io_manager",
            description=(
                "A json file with the course structure information."
            ),
            auto_materialize_policy=AutoMaterializePolicy.eager(
                max_materializations_per_minute=None
            ),
        ),
        "course_blocks": AssetOut(
            key=AssetKey(("openedx", "raw_data", "course_blocks")),
            io_manager_key="s3file_io_manager",
            description=(
                "A json file containing the hierarchical representation"
                "of the course structure information with course blocks."
            ),
            auto_materialize_policy=AutoMaterializePolicy.eager(
                max_materializations_per_minute=None
            ),
        ),
    },
    ins={
        "course_list": AssetIn(
            key=AssetKey(("openedx", "raw_data", "course_list"))
        )
    },
    # partitions_def=,
    group_name="openedx",
)
def course_structure(
    context: AssetExecutionContext, course_list: list[str],
):
    dagster_instance = context.instance
    ## TODO: Is this correctly iterating over the list of course_ids?
    input_asset_materialization_event =  dagster_instance.get_event_records(
        event_records_filter=EventRecordsFilter(
            asset_key=context.asset_key_for_input("course_list"),
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_partitions=[context.partiton_key],
        ),
        limit=1,
    )[0]
    course_id = input_asset_materialization_event.asset_materialization.metadata[
        "course_id"
    ]
    course_structure = context.resources.openedx.get_course_structure_document(
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
    # todo: How do i get the open_edx_deployment from the context? instance?
    structure_object_key = f"{context.open_edx_deployment}_openedx_extracts/course_structure/{context.partition_key}/course_structures_{data_version}.json" # noqa: E501
    blocks_object_key = f"{context.open_edx_deployment}_openedx_extracts/course_blocks/{context.partition_key}/course_blocks_{data_version}.json" # noqa: E501
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
            "course_id": course_id, "object_key": blocks_object_key,
        },
    )
