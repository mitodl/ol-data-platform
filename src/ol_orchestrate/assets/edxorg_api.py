# Call edxorg APIs to get courses and programs data
# Model the different asset objects according to their type and data structure
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import jsonlines
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    DataVersion,
    Output,
    multi_asset,
)

from ol_orchestrate.resources.openedx import OpenEdxApiClientFactory


@multi_asset(
    group_name="edxorg",
    outs={
        "program_metadata": AssetOut(
            description="The metadata for MITx programs extracted from edxorg "
            "program API",
            key=AssetKey(("edxorg", "processed_data", "program_metadata")),
        ),
        "program_course_metadata": AssetOut(
            description="The metadata of all the associated program courses extracted "
            "from edxorg program API",
            key=AssetKey(("edxorg", "processed_data", "program_course_metadata")),
        ),
    },
)
def edxorg_program_metadata(
    context: AssetExecutionContext, edxorg_api: OpenEdxApiClientFactory
):
    program_data_generator = edxorg_api.client.get_edxorg_programs()

    edxorg_programs = []
    edxorg_program_courses = []
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()
    for result_batch in program_data_generator:
        for program in result_batch:
            program_uuid = program["uuid"]
            edxorg_programs.append(
                {
                    "uuid": program_uuid,
                    "title": program["title"],
                    "subtitle": program["subtitle"],
                    "type": program["type"],
                    "status": program["status"],
                    "data_modified_timestamp": program["data_modified_timestamp"],
                    "retrieved_at": data_retrieval_timestamp,
                }
            )
            for course in program["courses"]:
                edxorg_program_courses.append(  # noqa: PERF401
                    {
                        "program_uuid": program_uuid,
                        "course_key": course["key"],
                        "course_title": course["title"],
                        "course_short_description": course["short_description"],
                        "course_type": course["course_type"],
                    }
                )

    context.log.info("Extracted %d programs....", len(edxorg_programs))
    context.log.info("Extracted %d program courses....", len(edxorg_program_courses))

    program_data_version = hashlib.sha256(
        json.dumps(edxorg_programs).encode("utf-8")
    ).hexdigest()
    program_course_data_version = hashlib.sha256(
        json.dumps(edxorg_program_courses).encode("utf-8")
    ).hexdigest()

    program_file = Path(f"program_{program_data_version}.json")
    program_course_file = Path(f"program_course_{program_course_data_version}.json")
    program_object_key = f"{'/'.join(context.asset_key_for_output('program_metadata').path)}/{program_data_version}.json"  # noqa: E501
    program_course_object_key = f"{'/'.join(context.asset_key_for_output('program_course_metadata').path)}/{program_course_data_version}.json"  # noqa: E501

    with (
        jsonlines.open(program_file, mode="w") as programs,
        jsonlines.open(program_course_file, mode="w") as program_courses,
    ):
        programs.write(edxorg_programs)
        program_courses.write(edxorg_program_courses)

    yield Output(
        (program_file, program_object_key),
        output_name="program_metadata",
        data_version=DataVersion(program_data_version),
        metadata={"object_key": program_object_key},
    )

    yield Output(
        (program_course_file, program_course_object_key),
        output_name="program_course_metadata",
        data_version=DataVersion(program_course_data_version),
        metadata={"object_key": program_course_object_key},
    )
