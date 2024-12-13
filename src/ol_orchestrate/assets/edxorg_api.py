# Call edxorg APIs to get courses and programs data
# Model the different asset objects according to their type and data structure
from datetime import UTC, datetime
from pathlib import Path

import jsonlines
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Output,
    multi_asset,
)


@multi_asset(
    group_name="edxorg",
    required_resource_keys={"edxorg_api"},
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
def edxorg_program_metadata(context: AssetExecutionContext):
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()

    program_data_generator = context.resources.edxorg_api.get_edxorg_programs()

    mitx_programs = []
    mitx_program_courses = []
    for result_batch in program_data_generator:
        for program in result_batch:
            # Check if 'authoring_organizations' exists in the program and that it's a
            # list with at least one element
            if (
                "authoring_organizations" in program
                and isinstance(program["authoring_organizations"], list)
                and len(program["authoring_organizations"]) > 0
                and program["authoring_organizations"][0]["key"] == "MITx"
            ):
                program_uuid = program["uuid"]
                mitx_programs.append(
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
                    mitx_program_courses.append(  # noqa: PERF401
                        {
                            "program_uuid": program_uuid,
                            "course_key": course["key"],
                            "course_title": course["title"],
                            "course_short_description": course["short_description"],
                            "course_type": course["course_type"],
                        }
                    )

    program_file = Path(f"program_{data_retrieval_timestamp}.json")
    program_course_file = Path(f"program_course_{data_retrieval_timestamp}.json")
    program_object_key = f"{'/'.join(context.asset_key_for_output('program_metadata').path)}/{data_retrieval_timestamp}.json"  # noqa: E501
    program_course_object_key = f"{'/'.join(context.asset_key_for_output('program_course_metadata').path)}//{data_retrieval_timestamp}.json"  # noqa: E501

    with (
        jsonlines.open(program_file, mode="w") as programs,
        jsonlines.open(program_course_file, mode="w") as program_courses,
    ):
        programs.write(mitx_programs)
        program_courses.write(mitx_program_courses)

    yield Output(
        (program_file, program_object_key),
        output_name="program_metadata",
        metadata={"object_key": program_object_key},
    )

    yield Output(
        (program_course_file, program_course_object_key),
        output_name="program_course_metadata",
        metadata={"object_key": program_course_object_key},
    )
