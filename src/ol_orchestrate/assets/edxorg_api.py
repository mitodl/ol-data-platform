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
            description="The metadata for programs extracted from edxorg program API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "api_data", "program_metadata")),
        ),
        "program_course_metadata": AssetOut(
            description="The metadata of all the associated program courses extracted "
            "from edxorg program API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "api_data", "program_course_metadata")),
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
    total_extracted_count = 0
    for i, data in enumerate(program_data_generator):
        if i == 0:
            count, result_batch = data  # First iteration gives total count and results
            context.log.info("Total programs to extract: %d programs", count)
        else:
            result_batch = data  # Subsequent iterations give only results

        batch_count = len(result_batch)
        total_extracted_count += batch_count
        context.log.info(
            "Extracted a batch of %d programs. Total so far: %d programs.",
            batch_count,
            total_extracted_count,
        )
        for program in result_batch:
            program_uuid = program["uuid"]
            org_keys = [org["key"] for org in program["authoring_organizations"]]
            org = ", ".join(org_keys)
            edxorg_programs.append(
                {
                    "uuid": program_uuid,
                    "title": program["title"],
                    "subtitle": program["subtitle"],
                    "type": program["type"],
                    "status": program["status"],
                    "authoring_organizations": org,
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

    context.log.info("Total extracted %d programs....", len(edxorg_programs))
    context.log.info(
        "Total extracted %d program courses....", len(edxorg_program_courses)
    )

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
        programs.write_all(edxorg_programs)
        program_courses.write_all(edxorg_program_courses)

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


@multi_asset(
    group_name="edxorg",
    outs={
        "course_metadata": AssetOut(
            description="The metadata for MITx courses extracted from the edxorg's "
            "course catalog API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "api_data", "course_metadata")),
        ),
        "course_run_metadata": AssetOut(
            description="The metadata for MITx course runs extracted from the edxorg's "
            "course catalog API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "api_data", "course_run_metadata")),
        ),
    },
)
def edxorg_mitx_course_metadata(
    context: AssetExecutionContext, edxorg_api: OpenEdxApiClientFactory
):
    mitx_course_generator = edxorg_api.client.get_edxorg_mitx_courses()

    mitx_courses = []
    mitx_course_runs = []
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()
    total_extracted_count = 0
    for i, data in enumerate(mitx_course_generator):
        if i == 0:
            count, result_batch = data
            context.log.info("Total MITx courses to extract: %d courses", count)
        else:
            result_batch = data

        batch_count = len(result_batch)
        total_extracted_count += batch_count
        context.log.info(
            "Extracted a batch of %d MITx courses. Total so far: %d courses.",
            batch_count,
            total_extracted_count,
        )
        for course in result_batch:
            course_key = course["key"]
            owner_keys = [owner["key"] for owner in course["owners"]]
            owner = ", ".join(owner_keys)
            mitx_courses.append(
                {
                    "course_key": course_key,
                    "title": course["title"],
                    "owner": owner,
                    "short_description": course["short_description"],
                    "full_description": course["full_description"],
                    "level_type": course["level_type"],
                    "marketing_url": course["marketing_url"],
                    "image": {
                        "url": course["image"].get("src"),
                        "description": course["image"].get("description"),
                    }
                    if course["image"] and course["image"].get("src")
                    else None,
                    "course_type": course["course_type"],
                    "subjects": [
                        {"name": subject.get("name")}
                        for subject in course.get("subjects", [])
                    ],
                    "prerequisites": course["prerequisites"],
                    "prerequisites_raw": course["prerequisites_raw"],
                    "modified": course["modified"],
                    "retrieved_at": data_retrieval_timestamp,
                }
            )
            for course_run in course["course_runs"]:
                mitx_course_runs.append(  # noqa: PERF401
                    {
                        "course_key": course_key,
                        "run_key": course_run["key"],
                        "title": course_run["title"],
                        "short_description": course_run["short_description"],
                        "full_description": course_run["full_description"],
                        "marketing_url": course_run["marketing_url"],
                        "level_type": course_run["level_type"],
                        "languages": course_run["content_language"],
                        "start_on": course_run["start"],
                        "end_on": course_run["end"],
                        "enrollment_start": course_run["enrollment_start"],
                        "enrollment_end": course_run["enrollment_end"],
                        "announcement": course_run["announcement"],
                        "pacing_type": course_run["pacing_type"],
                        "enrollment_type": course_run["type"],
                        "availability": course_run["availability"],
                        "status": course_run["status"],
                        "is_enrollable": course_run["is_enrollable"],
                        "image": {
                            "url": course_run["image"].get("src"),
                            "description": course_run["image"].get("description"),
                        }
                        if course_run["image"] and course_run["image"].get("src")
                        else None,
                        "seats": course_run["seats"],
                        "staff": [
                            {
                                "first_name": staff.get("given_name"),
                                "last_name": staff.get("family_name"),
                            }
                            for staff in course_run.get("staff")
                        ],
                        "weeks_to_complete": course_run["weeks_to_complete"],
                        "min_effort": course_run["min_effort"],
                        "max_effort": course_run["max_effort"],
                        "estimated_hours": course_run["estimated_hours"],
                        "modified": course_run["modified"],
                        "retrieved_at": data_retrieval_timestamp,
                    }
                )

    context.log.info("Total extracted %d MITx courses....", len(mitx_courses))
    context.log.info("Total extracted %d MITx course runs....", len(mitx_course_runs))

    course_data_version = hashlib.sha256(
        json.dumps(mitx_courses).encode("utf-8")
    ).hexdigest()
    course_run_data_version = hashlib.sha256(
        json.dumps(mitx_course_runs).encode("utf-8")
    ).hexdigest()

    course_file = Path(f"course_{course_data_version}.json")
    course_run_file = Path(f"course_run_{course_run_data_version}.json")
    course_object_key = f"{'/'.join(context.asset_key_for_output('course_metadata').path)}/{course_data_version}.json"  # noqa: E501
    course_run_object_key = f"{'/'.join(context.asset_key_for_output('course_run_metadata').path)}/{course_run_data_version}.json"  # noqa: E501

    with (
        jsonlines.open(course_file, mode="w") as courses,
        jsonlines.open(course_run_file, mode="w") as course_runs,
    ):
        courses.write_all(mitx_courses)
        course_runs.write_all(mitx_course_runs)

    yield Output(
        (course_file, course_object_key),
        output_name="course_metadata",
        data_version=DataVersion(course_data_version),
        metadata={"object_key": course_object_key},
    )

    yield Output(
        (course_run_file, course_run_object_key),
        output_name="course_run_metadata",
        data_version=DataVersion(course_run_data_version),
        metadata={"object_key": course_run_object_key},
    )
