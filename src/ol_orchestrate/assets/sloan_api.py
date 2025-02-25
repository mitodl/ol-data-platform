# Call MIT Sloan Executive Education APIs to get courses and course-offerings data
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

from ol_orchestrate.resources.oauth import OAuthApiClientFactory


@multi_asset(
    group_name="sloan_executive_education",
    outs={
        "course_metadata": AssetOut(
            description="The metadata for courses extracted from sloan course API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("sloan_executive_education", "course_metadata")),
        ),
        "course_offering_metadata": AssetOut(
            description="The metadata for course offerings extracted from sloan course "
            "offering API",
            io_manager_key="s3file_io_manager",
            key=AssetKey(("sloan_executive_education", "course_offering_metadata")),
        ),
    },
)
def sloan_course_metadata(
    context: AssetExecutionContext, sloan_api: OAuthApiClientFactory
):
    data_retrieval_timestamp = datetime.now(tz=UTC).isoformat()

    sloan_courses = sloan_api.client.get_sloan_courses()
    courses = [
        {
            "course_id": course["Course_Id"],
            "title": course["Title"],
            "description": course["Description"],
            "course_url": course["URL"],
            "certification_type": course["Certification_Type"],
            "topics": course["Topics"],
            "image_url": course["Image_Src"],
            "created": course["SourceCreateDate"],
            "modified": course["SourceLastModifiedDate"],
            "retrieved_at": data_retrieval_timestamp,
        }
        for course in sloan_courses
    ]

    context.log.info("Total extracted %d Sloan courses....", len(courses))

    sloan_course_offerings = sloan_api.client.get_sloan_course_offerings()
    course_offerings = [
        {
            "title": course_offering["CO_Title"],
            "course_id": course_offering["Course_Id"],
            "start_date": course_offering["Start_Date"],
            "end_date": course_offering["End_Date"],
            "delivery": course_offering["Delivery"],
            "duration": course_offering["Duration"],
            "price": course_offering["Price"],
            "continuing_ed_credits": course_offering["Continuing_Ed_Credits"],
            "time_commitment": course_offering["Time_Commitment"],
            "location": course_offering["Location"],
            "tuition_cost_non_usd": course_offering["Tuition_Cost(non-USD)"],
            "currency": course_offering["Currency"],
            "faculty": course_offering["Faculty_Name"],
            "retrieved_at": data_retrieval_timestamp,
        }
        for course_offering in sloan_course_offerings
    ]
    context.log.info(
        "Total extracted %d Sloan course offerings....", len(course_offerings)
    )

    course_data_version = hashlib.sha256(
        json.dumps(courses).encode("utf-8")
    ).hexdigest()
    course_offering_data_version = hashlib.sha256(
        json.dumps(course_offerings).encode("utf-8")
    ).hexdigest()

    course_file = Path(f"course_{course_data_version}.json")
    course_offering_file = Path(f"course_offering_{course_offering_data_version}.json")
    course_object_key = f"{'/'.join(context.asset_key_for_output('course_metadata').path)}/{course_data_version}.json"  # noqa: E501
    course_offering_object_key = f"{'/'.join(context.asset_key_for_output('course_offering_metadata').path)}/{course_offering_data_version}.json"  # noqa: E501

    with (
        jsonlines.open(course_file, mode="w") as course,
        jsonlines.open(course_offering_file, mode="w") as offering,
    ):
        course.write_all(courses)
        offering.write_all(course_offerings)

    yield Output(
        (course_file, course_object_key),
        output_name="course_metadata",
        data_version=DataVersion(course_data_version),
        metadata={"object_key": course_object_key},
    )

    yield Output(
        (course_offering_file, course_offering_object_key),
        output_name="course_offering_metadata",
        data_version=DataVersion(course_offering_data_version),
        metadata={"object_key": course_offering_object_key},
    )
