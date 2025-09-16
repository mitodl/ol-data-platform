import json
import time
from pathlib import Path

import httpx
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    DataVersion,
    Output,
    StaticPartitionsDefinition,
    asset,
    multi_asset,
)

from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import (
    EXPORT_TYPE_COMMON_CARTRIDGE,
    EXPORT_TYPE_EXTENSIONS,
)
from ol_orchestrate.lib.utils import compute_zip_content_hash

# predefined course IDs to export
canvas_course_ids = StaticPartitionsDefinition(
    [
        "155",
        "7023",
        "14566",
        "28766",
        "28768",
        "28770",
        "28772",
        "28774",
        "28777",
        "28751",
        "28753",
        "28755",
        "28759",
        "28765",
        "28767",
        "28785",
        "28760",
        "28761",
        "28762",
        "28803",
        "28805",
        "28807",
        "28808",
        "28811",
        "28813",
        "28815",
        "28816",
        "28818",
        "28821",
        "28822",
        "28824",
        "28839",
        "28841",
        "28842",
        "28845",
        "28847",
        "28849",
        "28880",
        "34062",
        "34545",
        "34625",
        "34631",
        "34642",
        "34653",
        "34716",
        "34721",
        "35054",
    ]
)


def _extract_course_files(context, course_id):
    """Extract course file information."""
    course_file_generator = context.resources.canvas_api.client.get_course_files(
        course_id
    )
    files_detail = []
    for course_file in course_file_generator:
        for file in course_file:
            folder_id = file["folder_id"]
            folder_detail = context.resources.canvas_api.client.get_course_folders(
                course_id, folder_id
            )
            files_detail.append(
                {
                    **{k if k != "url" else "download_url": v for k, v in file.items()},
                    "file_path": f"{folder_detail['full_name']}/{file['display_name']}",
                    "url": f"https://canvas.mit.edu/courses/{course_id}/files/{file['id']}/",
                    "folder": folder_detail,
                }
            )
    return files_detail


def _extract_course_assignments(context, course_id):
    """Extract course assignment information."""
    assignment_generator = context.resources.canvas_api.client.get_course_assignments(
        course_id
    )
    return [
        {
            "id": assignment["id"],
            "name": assignment["name"],
            "html_url": assignment["html_url"],
            "visible_to_everyone": assignment["visible_to_everyone"],
            "published": assignment["published"],
        }
        for batch in assignment_generator
        for assignment in batch
    ]


@multi_asset(
    group_name="canvas",
    required_resource_keys={"canvas_api"},
    partitions_def=canvas_course_ids,
    outs={
        "course_content": AssetOut(
            io_manager_key="s3file_io_manager",
            key=AssetKey(["canvas", "course_content"]),
        ),
        "course_metadata": AssetOut(
            io_manager_key="s3file_io_manager",
            key=AssetKey(["canvas", "course_metadata"]),
        ),
    },
)
def export_course_content(context: AssetExecutionContext):
    course_id = int(context.partition_key)
    # only export common cartridge for now
    export_type = EXPORT_TYPE_COMMON_CARTRIDGE
    extension = EXPORT_TYPE_EXTENSIONS[export_type]

    course = context.resources.canvas_api.client.get_course(course_id)
    export_course_response = context.resources.canvas_api.client.export_course_content(
        course_id, export_type
    )
    context.log.info(
        "Initiated export of course ID %s: %s", course_id, export_course_response
    )
    export_id = export_course_response["id"]

    # Poll until the export is ready
    max_retries = 35  # 70 minutes with 120 seconds interval
    retry_count = 0
    while retry_count < max_retries:
        export_status = context.resources.canvas_api.client.check_course_export_status(
            course_id, export_id
        )
        if export_status["workflow_state"] == "exported":
            attachment = export_status.get("attachment", {})
            download_url = attachment.get("url")
            context.log.info(
                "Export started for course ID %s at %s", course_id, download_url
            )
            break
        elif export_status["workflow_state"] in ["failed", "error"]:
            message = f"Export failed for course {course_id}"
            context.log.error(message)
            raise Exception(message)  # noqa: TRY002
        else:
            context.log.info(
                "Waiting for course content export (state: %s)",
                export_status["workflow_state"],
            )
            retry_count += 1
            time.sleep(120)
    else:
        message = f"Course content export timed out for {course_id}"
        context.log.error(message)
        raise Exception(message)  # noqa: TRY002

    course_content_path = Path(f"{course_id}_course_content.{extension}")
    downloaded_path = context.resources.canvas_api.client.download_course_export(
        download_url, course_content_path
    )

    data_version = compute_zip_content_hash(
        downloaded_path, skip_filename="imsmanifest.xml"
    )

    target_path = f"canvas/course_content/{course_id}/{data_version}.{extension}"

    context.log.info("Downloading %s file to %s", download_url, target_path)

    # Course file ID extraction
    files_detail = _extract_course_files(context, course_id)
    context.log.info(
        "Total extracted %d files from course %s", len(files_detail), course_id
    )

    # Course Assignments
    assignments = _extract_course_assignments(context, course_id)
    context.log.info(
        "Total extracted %d assignments from course %s", len(assignments), course_id
    )

    # course pages
    page_generator = context.resources.canvas_api.client.get_course_pages(course_id)
    pages = [item for batch in page_generator for item in batch]
    context.log.info("Total extracted %d pages from course %s", len(pages), course_id)

    json_file = Path(f"{data_version}.metadata.json")
    json_file_path = f"{'/'.join(context.asset_key_for_output('course_content').path)}/{course_id}/{data_version}.metadata.json"  # noqa: E501

    course_metadata = {
        "course_id": course_id,
        "course_name": course["name"],
        "course_code": course["course_code"],
        "course_readable_id": course["sis_course_id"],
        "course_created_at": course["created_at"],
        "course_files": files_detail,
        "assignments": assignments,
        "pages": pages,
    }
    with Path.open(json_file, "w") as file:
        json.dump(course_metadata, file, indent=2)

    yield Output(
        value=(course_content_path, target_path),
        output_name="course_content",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_id,
            "course_name": course["name"],
            "course_code": course["course_code"],
            "course_readable_id": course["sis_course_id"],
            "object_key": target_path,
        },
    )

    yield Output(
        (json_file, json_file_path),
        output_name="course_metadata",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_id,
            "file_count": len(files_detail),
            "data_version": data_version,
            "object_key": json_file_path,
        },
    )


@asset(
    key=AssetKey(["canvas", "course_content_metadata"]),
    group_name="course_content_metadata",
    description="Notify Learn API via webhook after canvas course export.",
    automation_condition=upstream_or_code_changes(),
    partitions_def=canvas_course_ids,
    ins={
        "canvas_content_export": AssetIn(key=AssetKey(["canvas", "course_content"])),
        "course_metadata_export": AssetIn(key=AssetKey(["canvas", "course_metadata"])),
    },
    required_resource_keys={"canvas_api", "learn_api"},
)
def course_content_metadata(
    context: AssetExecutionContext, canvas_content_export, course_metadata_export
):
    course_id = int(context.partition_key)
    metadata = context.resources.canvas_api.client.get_course(course_id)
    # canvas_content_export is a full path to the exported course content file
    relative_path = str(canvas_content_export).split("canvas/course_content/", 1)[-1]
    content_path = f"canvas/course_content/{relative_path}"

    metadata_relative_path = str(course_metadata_export).split(
        "canvas/course_content/", 1
    )[-1]
    metadata_path = f"canvas/course_content/{metadata_relative_path}"

    data = {
        "course_id": course_id,
        "course_name": metadata["name"],
        "course_code": metadata["course_code"],
        "course_readable_id": metadata["sis_course_id"],
        "content_path": content_path,
        "metadata_path": metadata_path,
        "source": "canvas",
        "time": time.time(),
    }
    context.log.info(
        "Sending webhook notification to Learn API for course_id=%s and data=%s",
        course_id,
        data,
    )

    try:
        response = context.resources.learn_api.client.notify_course_export(data)
        context.log.info(
            "Learn API webhook notification succeeded for course_id=%s, response=%s",
            course_id,
            response,
        )
        return Output(
            value={"course_id": course_id},
            metadata={
                "status": "success",
                "course_id": course_id,
                "response": response,
            },
        )

    except httpx.HTTPStatusError as error:
        error_message = (
            f"Learn API webhook notification failed for course_id={course_id} "
            f"with status code {error.response.status_code} and error: {error!s}"
        )
        context.log.exception(error_message)
        raise Exception(error_message) from error  # noqa: TRY002
