import time
from pathlib import Path

import httpx
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    DataVersion,
    Output,
    StaticPartitionsDefinition,
    asset,
)

from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.constants import (
    EXPORT_TYPE_COMMON_CARTRIDGE,
    EXPORT_TYPE_EXTENSIONS,
)
from ol_orchestrate.lib.utils import compute_zip_content_hash

# predefined course IDs to export
canvas_course_ids = StaticPartitionsDefinition(["7023"])


@asset(
    key=AssetKey(["canvas", "course_content"]),
    group_name="canvas",
    required_resource_keys={"canvas_api"},
    io_manager_key="s3file_io_manager",
    partitions_def=canvas_course_ids,
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
    max_retries = 20  # 10 minutes with 30 seconds interval
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
            time.sleep(30)
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

    yield Output(
        value=(course_content_path, target_path),
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_id,
            "course_name": course["name"],
            "course_code": course["course_code"],
            "course_readable_id": course["sis_course_id"],
            "object_key": target_path,
        },
    )


@asset(
    key=AssetKey(["canvas", "course_content_metadata"]),
    group_name="course_content_metadata",
    description="Notify Learn API via webhook after canvas course export.",
    automation_condition=upstream_or_code_changes(),
    partitions_def=canvas_course_ids,
    ins={"canvas_content_export": AssetIn(key=AssetKey(["canvas", "course_content"]))},
    required_resource_keys={"canvas_api", "learn_api"},
)
def course_content_metadata(context: AssetExecutionContext, canvas_content_export):
    course_id = int(context.partition_key)
    metadata = context.resources.canvas_api.client.get_course(course_id)
    # canvas_content_export is a full path to the exported course content file
    relative_path = str(canvas_content_export).split("canvas/course_content/", 1)[-1]
    content_path = f"canvas/course_content/{relative_path}"

    data = {
        "course_id": course_id,
        "course_name": metadata["name"],
        "course_code": metadata["course_code"],
        "course_readable_id": metadata["sis_course_id"],
        "content_path": content_path,
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
