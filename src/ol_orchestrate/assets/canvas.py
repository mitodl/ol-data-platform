import hashlib
import time
from datetime import UTC, datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetKey,
    DataVersion,
    Output,
    StaticPartitionsDefinition,
    asset,
)

# predefined course IDs to export
course_partitions = StaticPartitionsDefinition(["7023"])


@asset(
    key=AssetKey(["canvas", "course_export"]),
    group_name="canvas",
    required_resource_keys={"canvas_api"},
    io_manager_key="s3file_io_manager",
    partitions_def=course_partitions,
)
def export_courses(context: AssetExecutionContext):
    course_id = int(context.partition_key)
    export_date = datetime.now(tz=UTC).strftime("%Y%m%d")
    # only export common cartridge for now
    export_type = "common_cartridge"
    extension = "imscc" if export_type == "common_cartridge" else export_type

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
                "Waiting for course export (state: %s)",
                export_status["workflow_state"],
            )
            retry_count += 1
            time.sleep(30)
    else:
        message = f"Course export timed out for {course_id}"
        context.log.error(message)
        raise Exception(message)  # noqa: TRY002

    course_export_path = Path(f"{course_id}_course_export.{extension}")
    context.resources.canvas_api.client.download_course_export(
        download_url, course_export_path
    )

    with course_export_path.open("rb") as f:
        data_version = hashlib.file_digest(f, "sha256").hexdigest()

    target_path = f"canvas/{export_date}/course_{course_id}/{data_version}.{extension}"

    context.log.info("Downloading %s file to %s", download_url, target_path)

    yield Output(
        value=(course_export_path, target_path),
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_id,
            "course_name": course["name"],
            "course_code": course["course_code"],
            "course_readable_id": course["sis_course_id"],
            "object_key": target_path,
        },
    )
