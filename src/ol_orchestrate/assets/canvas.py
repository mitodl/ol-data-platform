import hashlib
import time
from pathlib import Path

import requests
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    DataVersion,
    Output,
    multi_asset,
)

# Define a list of course IDs to export.
course_ids = [7023]


@multi_asset(
    outs={
        f"course_{course_id}": AssetOut(
            key=AssetKey(["canvas", f"course_{course_id}"]),
            io_manager_key="s3file_io_manager",
        )
        for course_id in course_ids
    },
    group_name="canvas",
    required_resource_keys={"canvas_api"},
)
def export_courses(context: AssetExecutionContext):
    for course_id in course_ids:
        course = context.resources.canvas_api.client.get_course(course_id)

        export_course_response = (
            context.resources.canvas_api.client.export_course_content(course_id)
        )
        context.log.info(
            "Initiated export of course ID %s: %s", course_id, export_course_response
        )
        export_id = export_course_response["id"]

        # Poll until the export is ready
        while True:
            export_status = (
                context.resources.canvas_api.client.check_course_export_status(
                    course_id, export_id
                )
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
                time.sleep(30)

        course_export_path = Path(f"{course_id}_course_export.imscc")
        response = requests.get(download_url, timeout=20)
        response.raise_for_status()

        course_export_path.write_bytes(response.content)

        with course_export_path.open("rb") as f:
            data_version = hashlib.file_digest(f, "sha256").hexdigest()

        target_path = f"canvas/course_export/{course_id}/{data_version}.imscc"

        context.log.info("Downloading %s file to %s", download_url, target_path)

        yield Output(
            value=(course_export_path, target_path),
            output_name=f"course_{course_id}",
            data_version=DataVersion(data_version),
            metadata={
                "course_id": course_id,
                "course_name": course["name"],
                "course_code": course["course_code"],
                "course_readable_id": course["sis_course_id"],
                "object_key": target_path,
            },
        )
