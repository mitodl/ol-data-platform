"""Tell MIT Learn about each Canvas course export.

The export itself is data_loading's (data_loading.defs.canvas.assets); this is
the push half, moved here from the canvas code location (issue #2260). It still
calls the content_files webhook. Moving it onto the scoped-pull endpoint is an
edit to this asset (tk-platform-scoped-pull-sender-client-method-then-s-b3aca9).
"""

import time

import httpx2 as httpx
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetSpec,
    DynamicPartitionsDefinition,
    Output,
    asset,
)
from ol_orchestrate.lib.http_errors import http_failure

# Shared with data_loading's export by name: dynamic partitions are stored
# instance-wide, keyed only on the definition's name, and the course ID sheet
# sensor there is what adds and removes them.
canvas_course_ids = DynamicPartitionsDefinition(name="canvas_course_ids")

# The exports this asset reads, materialized in data_loading. An input that loads
# a value has to name an asset the location defines, so they are declared here as
# external specs; the workspace merges them with data_loading's definitions.
canvas_course_exports = [
    AssetSpec(
        key=AssetKey(["canvas", "course_content"]), partitions_def=canvas_course_ids
    ),
    AssetSpec(
        key=AssetKey(["canvas", "course_metadata"]), partitions_def=canvas_course_ids
    ),
]


@asset(
    code_version="canvas_course_export_webhook_v1",
    key=AssetKey(["canvas", "course_content_metadata"]),
    group_name="course_content_metadata",
    description="Notify Learn API via webhook after canvas course export.",
    partitions_def=canvas_course_ids,
    # Loaded through the file object IO manager, which resolves the path from
    # the latest materialization's metadata, whichever location wrote it. An
    # external spec would otherwise load through this location's io_manager.
    ins={
        "canvas_content_export": AssetIn(
            key=AssetKey(["canvas", "course_content"]),
            input_manager_key="s3file_io_manager",
        ),
        "course_metadata_export": AssetIn(
            key=AssetKey(["canvas", "course_metadata"]),
            input_manager_key="s3file_io_manager",
        ),
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
        "content_path": content_path,
        "metadata_path": metadata_path,
        "source": "canvas",
        "time": time.time(),
    }
    # Learn rejects a null course_readable_id, which Canvas returns for any
    # course with no SIS mapping.
    if metadata["sis_course_id"]:
        data["course_readable_id"] = metadata["sis_course_id"]
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
        context.log.exception(
            "Learn API webhook notification failed for course_id=%s with status "
            "code %s",
            course_id,
            error.response.status_code,
        )
        raise http_failure(
            error,
            "Learn API webhook notification failed",
            metadata={"course_id": course_id},
        ) from error
