# - Query the openedx api to get course structures and course blocks data
# - Model the different asset objects according to their type

import hashlib
import io
import json
import tarfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import httpx
import jsonlines
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    DataVersion,
    Output,
    asset,
    multi_asset,
)
from flatten_dict import flatten
from flatten_dict.reducers import make_reducer
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.openedx import (
    process_course_xml,
    process_course_xml_blocks,
    process_video_xml,
    un_nest_course_structure,
)
from upath import UPath

HTTP_SUCCESS = 200
HTTP_NOT_FOUND = 404
COURSE_EXPORT_GET_TASKS_STATUS_TIMEOUT = timedelta(minutes=60)


@asset(
    description=("An instance of courseware running in an Open edX environment."),
    group_name="openedx",
    key=AssetKey(["openedx", "courseware"]),
    required_resource_keys={"openedx"},
)
def openedx_live_courseware(context: AssetExecutionContext):
    courserun_id = context.partition_key
    # Retrieve the last published timestamp from
    # /learning_sequences/v1/course_outline/{course_key_str}, using the last published
    # information as the data version
    course_outline = context.resources.openedx.client.get_course_outline(courserun_id)
    return Output(
        course_outline,
        data_version=DataVersion(course_outline["published_version"]),
        metadata={
            "course_key": courserun_id,
            "course_title": course_outline["title"],
            "courseware_published_version": course_outline["published_version"],
            "courseware_published_at": course_outline["published_at"],
        },
    )


@multi_asset(
    group_name="openedx",
    ins={"courseware": AssetIn(key=AssetKey(["openedx", "courseware"]))},
    outs={
        "course_blocks": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "A json file containing the hierarchical representation"
                "of the course structure information with course blocks."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_blocks")),
            is_required=False,
        ),
        "course_structure": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=("A json file with the course structure information."),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_structure")),
            is_required=False,
        ),
    },
    required_resource_keys={"openedx"},
)
def course_structure(context: AssetExecutionContext, courseware):  # noqa: ARG001
    course_id = context.partition_key
    course_status = context.resources.openedx.client.check_course_status(course_id)
    context.log.info("Course status for %s: %s", course_id, course_status)
    # if the course is found, trigger the XML export
    if course_status == HTTP_SUCCESS:
        course_structure_document = (
            context.resources.openedx.client.get_course_structure_document(course_id)
        )
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
        structure_object_key = f"{'/'.join(context.asset_key_for_output('course_structure').path)}/{course_id}/{data_version}.json"  # noqa: E501
        blocks_object_key = f"{'/'.join(context.asset_key_for_output('course_blocks').path)}/{course_id}/{data_version}.json"  # noqa: E501
        yield Output(
            (structures_file, structure_object_key),
            output_name="course_structure",
            data_version=DataVersion(data_version),
            metadata={"course_id": course_id, "object_key": structure_object_key},
        )
        yield Output(
            (blocks_file, blocks_object_key),
            output_name="course_blocks",
            data_version=DataVersion(data_version),
            metadata={
                "course_id": course_id,
                "object_key": blocks_object_key,
            },
        )
    # if the course is not found, refer to the last successful materialization
    elif course_status == HTTP_NOT_FOUND:
        context.log.info("Course %s not found in the Open edX platform.", course_id)
    # if the course status query results in some other error, raise an exception
    else:
        err_msg = f"Unexpected course status: {course_status} for course: {course_id}"
        context.log.exception(err_msg)
        raise ValueError(err_msg)


@asset(
    automation_condition=upstream_or_code_changes(),
    description=(
        "An importable artifact representing the contents of an Open edX course."
    ),
    group_name="openedx",
    ins={"courseware": AssetIn(key=AssetKey(["openedx", "courseware"]))},
    io_manager_key="s3file_io_manager",
    key=AssetKey(["openedx", "raw_data", "course_xml"]),
    required_resource_keys={"openedx", "s3"},
    output_required=False,
)
def course_xml(context: AssetExecutionContext, courseware):  # noqa: ARG001
    course_key = context.partition_key
    course_status = context.resources.openedx.client.check_course_status(course_key)
    # if the course is found, trigger the XML export
    if course_status == HTTP_SUCCESS:
        exported_courses = context.resources.openedx.client.export_courses(
            course_ids=[course_key],
        )
        context.log.debug(
            "Initiated export of course %s: %s", course_key, exported_courses
        )
        successful_exports: set[str] = set()
        failed_exports: set[str] = set()
        tasks = exported_courses["upload_task_ids"]
        start_time = datetime.now(tz=UTC)
        while len(successful_exports.union(failed_exports)) < len(tasks):
            if (
                datetime.now(tz=UTC) - start_time
                > COURSE_EXPORT_GET_TASKS_STATUS_TIMEOUT
            ):
                err_msg = f"Course export timed out for {course_key}"
                raise TimeoutError(err_msg)
            time.sleep(timedelta(seconds=20).seconds)
            for course_id, task_id in tasks.items():
                task_status = (
                    context.resources.openedx.client.check_course_export_status(
                        course_id,
                        task_id,
                    )
                )
                state = task_status.get("state")
                details = task_status.get("details")
                if state == "Succeeded":
                    successful_exports.add(course_id)
                elif state in {"Failed", "Canceled", "Retrying"}:
                    failed_exports.add(course_id)
                elif details:
                    context.log.info(
                        "Details of export task for course %s (task %s): %s",
                        course_id,
                        task_id,
                        details,
                    )
        if failed_exports:
            errmsg = f"Unable to export the course XML for {course_key}"
            raise Exception(errmsg)  # noqa: TRY002
        s3_location = exported_courses["upload_urls"][course_key]
        context.log.debug("Attempting to download the course XML from %s", s3_location)
        s3_path = urlparse(s3_location)
        course_file = Path(f"{course_key}.xml.tar.gz")
        context.resources.s3.download_file(
            Bucket=s3_path.hostname.split(".")[0],
            Key=s3_path.path.lstrip("/"),
            Filename=course_file,
        )
        data_version = hashlib.file_digest(course_file.open("rb"), "sha256").hexdigest()
        target_path = f"{'/'.join(context.asset_key.path)}/{context.partition_key}/{data_version}.xml.tar.gz"  # noqa: E501
        yield Output(
            (course_file, target_path),
            data_version=DataVersion(data_version),
            metadata={"course_id": course_key, "object_key": target_path},
        )
    # if the course is not found, refer to the last successful materialization
    elif course_status in {None, HTTP_NOT_FOUND}:
        context.log.info("Course %s not found in the Open edX platform.", course_key)
    # if the course status query results in some other error, raise an exception
    else:
        err_msg = f"Unexpected course status: {course_status} for course: {course_key}"
        context.log.exception(err_msg)
        raise ValueError(err_msg)


@multi_asset(
    group_name="openedx",
    ins={"course_xml": AssetIn(key=AssetKey(("openedx", "raw_data", "course_xml")))},
    outs={
        "course_metadata": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Metadata about the course run that is extracted from the XML export."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_metadata")),
        ),
        "course_video": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Details about the video elements in the course that are extracted "
                "from the XML export."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_video")),
        ),
        "course_xml_blocks": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Comprehensive block-level data extracted from course XML archives, "
                "including chapters, sequentials, verticals, and content components"
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_xml_blocks")),
        ),
        "course_static_assets": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Non-XML files extracted from the course archive (e.g. SRT subtitles, "
                "HTML content, PDFs), bundled as a tar archive for downstream use."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("openedx", "processed_data", "course_static_assets")),
        ),
        "course_static_assets_manifest": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Manifest JSON listing every non-XML static file in the course archive "
                "(path, MIME type, size). Versioned by a content hash so downstream "
                "consumers can detect changes without fetching the full archive."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(
                ("openedx", "processed_data", "course_static_assets_manifest")
            ),
        ),
    },
    required_resource_keys={"openedx"},
)
def extract_courserun_details(context: AssetExecutionContext, course_xml: UPath):
    # Download the remote file to the current working directory
    course_xml_path = Path(NamedTemporaryFile(delete=False, suffix=".xml.tar.gz").name)
    context.log.info(
        "Attempting to download course XML from %s to %s", course_xml, course_xml_path
    )
    course_xml.fs.get_file(str(course_xml), str(course_xml_path))
    data_version = hashlib.file_digest(course_xml_path.open("rb"), "sha256").hexdigest()

    # Process the course metadata
    course_metadata = process_course_xml(course_xml_path)
    course_metadata_file = Path(
        NamedTemporaryFile(delete=False, suffix="_metadata.json").name
    )
    course_metadata_file.write_text(json.dumps(course_metadata))
    course_metadata_object_key = f"{'/'.join(context.asset_key_for_output('course_metadata').path)}/{context.partition_key}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_metadata_file, course_metadata_object_key),
        output_name="course_metadata",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": context.partition_key,
            "object_key": course_metadata_object_key,
        },
    )

    # Process the course video details
    video_details = process_video_xml(course_xml_path)
    course_video_file = Path(
        NamedTemporaryFile(delete=False, suffix="_video.jsonl").name
    )
    jsonlines.open(course_video_file, "w").write_all(video_details)
    course_video_object_key = f"{'/'.join(context.asset_key_for_output('course_video').path)}/{context.partition_key}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_video_file, course_video_object_key),
        output_name="course_video",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": context.partition_key,
            "object_key": course_video_object_key,
        },
    )

    # Process comprehensive XML block data and collect non-XML assets
    source_system = context.resources.openedx.deployment
    xml_blocks, static_bundle = process_course_xml_blocks(
        course_xml_path, source_system
    )
    course_xml_blocks_file = Path(
        NamedTemporaryFile(delete=False, suffix="_xml_blocks.jsonl").name
    )
    with jsonlines.open(course_xml_blocks_file, "w") as writer:
        writer.write_all(block.model_dump() for block in xml_blocks)
    course_xml_blocks_object_key = f"{'/'.join(context.asset_key_for_output('course_xml_blocks').path)}/{source_system}/{context.partition_key}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_xml_blocks_file, course_xml_blocks_object_key),
        output_name="course_xml_blocks",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": context.partition_key,
            "object_key": course_xml_blocks_object_key,
            "block_count": len(xml_blocks),
        },
    )

    # Materialize non-XML static assets to S3. Files are bundled into a single
    # tar archive per course. The data_version is a content hash of the static
    # files themselves, so it only changes when the assets actually change.
    static_assets_file = Path(
        NamedTemporaryFile(delete=False, suffix="_static_assets.tar.gz").name
    )
    with tarfile.open(static_assets_file, "w:gz") as assets_tar:
        for relative_path, asset_bytes in static_bundle.files:
            info = tarfile.TarInfo(name=relative_path)
            info.size = len(asset_bytes)
            assets_tar.addfile(info, io.BytesIO(asset_bytes))
    course_static_assets_object_key = f"{'/'.join(context.asset_key_for_output('course_static_assets').path)}/{source_system}/{context.partition_key}/{static_bundle.data_version}.tar.gz"  # noqa: E501
    yield Output(
        (static_assets_file, course_static_assets_object_key),
        output_name="course_static_assets",
        data_version=DataVersion(static_bundle.data_version),
        metadata={
            "course_id": context.partition_key,
            "object_key": course_static_assets_object_key,
            "asset_count": static_bundle.manifest["file_count"],
        },
    )

    # Materialize the manifest as a separate lightweight JSON object so downstream
    # consumers can inspect available files and check the version without fetching
    # the full archive. The key uses a fixed name (manifest.json) so it is always
    # overwritten with the latest and cheaply readable without knowing the version.
    static_manifest_file = Path(
        NamedTemporaryFile(delete=False, suffix="_static_manifest.json").name
    )
    static_manifest_file.write_text(json.dumps(static_bundle.manifest, indent=2))
    course_static_assets_manifest_object_key = f"{'/'.join(context.asset_key_for_output('course_static_assets_manifest').path)}/{source_system}/{context.partition_key}/manifest.json"  # noqa: E501
    yield Output(
        (static_manifest_file, course_static_assets_manifest_object_key),
        output_name="course_static_assets_manifest",
        data_version=DataVersion(static_bundle.data_version),
        metadata={
            "course_id": context.partition_key,
            "object_key": course_static_assets_manifest_object_key,
            "asset_count": static_bundle.manifest["file_count"],
        },
    )

    course_xml_path.unlink()


@asset(
    code_version="openedx_course_export_webhook_v1",
    key=AssetKey(["openedx", "course_content_webhook"]),
    group_name="course_content_metadata",
    description="Notify Learn API via webhook after Open edX course export.",
    automation_condition=upstream_or_code_changes(),
    ins={
        "course_xml": AssetIn(key=AssetKey(["openedx", "raw_data", "course_xml"])),
    },
    required_resource_keys={"openedx", "learn_api"},
    output_required=False,
)
def openedx_course_content_webhook(
    context: AssetExecutionContext, course_xml: UPath | None
):
    """Send webhook notification to Learn API after Open edX course XML export.

    Sends a notification for xpro, mitxonline, and edxorg deployments.
    Skips notification for mitx deployment.
    """
    course_id = context.partition_key
    source = context.resources.openedx.deployment

    # Skip webhook for mitx deployment
    if source == "mitx":
        context.log.info(
            "Skipping webhook notification for mitx deployment (course_id=%s)",
            course_id,
        )
        return None

    # Build the content path from the course_xml UPath
    # Path format: {source}/openedx/raw_data/course_xml/{course_id}/{hash}.xml.tar.gz
    content_path = str(course_xml).split("://", 1)[-1]  # Remove s3:// prefix if present
    # Extract just the relative path portion
    if "/" in content_path:
        # Path is like: bucket/prefix/openedx/raw_data/course_xml/...
        # We want: {source}/openedx/raw_data/course_xml/...
        parts = content_path.split("/")
        try:
            openedx_idx = parts.index(source)
            content_path = "/".join(parts[openedx_idx:])
        except ValueError:
            # If 'openedx' not found, use the full path
            pass

    data = {
        "course_id": course_id,
        "course_readable_id": course_id,
        "content_path": content_path,
        "source": source,
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
                "source": source,
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
