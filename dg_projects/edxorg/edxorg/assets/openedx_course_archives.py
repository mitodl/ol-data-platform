import hashlib
import json
from pathlib import Path
from tempfile import NamedTemporaryFile

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
from dagster._core.definitions.partitions.utils.multi import (
    MULTIPARTITION_KEY_DELIMITER,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.openedx import (
    process_course_xml,
    process_policy_json,
    process_video_xml,
)
from ol_orchestrate.partitions.edxorg import course_and_source_partitions
from upath import UPath


@asset(
    key=AssetKey(("edxorg", "raw_data", "course_xml")),
    partitions_def=course_and_source_partitions,
    group_name="edxorg",
)
def dummy_edxorg_course_xml(): ...


@multi_asset(
    partitions_def=course_and_source_partitions,
    group_name="edxorg",
    ins={"course_archive": AssetIn(key=AssetKey(("edxorg", "raw_data", "course_xml")))},
    outs={
        "course_metadata": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Metadata about the course run that is extracted form the XML export."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_metadata")),
        ),
        "course_video": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Details about the video elements in the courses that are extracted "
                "from the XML export."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_video")),
        ),
        "course_certificate_signatory": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Details about the course certificate signatories in the course"
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_certificate_signatory")),
        ),
        "course_policy": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=("Details about the course policy in the course"),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_policy")),
        ),
    },
)
def extract_edxorg_courserun_metadata(
    context: AssetExecutionContext, course_archive: UPath
):
    # Download the remote file to the current working directory
    course_xml_path = Path(NamedTemporaryFile(delete=False, suffix=".xml.tar.gz").name)
    course_archive.fs.get_file(course_archive, course_xml_path)
    data_version = hashlib.file_digest(course_xml_path.open("rb"), "sha256").hexdigest()

    # Process the course metadata
    course_metadata = process_course_xml(course_xml_path)
    course_metadata_file = Path(
        NamedTemporaryFile(delete=False, suffix="_metadata.json").name
    )
    course_metadata_file.write_text(json.dumps(course_metadata))

    # Multipartition Key Logic
    partition_parts = context.partition_key.split(MULTIPARTITION_KEY_DELIMITER)
    partition_dict = {}
    for partition_part in partition_parts:
        if partition_part in ("prod", "edge"):
            partition_dict["source_system"] = partition_part
        else:
            partition_dict["course_id"] = partition_part
    course_metadata_object_key = f"{'/'.join(context.asset_key_for_output('course_metadata').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_metadata_file, course_metadata_object_key),
        output_name="course_metadata",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": partition_dict["course_id"],
            "object_key": course_metadata_object_key,
        },
    )

    # Process the course video details
    video_details = process_video_xml(course_xml_path)
    course_video_file = Path(
        NamedTemporaryFile(delete=False, suffix="_video.jsonl").name
    )
    jsonlines.open(course_video_file, "w").write_all(video_details)
    course_video_object_key = f"{'/'.join(context.asset_key_for_output('course_video').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_video_file, course_video_object_key),
        output_name="course_video",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": partition_dict["course_id"],
            "object_key": course_video_object_key,
        },
    )

    course_policy_rows, signatory_rows = process_policy_json(course_xml_path)
    course_policy_file = Path(
        NamedTemporaryFile(delete=False, suffix="_policy.jsonl").name
    )
    jsonlines.open(course_policy_file, "w").write_all(course_policy_rows)
    course_policy_object_key = f"{'/'.join(context.asset_key_for_output('course_policy').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_policy_file, course_policy_object_key),
        output_name="course_policy",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": partition_dict["course_id"],
            "object_key": course_policy_object_key,
        },
    )

    course_certificate_signatory_file = Path(
        NamedTemporaryFile(delete=False, suffix="_certificate_signatory.jsonl").name
    )
    jsonlines.open(course_certificate_signatory_file, "w").write_all(signatory_rows)
    course_certificate_signatory_object_key = f"{'/'.join(context.asset_key_for_output('course_certificate_signatory').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_certificate_signatory_file, course_certificate_signatory_object_key),
        output_name="course_certificate_signatory",
        data_version=DataVersion(data_version),
        metadata={
            "course_id": partition_dict["course_id"],
            "object_key": course_certificate_signatory_object_key,
        },
    )
    course_xml_path.unlink()


@asset(
    code_version="edxorg_course_export_webhook_v1",
    key=AssetKey(["edxorg", "course_content_webhook"]),
    group_name="course_content_metadata",
    description="Notify Learn API via webhook after edx.org course XML export.",
    automation_condition=upstream_or_code_changes(),
    partitions_def=course_and_source_partitions,
    ins={
        "course_archive": AssetIn(key=AssetKey(["edxorg", "raw_data", "course_xml"])),
    },
    required_resource_keys={"learn_api"},
    output_required=False,
)
def edxorg_course_content_webhook(
    context: AssetExecutionContext, course_archive: UPath | None
):
    """Send webhook notification to Learn API after edx.org course XML export."""
    # Parse the multipartition key to extract course_id
    partition_parts = context.partition_key.split(MULTIPARTITION_KEY_DELIMITER)
    partition_dict = {}
    for partition_part in partition_parts:
        if partition_part in ("prod", "edge"):
            partition_dict["source_system"] = partition_part
        else:
            partition_dict["course_id"] = partition_part

    course_id = partition_dict.get("course_id", context.partition_key)

    # Skip if no course_archive was produced
    if course_archive is None:
        context.log.info(
            "No course XML available for course_id=%s, skipping webhook",
            course_id,
        )
        return

    # Build the content path from the course_archive UPath
    content_path = str(course_archive).split("://", 1)[-1]  # Remove s3:// prefix
    # Extract just the relative path portion
    if "/" in content_path:
        # Path is like: bucket/prefix/edxorg/raw_data/course_xml/...
        # We want: edxorg/raw_data/course_xml/...
        parts = content_path.split("/")
        try:
            edxorg_idx = parts.index("edxorg")
            content_path = "/".join(parts[edxorg_idx:])
        except ValueError:
            # If 'edxorg' not found, use the full path
            pass

    data = {
        "course_id": course_id,
        "course_readable_id": course_id,
        "content_path": content_path,
        "source": "mit_edx",
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
    except httpx.HTTPStatusError as error:
        error_message = (
            f"Learn API webhook notification failed for course_id={course_id} "
            f"with status code {error.response.status_code} and error: {error!s}"
        )
        context.log.exception(error_message)
        raise Exception(error_message) from error  # noqa: TRY002
    return
