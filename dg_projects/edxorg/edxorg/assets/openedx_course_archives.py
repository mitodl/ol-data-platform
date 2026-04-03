import hashlib
import io
import json
import tarfile
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
    process_course_xml_blocks,
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
        "course_xml_blocks": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Comprehensive block-level data extracted from course XML archives, "
                "including chapters, sequentials, verticals, and content components"
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_xml_blocks")),
        ),
        "course_static_assets": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Non-XML files extracted from the course archive (e.g. SRT subtitles, "
                "HTML content, PDFs), bundled as a tar archive for downstream use."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_static_assets")),
        ),
        "course_static_assets_manifest": AssetOut(
            automation_condition=upstream_or_code_changes(),
            description=(
                "Manifest JSON listing every non-XML static file in the course archive "
                "(path, MIME type, size). Versioned by a content hash so downstream "
                "consumers can detect changes without fetching the full archive."
            ),
            io_manager_key="s3file_io_manager",
            key=AssetKey(("edxorg", "processed_data", "course_static_assets_manifest")),
        ),
    },
)
def extract_edxorg_courserun_metadata(  # noqa: PLR0915
    context: AssetExecutionContext, course_archive: UPath
):
    temp_files: list[Path] = []
    try:
        # Download the remote file to the current working directory
        course_xml_path = Path(
            NamedTemporaryFile(delete=False, suffix=".xml.tar.gz").name
        )
        temp_files.append(course_xml_path)
        context.log.info(
            "Attempting to download course XML from %s to %s",
            course_archive,
            course_xml_path,
        )
        course_archive.fs.get_file(str(course_archive), str(course_xml_path))
        data_version = hashlib.file_digest(
            course_xml_path.open("rb"), "sha256"
        ).hexdigest()

        # Process the course metadata
        course_metadata = process_course_xml(course_xml_path)
        course_metadata_file = Path(
            NamedTemporaryFile(delete=False, suffix="_metadata.json").name
        )
        temp_files.append(course_metadata_file)
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
        temp_files.append(course_video_file)
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
        temp_files.append(course_policy_file)
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
        temp_files.append(course_certificate_signatory_file)
        jsonlines.open(course_certificate_signatory_file, "w").write_all(signatory_rows)
        course_certificate_signatory_object_key = f"{'/'.join(context.asset_key_for_output('course_certificate_signatory').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
        yield Output(
            (
                course_certificate_signatory_file,
                course_certificate_signatory_object_key,
            ),
            output_name="course_certificate_signatory",
            data_version=DataVersion(data_version),
            metadata={
                "course_id": partition_dict["course_id"],
                "object_key": course_certificate_signatory_object_key,
            },
        )

        # Process comprehensive XML block data and collect non-XML assets
        xml_blocks, static_bundle = process_course_xml_blocks(
            course_xml_path, partition_dict["source_system"]
        )
        course_xml_blocks_file = Path(
            NamedTemporaryFile(delete=False, suffix="_xml_blocks.jsonl").name
        )
        temp_files.append(course_xml_blocks_file)
        with jsonlines.open(course_xml_blocks_file, "w") as writer:
            writer.write_all(block.model_dump() for block in xml_blocks)
        course_xml_blocks_object_key = f"{'/'.join(context.asset_key_for_output('course_xml_blocks').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
        yield Output(
            (course_xml_blocks_file, course_xml_blocks_object_key),
            output_name="course_xml_blocks",
            data_version=DataVersion(data_version),
            metadata={
                "course_id": partition_dict["course_id"],
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
        temp_files.append(static_assets_file)
        with tarfile.open(static_assets_file, "w:gz") as assets_tar:
            for relative_path, asset_bytes in static_bundle.files:
                info = tarfile.TarInfo(name=relative_path)
                info.size = len(asset_bytes)
                assets_tar.addfile(info, io.BytesIO(asset_bytes))
        course_static_assets_object_key = f"{'/'.join(context.asset_key_for_output('course_static_assets').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/{static_bundle.data_version}.tar.gz"  # noqa: E501
        yield Output(
            (static_assets_file, course_static_assets_object_key),
            output_name="course_static_assets",
            data_version=DataVersion(static_bundle.data_version),
            metadata={
                "course_id": partition_dict["course_id"],
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
        temp_files.append(static_manifest_file)
        static_manifest_file.write_text(json.dumps(static_bundle.manifest, indent=2))
        course_static_assets_manifest_object_key = f"{'/'.join(context.asset_key_for_output('course_static_assets_manifest').path)}/{partition_dict['source_system']}/{partition_dict['course_id']}/manifest.json"  # noqa: E501
        yield Output(
            (static_manifest_file, course_static_assets_manifest_object_key),
            output_name="course_static_assets_manifest",
            data_version=DataVersion(static_bundle.data_version),
            metadata={
                "course_id": partition_dict["course_id"],
                "object_key": course_static_assets_manifest_object_key,
                "asset_count": static_bundle.manifest["file_count"],
            },
        )
    finally:
        for temp_file in temp_files:
            temp_file.unlink(missing_ok=True)


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
    io_manager_key="default_io_manager",
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

    # Build the content path from the course_archive UPath
    content_path = str(course_archive).split("://", 1)[-1]  # Remove s3:// prefix
    # Extract just the relative path portion
    if "/" in content_path:
        # Path is like: bucket/prefix/edxorg/raw_data/course_xml/...
        # We want: /edxorg/raw_data/course_xml/...
        parts = content_path.split("/")
        try:
            edxorg_idx = parts.index("edxorg")
            content_path = "/".join(parts[edxorg_idx:])
        except ValueError:
            # If 'edxorg' not found, use the full path
            pass

    # Make sure path starts with a slash
    content_path = f"/{content_path.lstrip('/')}"

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
        return Output(
            value={"course_id": course_id},
            metadata={
                "status": "success",
                "course_id": course_id,
                "source": "mit_edx",
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
