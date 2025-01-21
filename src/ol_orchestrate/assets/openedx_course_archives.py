import hashlib
import json
from pathlib import Path
from tempfile import NamedTemporaryFile

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
from dagster._core.definitions.multi_dimensional_partitions import (
    MULTIPARTITION_KEY_DELIMITER,
)
from upath import UPath

from ol_orchestrate.assets.edxorg_archive import course_and_source_partitions
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.openedx import (
    process_course_xml,
    process_video_xml,
)


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
    course_xml_path.unlink()
