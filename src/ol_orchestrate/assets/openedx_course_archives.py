import hashlib
import json
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AutoMaterializePolicy,
    DataVersion,
    Output,
    asset,
)
from dagster._core.definitions.multi_dimensional_partitions import (
    MULTIPARTITION_KEY_DELIMITER,
)
from upath import UPath

from ol_orchestrate.assets.edxorg_archive import course_and_source_partitions
from ol_orchestrate.lib.openedx import process_course_xml


@asset(
    key=AssetKey(("edxorg", "raw_data", "course_xml")),
    partitions_def=course_and_source_partitions,
    group_name="edxorg",
)
def dummy_edxorg_course_xml(): ...


@asset(
    key=AssetKey(("edxorg", "processed_data", "course_metadata")),
    partitions_def=course_and_source_partitions,
    group_name="edxorg",
    io_manager_key="s3file_io_manager",
    ins={"course_archive": AssetIn(key=AssetKey(("edxorg", "raw_data", "course_xml")))},
    auto_materialize_policy=AutoMaterializePolicy.eager(
        max_materializations_per_minute=None
    ),
)
def extract_edxorg_courserun_metadata(
    context: AssetExecutionContext, course_archive: UPath
):
    # Download the remote file to the current working directory
    course_xml = Path("course.xml.tar.gz")
    course_archive.fs.get_file(course_archive, course_xml)
    course_metadata = process_course_xml(course_xml)
    course_metadata_file = Path("course_metadata.json")
    course_metadata_file.write_text(json.dumps(course_metadata))
    data_version = hashlib.file_digest(
        course_metadata_file.open("rb"), "sha256"
    ).hexdigest()
    partition_parts = context.partition_key.split(MULTIPARTITION_KEY_DELIMITER)
    partition_dict = {}
    for partition_part in partition_parts:
        if partition_part in ("prod", "edge"):
            partition_dict["source_system"] = partition_part
        else:
            partition_dict["course_id"] = partition_part
    course_metadata_object_key = f"edxorg/processed_data/course_metadata/{partition_dict['source_system']}/{partition_dict['course_id']}/{data_version}.json"  # noqa: E501
    yield Output(
        (course_metadata_file, course_metadata_object_key),
        data_version=DataVersion(data_version),
        metadata={
            "course_id": course_metadata["course_id"],
            "object_key": course_metadata_object_key,
        },
    )
    course_xml.unlink()
