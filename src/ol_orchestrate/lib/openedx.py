import hashlib
import json
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import IO, Any
from xml.etree.ElementTree import ElementTree


def generate_block_indexes(
    course_structure: dict[str, Any], root_block_id: str
) -> dict[str, int]:
    """Walk the course structure to generate an index for the blocks.

    Traversal is done in a depth first manner to signify the sequenced progression
    through the course that learners experience.
    """
    block_index: dict[str, int] = {}
    block_stack = [root_block_id]
    previous_block_id = root_block_id
    while block_stack:
        block_id = block_stack.pop()
        block_index[block_id] = block_index.get(previous_block_id, 0) + 1
        previous_block_id = block_id
        # Add new blocks to the end of the list, ensure that the traversal is done in
        # order of definition by reversing, since we're pulling from the end of the list
        # for each iteration.
        block_stack.extend(reversed(course_structure[block_id]["children"]))
    return block_index


def un_nest_course_structure(
    course_id: str, course_structure: dict[str, Any], retrieval_time: str | None
) -> list[dict[str, Any]]:
    """
    Recursively unnest the course structure

    :param course_structure: The course structure to unnest

    :return: The unnested course structure
    """
    # Block per row
    # Include children and parent
    # Include full hierarchy as block IDs and block names
    ancestry = {}
    root_block = ""
    course_title = None
    course_start = None
    retrieved_at = retrieval_time or datetime.now(tz=UTC).isoformat()
    course_blocks = []
    content_hash = hashlib.sha256(
        json.dumps(course_structure).encode("utf-8")
    ).hexdigest()
    # We need to loop through the course structure first to populate the full
    # ancestry and again to build the record structure so that we can pull the
    # block parents.
    for block_id, block_contents in course_structure.items():
        if block_contents["category"] == "course":
            course_title = block_contents["metadata"].get("display_name")
            course_start = block_contents["metadata"].get("start")
            root_block = block_id
        for child in block_contents["children"]:
            ancestry[child] = block_id

    block_index = generate_block_indexes(course_structure, root_block)
    # Loop through the course structure again to build the record structure
    for block_id, block_contents in course_structure.items():
        course_blocks.append(
            {
                "block_content_hash": hashlib.sha256(
                    json.dumps(block_contents).encode("utf-8")
                ).hexdigest(),
                "block_details": block_contents,
                "block_due": block_contents["metadata"].get("due"),
                "block_id": block_id,
                "block_index": block_index.get(block_id, 0),
                "block_parent": ancestry.get(block_id),
                "block_start": block_contents["metadata"].get("start"),
                "block_title": block_contents["metadata"].get("display_name"),
                "block_type": block_contents["category"],
                "course_content_hash": content_hash,
                "course_id": course_id,
                "course_start": course_start,
                "course_title": course_title,
                "retrieved_at": retrieved_at,
            }
        )
    return course_blocks


def process_video_xml(archive_path: Path) -> list[dict[str, str]]:
    video_block_details = []
    with tarfile.open(archive_path, "r") as tf:
        # get course info from the course xml file in the root directory
        archive_root = tf.next()
        if archive_root is None:
            msg = "Unable to retrieve the archive root of the course XML."
            raise ValueError(msg)
        tar_info_course = tf.getmember(f"{archive_root.name}/course.xml")
        course_xml_file = Path(
            NamedTemporaryFile(delete=False, suffix="_course.xml").name
        )
        course_xml_file.write_bytes(tf.extractfile(tar_info_course).read())  # type: ignore[union-attr]
        course_id, course_number, run_tag, org = parse_course_id(str(course_xml_file))
        course_xml_file.unlink()
        for member in tf.getmembers():
            if not member.isdir() and member.path.startswith(
                f"{archive_root.name}/video/"
            ):
                xml_data = tf.extractfile(member)
                if not xml_data:
                    continue
                video_data = parse_video_xml(xml_data)
                video_data["course_id"] = course_id
                video_block_details.append(video_data)
    return video_block_details


def process_policy_json(
    archive_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    signatory_rows = []
    course_policy_rows = []
    with tarfile.open(archive_path, "r") as tf:
        archive_root = tf.next()
        if archive_root is None:
            msg = "Unable to retrieve the archive root of the course XML."
            raise Exception(msg)  # noqa: TRY002
        tar_info_course = tf.getmember(f"{archive_root.name}/course.xml")
        course_xml_file = Path(
            NamedTemporaryFile(delete=False, suffix="_course.xml").name
        )
        course_xml_file.write_bytes(tf.extractfile(tar_info_course).read())  # type: ignore[union-attr]
        course_id, course_number, run_tag, org = parse_course_id(str(course_xml_file))
        course_xml_file.unlink()
        for member in tf.getmembers():
            if not member.isdir() and member.path.startswith(
                f"{archive_root.name}/policies/{run_tag}/"
            ):
                json_data = tf.extractfile(member)
                if not json_data:
                    continue

                policy_data = json.load(json_data)

                for course_id, course_info in policy_data.items():
                    if course_id.startswith("course/"):
                        course_policy_row = {
                            "course_id": course_id,
                            "advanced_modules": course_info.get("advanced_modules"),
                            "discussions_settings": course_info.get(
                                "discussions_settings"
                            ),
                            "display_coursenumber": course_info.get(
                                "display_coursenumber"
                            ),
                            "self_paced": course_info.get("self_paced"),
                            "tabs": course_info.get("tabs"),
                            "tags": course_info.get("tags"),
                        }
                        course_policy_rows.append(course_policy_row)

                        certificates = course_info.get("certificates", {}).get(
                            "certificates", []
                        )
                        for cert in certificates:
                            cert_id = cert.get("id")
                            signatories = cert.get("signatories", [])
                            for signatory in signatories:
                                signatory_row = signatory.copy()
                                signatory_row["course_id"] = course_id
                                signatory_row["certificate_id"] = cert_id
                                signatory_rows.append(signatory_row)
                        break  # Only process the first element

    return course_policy_rows, signatory_rows


def parse_course_id(course_xml: str | Path) -> tuple[str, str, str, str]:
    """
    Parse the attributes of the course.xml file in the root directory
    and generate a properly formatted course_id string.

    :param course_xml: The file path to course.xml file

    :return: A list containing the formatted course_id string, course_number,
     the course run_tag, and the org of the course
    """
    with Path(course_xml).open("r") as course:
        tree = ElementTree()
        tree.parse(course)
        course_root = tree.getroot()
        if course_root is None:
            msg = (
                "Unable to locate the root of the XML file. "
                "Please verify that it is properly constructed and not malformed."
            )
            raise ValueError(msg)
        run_tag = str(course_root.attrib.get("url_name", None))
        org = str(course_root.attrib.get("org", None))
        course_number = str(course_root.attrib.get("course", None))
    return f"course-v1:{org}+{course_number}+{run_tag}", course_number, run_tag, org


def parse_video_xml(video_file: IO[bytes]) -> dict[str, Any]:
    tree = ElementTree()
    tree.parse(video_file)
    video_root = tree.getroot()
    if video_root is None:
        msg = (
            "Unable to locate the root of the XML file. "
            "Please verify that it is properly constructed and not malformed."
        )
        raise ValueError(msg)
    video_block_id = video_root.attrib.get("url_name", None)
    edx_video_id = video_root.attrib.get("edx_video_id", None)
    video_asset = video_root.find("video_asset", None)
    duration = video_asset.attrib.get("duration", None) if video_asset else "0.0"

    return {
        "video_block_id": video_block_id,
        "edx_video_id": edx_video_id,
        "duration": duration,
    }


def write_video_json_file(video_data: dict[str, Any], video_data_file: Path):
    with Path(video_data_file).open("w") as video_file:
        video_file.write(json.dumps(video_data))
        video_file.write("\n")


def process_course_xml(archive_path: Path) -> dict[str, Any]:
    """
    Pull course metadata out of course xml files for edx.org courses
    (edx.org dumps). Process the course bundle by
    getting the course_id, finding the metadata file path, calling the
    function to parse the XML, and then return a dictionary with the metadata elements.

    :param archive_path: The path to the tar archive for the course bundle

    :return: A dictionary with the parsed course metadata.
    :rtype: Dict[str, Any]
    """
    with tarfile.open(archive_path, "r") as tf:
        # get course info from the course xml file in the root directory
        archive_root = tf.next()
        if archive_root is None:
            msg = "Unable to retrieve the archive root of the course XML."
            raise ValueError(msg)
        tar_info_course = tf.getmember(f"{archive_root.name}/course.xml")
        course_xml_file = Path(
            NamedTemporaryFile(delete=False, suffix="_course.xml").name
        )
        course_xml_file.write_bytes(tf.extractfile(tar_info_course).read())  # type: ignore[union-attr]
        course_id, course_number, run_tag, org = parse_course_id(str(course_xml_file))
        # use the run_tag to find the course metadata file
        tar_info_metadata = tf.getmember(f"{archive_root.name}/course/{run_tag}.xml")
        course_metadata_file = Path(
            NamedTemporaryFile(delete=False, suffix="_metadata.xml").name
        )
        course_metadata_file.write_bytes(tf.extractfile(tar_info_metadata).read())  # type: ignore[union-attr]
        course_metadata = parse_course_xml(str(course_metadata_file))
        # add courserun data from the parse_course_id output
        course_metadata["course_id"] = course_id
        course_metadata["course_number"] = course_number
        # courserun_semester is run_tag
        course_metadata["semester"] = run_tag
        course_metadata["institution"] = org
        course_xml_file.unlink()
        course_metadata_file.unlink()
    return course_metadata


def parse_course_xml(metadata_file: str) -> dict[str, Any]:
    """
    Parse the attributes of the metadata XML file in the 'course/course' directory and
    generate a dictionary with all the course metadata attributes.

    :param metadata_file: The file path to metadata XML file. References the run_tag
    from the parse_course_id function ('course/course/{run_tag}.xml').

    :return: A dictionary with all of the course metadata attributes
    :rtype: dict[str, Any]
    """
    with Path(metadata_file).open("r") as metadata:
        tree = ElementTree()
        tree.parse(metadata)
        metadata_root = tree.getroot()
    if not metadata_root:
        msg = (
            "Unable to locate the root of the XML file. "
            "Please verify that it is properly constructed and not malformed."
        )
        raise ValueError(msg)
    enrollment_start = metadata_root.attrib.get("enrollment_start")
    enrollment_end = metadata_root.attrib.get("enrollment_end")
    # Default value as defined in edx-platform code
    # https://github.com/openedx/edx-platform/blob/master/xmodule/course_metadata_utils.py#L17
    start = metadata_root.attrib.get("start")
    end = metadata_root.attrib.get("end")
    # default to empty list
    instructor_info = metadata_root.attrib.get("instructor_info")
    self_paced = bool(metadata_root.attrib.get("self_paced", False))
    title = metadata_root.attrib.get("display_name")
    return {
        "enrollment_start": enrollment_start,
        "enrollment_end": enrollment_end,
        "course_start": start,
        "course_end": end,
        "instructor_info": instructor_info,
        "self_paced": self_paced,
        "title": title,
    }
