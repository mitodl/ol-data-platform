import datetime
import hashlib
import json
from typing import Any


def un_nest_course_structure(
    course_id: str, course_structure: dict[str, Any]
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
    course_title = None
    course_start = None
    retrieved_at = datetime.datetime.now(tz=datetime.UTC).isoformat()
    course_blocks = []
    # We need to loop through the course structure twice. Once to populate the full
    # ancestry and a second time to build the record structure so that we can pull the
    # block parents.
    content_hash = hashlib.sha256(
        json.dumps(course_structure).encode("utf-8")
    ).hexdigest()
    for block_id, block_contents in course_structure.items():
        if block_contents["category"] == "course":
            course_title = block_contents["metadata"].get("display_name")
            course_start = block_contents["metadata"].get("start")
        for child in block_contents["children"]:
            ancestry[child] = block_id
    for block_id, block_contents in course_structure.items():
        course_blocks.append(
            {
                "block_content_hash": hashlib.sha256(
                    json.dumps(block_contents).encode("utf-8")
                ).hexdigest(),
                "block_details": block_contents,
                "block_due": block_contents["metadata"].get("due"),
                "block_id": block_id,
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
