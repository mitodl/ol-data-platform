import datetime
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
    for block_id, block_contents in course_structure.items():
        if block_contents["category"] == "course":
            course_title = block_contents["metadata"].get("display_name")
            course_start = block_contents["metadata"].get("start")
        for child in block_contents["children"]:
            ancestry[child] = block_id
    for block_id, block_contents in course_structure.items():
        course_blocks.append(
            {
                "course_id": course_id,
                "block_id": block_id,
                "block_details": block_contents,
                "block_parent": ancestry.get(block_id),
                "block_title": block_contents["metadata"].get("display_name"),
                "block_type": block_contents["category"],
                "block_start": block_contents["metadata"].get("start"),
                "block_due": block_contents["metadata"].get("due"),
                "retrieved_at": retrieved_at,
                "course_title": course_title,
                "course_start": course_start,
            }
        )
    return course_blocks
