import datetime
import hashlib
import io
import json
from datetime import UTC, datetime
from typing import Any, Literal, Optional

from flatten_dict import flatten
from flatten_dict.reducers import make_reducer
from pydantic import BaseModel


def write_course_structures(
    course_id: str,
    course_structure: dict[str, Any],
    structures_file: io.TextIOWrapper,
    blocks_file: io.TextIOWrapper,
    flattened_dict_separator: str = "__",
) -> None:
    table_row = {
        "content_hash": hashlib.sha256(
            json.dumps(course_structure).encode("utf-8")
        ).hexdigest(),
        "course_id": course_id,
        "course_structure": course_structure,
        "course_structure_flattened": flatten(
            course_structure,
            reducer=make_reducer(flattened_dict_separator),
        ),
        "retrieved_at": datetime.now(tz=UTC).isoformat(),
    }
    structures_file.write(json.dumps(table_row))
    structures_file.write("\n")
    for block in un_nest_course_structure(course_id, course_structure):
        blocks_file.write(json.dumps(block))
        blocks_file.write("\n")


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
    course_id: str, course_structure: dict[str, Any], retrieval_time: Optional[str]
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
    retrieved_at = retrieval_time or datetime.datetime.now(tz=datetime.UTC).isoformat()
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


class EdxAssetKey(BaseModel):
    source_system: str
    course_id: str
    asset_type: Literal["course_xml", "course_structure", "forum_mongo", "db_table"]
    table_object: Optional[str]


edxorg_table_objects = [
    "assessment_assessment",
    "assessment_assessmentfeedback",
    "assessment_assessmentfeedback_assessments",
    "assessment_assessmentfeedback_options",
    "assessment_assessmentfeedbackoption",
    "assessment_assessmentpart",
    "assessment_criterion",
    "assessment_criterionoption",
    "assessment_peerworkflow",
    "assessment_peerworkflowitem",
    "assessment_rubric",
    "assessment_studenttrainingworkflow",
    "assessment_studenttrainingworkflowitem",
    "assessment_trainingexample",
    "assessment_trainingexample_options_selected",
    "auth_user",
    "auth_userprofile",
    "certificates_generatedcertificate",
    "course",
    "course_groups_cohortmembership",
    "course_structure",
    "courseware_studentmodule",
    "credit_crediteligibility",
    "django_comment_client_role_users",
    "examples",
    "grades_persistentcoursegrade",
    "grades_persistentsubsectiongrade",
    "student_anonymoususerid",
    "student_courseaccessrole",
    "student_courseenrollment",
    "student_languageproficiency",
    "submissions_score",
    "submissions_scoresummary",
    "submissions_studentitem",
    "submissions_submission",
    "teams",
    "teams_membership",
    "user_api_usercoursetag",
    "user_id_map",
    "validate",
    "wiki_article",
    "wiki_articlerevision",
    "workflow_assessmentworkflow",
    "workflow_assessmentworkflowstep",
]
