import re
from typing import Annotated, Literal, Optional

from pydantic import BaseModel, StringConstraints

COURSE_ID_REGEX = re.compile(
    r"^(?P<organization>[a-zA-Z0-9]+)-(?P<course_number>[a-zA-Z0-9._]+)-(?P<course_run>[a-zA-Z0-9_]+)(?>-ccx-)?(?P<ccx_id>\d+)?"
)
FILE_TYPE_REGEX = re.compile(r"\.(mongo|json|xml\.tar\.gz|sql)")
DATA_ATTRIBUTE_REGEX = re.compile(r"(?>\.sql)(?P<table_name>[a-zA-Z_]+)-")

# source_system, archive_date, course_id, data_category, data_content
# Categories:
# - Forum data
# - Course export
# - Course structure
# - DB Tables:
#   - variable based on course attributes


def categorize_archive_element(archive_element: str):
    """Determine the data asset category based on the file name."""
    {
        "mongo": "forum",
        "json": "course-structure",
        "xml.tar.gz": "course-export",
        "sql": "db-table",
    }


def parse_archive_path(archive_path: str):
    """Categorize contents of an edx data archive based on file name.

    Files ending in .sql are actually tab-separated-value files representing tabular
    data from the edX platform MySQL database.

    Files ending in .json are unstructured data objects, primarily representing course
    structures.

    Files ending in .xml.tar.gz are XML exports of course contents.  These need to be
    processed to extract information not available elsewhere.


    """
    file_components = archive_path.split("/")[-1].split("-")
    if len(file_components) < 4:
        raise ValueError(f"Invalid archive path: {archive_path}")
    course_id = "-".join(file_components[:3])
    extension: Literal["json", "mongo", "xml.tar.gz", "sql"] = file_components[
        -1
    ].split(".", 1)[-1]
    if extension == "mongo":
        data_object = "mongo"
    else:
        data_object = file_components[3]
    system_of_origin = "-".join(file_components[4:-1])

    return extension, course_id, data_object, system_of_origin


# Course ID (may have CCX component)
# ORA just means extra DB tables, turn all data attributes into elements of the data asset
# segment prod and edge courses into separate bucket prefixes and table prefixes


class EdxorgCourseDataSnapshotFile(BaseModel):
    element_type: Literal["forum", "course-structure", "course-export", "tabular"]
    table_name: Optional[str]

    def process_element(self):
        ...


class EdxorgCourseDataSnapshot(BaseModel):
    course_id: Annotated[str, StringConstraints(pattern=COURSE_ID_REGEX)]
    data_snapshot_elements: list[EdxorgCourseDataSnapshotFile]
    snapshot_date: str
    snapshot_source: Literal["edge", "production"]
