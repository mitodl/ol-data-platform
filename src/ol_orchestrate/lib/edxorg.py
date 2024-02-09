import re

COURSE_ID_REGEX = re.compile(
    r"^(?P<course_id>(?P<organization>[a-zA-Z0-9._]+)-(?P<course_number>[a-zA-Z0-9._-]+?)-(?P<course_run>[a-zA-Z0-9._]+)(?>-ccx-)?(?P<ccx_id>\d+)?)"
)
FILE_TYPE_REGEX = re.compile(r"\.(?P<extension>mongo|json|xml\.tar\.gz|sql)$")
DATA_ATTRIBUTE_REGEX = re.compile(r"(-(?P<table_name>[a-zA-Z_]+)-)?")
SYSTEM_OF_ORIGIN_REGEX = re.compile(
    r"-?(?P<source_system>((prod)?-?(edge)?))(-analytics)?"
)

# source_system, archive_date, course_id, data_category, data_content
# Categories:
# - Forum data
# - Course export
# - Course structure
# - DB Tables:
#   - variable based on course attributes


def categorize_archive_element(archive_element: str) -> str:
    """Determine the data asset category based on the file name."""
    element_map = {
        "mongo": "forum_mongo",
        "json": "course_structure",
        "xml.tar.gz": "course_xml",
        "sql": "db_table",
    }
    extension = re.search(FILE_TYPE_REGEX, archive_element)
    if extension:
        extension = extension.group(1)
    return element_map.get(extension, "unhandled")


def parse_archive_path(archive_path: str) -> dict[str, str]:
    """Categorize contents of an edx data archive based on file name.

    Files ending in .sql are actually tab-separated-value files representing tabular
    data from the edX platform MySQL database.

    Files ending in .json are unstructured data objects, primarily representing course
    structures.

    Files ending in .xml.tar.gz are XML exports of course contents.  These need to be
    processed to extract information not available elsewhere.


    """
    components = re.fullmatch(
        COURSE_ID_REGEX.pattern
        + DATA_ATTRIBUTE_REGEX.pattern
        + SYSTEM_OF_ORIGIN_REGEX.pattern
        + FILE_TYPE_REGEX.pattern,
        archive_path.split("/")[-1],
    )
    if components:
        return components.groupdict()
    else:
        return {}
