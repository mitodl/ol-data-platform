import re
from collections.abc import Sequence

COURSE_ID_REGEX = r"^(?P<course_id>(?P<organization>[a-zA-Z0-9._]+)-(?P<course_number>[a-zA-Z0-9._-]+?)-(?P<course_run>[a-zA-Z0-9._]+)(?>-ccx-)?(?P<ccx_id>\d+)?)"  # noqa: E501
FILE_TYPE_REGEX = r"\.(?P<extension>mongo|json|xml\.tar\.gz|sql)$"
DATA_ATTRIBUTE_REGEX = r"(-(?P<table_name>[a-zA-Z_]+)-)?"
SYSTEM_OF_ORIGIN_REGEX = r"-?(?P<source_system>((prod)?-?(edge)?))(-analytics)?"

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
    extension_match = re.search(FILE_TYPE_REGEX, archive_element)
    extension = ""
    if extension_match:
        extension = extension_match.group(1)
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
    pattern_pieces = [COURSE_ID_REGEX]
    if categorize_archive_element(archive_path) != "forum_mongo":
        pattern_pieces.append(DATA_ATTRIBUTE_REGEX)
    pattern_pieces.extend([SYSTEM_OF_ORIGIN_REGEX, FILE_TYPE_REGEX])
    regexes = "".join(pattern_pieces)
    components = re.fullmatch(
        regexes,
        archive_path.split("/")[-1],
    )
    if components:
        asset_info = components.groupdict()
        asset_info["data_category"] = categorize_archive_element(archive_path)
        return asset_info
    else:
        return {}


def build_mapping_key(
    asset_info: dict[str, str],
    is_asset_key: bool = False,  # noqa: FBT001, FBT002
) -> Sequence[str]:
    """Generate the hierarchical mapping of the asset.

    This is to be used either for generating the asset key,orthe mapping while using an
    op-based approach.
    """
    key_sequence = ["edxorg", "raw_data"]
    data_category = asset_info["data_category"]
    key_sequence.append(data_category)
    if data_category == "db_table":
        key_sequence.append(asset_info["table_name"])
    if is_asset_key:
        return key_sequence
    normalized_source_system = (
        "edge" if "edge" in asset_info["source_system"] else "prod"
    )
    key_sequence.append(normalized_source_system)
    key_sequence.append(asset_info["course_id"])
    return key_sequence
