"""Tests for edxorg.lib.edxorg parsing utilities."""

import pytest
from edxorg.lib.edxorg import categorize_archive_element, parse_archive_path


class TestCategorizeArchiveElement:
    def test_sql_is_db_table(self):
        result = categorize_archive_element("MITx-15.415x-3T2018-auth_user-prod.sql")
        assert result == "db_table"

    def test_json_is_course_structure(self):
        result = categorize_archive_element("MITx-15.415x-3T2018-course-prod.json")
        assert result == "course_structure"

    def test_xml_tar_gz_is_course_xml(self):
        result = categorize_archive_element("MITx-15.415x-3T2018-course.xml.tar.gz")
        assert result == "course_xml"

    def test_mongo_is_forum_mongo(self):
        assert categorize_archive_element("MITx-15.415x-3T2018.mongo") == "forum_mongo"

    def test_unknown_extension_is_unhandled(self):
        assert categorize_archive_element("something.csv") == "unhandled"


class TestParseArchivePath:
    """parse_archive_path must extract a canonical course_id regardless of file type.

    Regression coverage for the bug introduced in commit 2c2b9c3a where the
    -course / -course_structure suffix from JSON filenames was absorbed into the
    course_id, creating invalid partition keys such as
    MITx-15.415x-3T2018-course|edge instead of MITx-15.415x-3T2018|edge.
    """

    def _parse(self, filename: str) -> dict[str, str]:
        """Wrap parse_archive_path to simulate a file inside a nested path."""
        return parse_archive_path(f"some/archive/path/{filename}")

    # ------------------------------------------------------------------
    # JSON (course_structure) files
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        ("filename", "expected_source"),
        [
            ("MITx-15.415x-3T2018-course-prod.json", "prod"),
            ("MITx-15.415x-3T2018-course-edge.json", "edge"),
            ("MITx-15.415x-3T2018-course_structure-prod.json", "prod"),
            ("MITx-15.415x-3T2018-course_structure-edge.json", "edge"),
        ],
    )
    def test_json_course_id_no_suffix(self, filename: str, expected_source: str):
        """JSON filenames must yield the bare course_id without -course* suffix."""
        result = self._parse(filename)
        assert result["course_id"] == "MITx-15.415x-3T2018"
        assert result["data_category"] == "course_structure"
        assert expected_source in result["source_system"]

    @pytest.mark.parametrize(
        "filename",
        [
            "MITx-15.415x-3T2018-course-prod.json",
            "MITx-15.415x-3T2018-course_structure-prod.json",
        ],
    )
    def test_json_data_category_is_course_structure(self, filename: str):
        """Non-db_table files must have data_category == 'course_structure'.

        The caller (process_edxorg_archive_bundle) guards output_key with
        data_category == 'db_table', so verifying the category is the critical
        check. The table_name field may be populated as a side-effect of
        DATA_ATTRIBUTE_REGEX consuming the suffix, which is intentional and
        harmless because the caller ignores it for non-db_table files.
        """
        result = self._parse(filename)
        assert result["data_category"] == "course_structure"

    # ------------------------------------------------------------------
    # SQL (db_table) files - table_name IS meaningful here
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        ("filename", "expected_table"),
        [
            ("MITx-15.415x-3T2018-auth_user-prod.sql", "auth_user"),
            ("MITx-15.415x-3T2018-course-prod.sql", "course"),
            ("MITx-15.415x-3T2018-course_structure-prod.sql", "course_structure"),
            (
                "MITx-15.415x-3T2018-student_courseenrollment-edge.sql",
                "student_courseenrollment",
            ),
        ],
    )
    def test_sql_course_id_and_table_name(self, filename: str, expected_table: str):
        """SQL files must have correct course_id and table_name."""
        result = self._parse(filename)
        assert result["course_id"] == "MITx-15.415x-3T2018"
        assert result["data_category"] == "db_table"
        assert result["table_name"] == expected_table

    # ------------------------------------------------------------------
    # Mongo (forum_mongo) files
    # ------------------------------------------------------------------
    def test_mongo_course_id(self):
        result = self._parse("MITx-15.415x-3T2018.mongo")
        assert result["course_id"] == "MITx-15.415x-3T2018"
        assert result["data_category"] == "forum_mongo"

    # ------------------------------------------------------------------
    # XML (course_xml) files
    # ------------------------------------------------------------------
    @pytest.mark.parametrize(
        "filename",
        [
            "MITx-15.415x-3T2018-course.xml.tar.gz",
            "MITx-15.415x-3T2018.xml.tar.gz",
        ],
    )
    def test_xml_course_id_no_suffix(self, filename: str):
        """XML archives with or without -course suffix must yield the bare course_id."""
        result = self._parse(filename)
        assert result["course_id"] == "MITx-15.415x-3T2018"
        assert result["data_category"] == "course_xml"

    # ------------------------------------------------------------------
    # General: component decomposition
    # ------------------------------------------------------------------
    def test_components_extracted(self):
        result = self._parse("MITx-15.415x-3T2018-course-edge.json")
        assert result["organization"] == "MITx"
        assert result["course_number"] == "15.415x"
        assert result["course_run"] == "3T2018"
        assert "edge" in result["source_system"]

    # ------------------------------------------------------------------
    # Invalid / unrecognised paths return empty dict
    # ------------------------------------------------------------------
    def test_invalid_path_returns_empty(self):
        assert self._parse("not_a_real_course_file.txt") == {}
