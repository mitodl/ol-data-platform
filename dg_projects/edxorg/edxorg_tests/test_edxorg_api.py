"""Tests for flattening edX discovery API programs into metadata rows."""

import json
from typing import Any

from edxorg.assets.edxorg_api import program_course_records, program_record

RUN = {
    "key": "course-v1:MITx+6.002x+1T2026",
    "start": "2026-01-10T00:00:00Z",
    "end": "2026-05-01T00:00:00Z",
    "status": "published",
    "is_enrollable": True,
    "pacing_type": "instructor_paced",
    "seats": [{"type": "verified", "price": "99.00", "currency": "USD"}],
}


def program(**overrides: Any) -> dict[str, Any]:
    """Build a trimmed discovery API program, field names as the API returns them."""
    return {
        "uuid": "927093e3-46ba-4f44-a861-0f8c7aec4f74",
        "title": "Circuits and Electronics",
        "subtitle": "Learn the fundamentals.",
        "type": "XSeries",
        "status": "active",
        "authoring_organizations": [{"key": "MITx"}, {"key": "MITx_PRO"}],
        "data_modified_timestamp": "2026-01-01T00:00:00Z",
        "marketing_url": "https://www.edx.org/xseries/mitx-circuits",
        "banner_image": {
            "medium": {"url": "https://cdn.example.com/banner.medium.png"}
        },
        "level_type_override": "Intermediate",
        "courses": [
            {
                "key": "MITx+6.002.1x",
                "title": "Circuits 1",
                "short_description": "Part 1",
                "course_type": "verified-audit",
                "excluded_from_search": False,
                "course_runs": [RUN],
            }
        ],
        **overrides,
    }


def test_program_record_keeps_learn_fields() -> None:
    """The fields MIT Learn builds program records from survive flattening."""
    record = program_record(program(), retrieved_at="2026-09-19T00:00:00+00:00")
    assert record["authoring_organizations"] == "MITx, MITx_PRO"
    assert record["marketing_url"] == "https://www.edx.org/xseries/mitx-circuits"
    assert record["banner_image_url"] == "https://cdn.example.com/banner.medium.png"
    assert record["level_type_override"] == "Intermediate"
    assert record["retrieved_at"] == "2026-09-19T00:00:00+00:00"


def test_program_record_tolerates_missing_banner_and_level() -> None:
    """A program without a banner image or level override yields nulls."""
    record = program_record(
        program(banner_image=None, level_type_override=None), retrieved_at="t"
    )
    assert record["banner_image_url"] is None
    assert record["level_type_override"] is None


def test_program_course_records_keep_runs_as_json() -> None:
    """Each course keeps its search exclusion and its runs, whole, as JSON."""
    (record,) = program_course_records(program(), retrieved_at="t")
    assert record["program_uuid"] == "927093e3-46ba-4f44-a861-0f8c7aec4f74"
    assert record["course_key"] == "MITx+6.002.1x"
    assert record["course_position"] == 1
    assert record["excluded_from_search"] is False
    assert json.loads(record["course_runs"]) == [RUN]
    assert record["retrieved_at"] == "t"
