"""Tests for shaping MIT PE integration rows into MIT Learn's webhook payload."""

from decimal import Decimal
from typing import Any

from delivery.assets.mitpe import build_resources


def course_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__mitpe_courses row."""
    return {
        "readable_id": "course-1",
        "title": "A Course",
        "url": "https://professional.mit.edu/course-1",
        "image_url": None,
        "image_alt": None,
        "description": "<p>About</p>",
        "topics": None,
        "delivery": "in_person",
        "location": "Cambridge",
        "duration": "5 Days",
        "min_weeks": 1,
        "max_weeks": 1,
        "price": Decimal("3600.00"),
        "instructors": ["Lead Person", "Other Person"],
        "etl_source": "mitpe",
        "platform": "mitpe",
        "resource_type": "course",
        **overrides,
    }


def program_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__mitpe_programs row."""
    return course_row(
        **{
            "readable_id": "program-1",
            "title": "A Program",
            "resource_type": "program",
            "course_readable_ids": ["course-1"],
            **overrides,
        }
    )


def run_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__mitpe_runs row."""
    return {
        "readable_id": "course-1",
        "run_id": "r1",
        "run_position": 1,
        "start_date": "2026-06-05T04:00:00.000Z",
        "end_date": "2026-06-09T04:00:00.000Z",
        "enrollment_end": None,
        **overrides,
    }


def test_courses_are_delivered_before_programs() -> None:
    """MIT Learn looks program courses up, so courses must load first."""
    resources = build_resources([course_row()], [program_row()], [])
    assert [r["resource_type"] for r in resources] == ["course", "program"]


def test_runs_attach_in_feed_order_with_item_level_fields() -> None:
    """Each item's runs are ordered by position and share its price and staff."""
    resources = build_resources(
        [course_row()],
        [],
        [
            run_row(run_id="r2", run_position=2),
            run_row(run_id="r1", run_position=1),
            run_row(readable_id="other", run_id="x"),
        ],
    )
    runs = resources[0]["runs"]
    assert [run["run_id"] for run in runs] == ["r1", "r2"]
    assert runs[0]["prices"] == [{"amount": "3600.00", "currency": "USD"}]
    assert runs[0]["instructors"] == [
        {"full_name": "Lead Person"},
        {"full_name": "Other Person"},
    ]
    assert runs[0]["description"] == "<p>About</p>"
    assert runs[0]["delivery"] == ["in_person"]


def test_missing_topics_and_price_become_empty_lists() -> None:
    """MIT Learn keeps existing topics on None, so no topics must be sent as []."""
    resource = build_resources([course_row(price=None)], [], [run_row()])[0]
    assert resource["topics"] == []
    assert resource["runs"][0]["prices"] == []


def test_program_courses_are_references_and_courses_get_course_data() -> None:
    """Programs carry course references; only courses carry the course sub-dict."""
    course, program = build_resources(
        [course_row()], [program_row(course_readable_ids=None)], []
    )
    assert course["course"] == {"course_numbers": []}
    assert "courses" not in course
    assert program["courses"] == []
    assert "course" not in program

    program = build_resources([], [program_row()], [])[0]
    assert program["courses"] == [{"readable_id": "course-1", "platform": "mitpe"}]
