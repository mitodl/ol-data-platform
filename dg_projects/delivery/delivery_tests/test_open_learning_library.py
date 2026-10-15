"""Tests for shaping OLL integration rows into MIT Learn's webhook payload."""

from typing import Any

from delivery.assets.open_learning_library import build_resources


def course_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__oll_courses row."""
    return {
        "readable_id": "18.05+summer_2022",
        "run_id": "MITx+18.05r_10+2022_Summer",
        "course_number": "18.05",
        "title": "Introduction to Probability and Statistics",
        "url": "https://openlearninglibrary.mit.edu/courses/course-v1:MITx+18.05r_10+2022_Summer/about",
        "description": "About",
        "image_url": "https://example.com/18.05.jpg",
        "offered_by": "ocw",
        "published": True,
        "level": "undergraduate",
        "topics": ["Mathematics", "Data Science"],
        "instructors": ["Jeremy Orloff", "Jennifer French Kamrin"],
        "year": 2022,
        "semester": "Summer",
        "duration": "6 weeks",
        "min_weeks": 6,
        "max_weeks": 6,
        "time_commitment": "4 hours/week",
        "min_weekly_hours": 4,
        "max_weekly_hours": 4,
        "last_modified": "2026-10-15T00:00:00.000Z",
        "etl_source": "oll",
        "platform": "oll",
        "resource_type": "course",
        **overrides,
    }


def test_course_carries_the_rows_offeror_and_one_run() -> None:
    """OCW-offered rows keep their offeror; the run is the sheet's run key."""
    [resource] = build_resources([course_row()])
    assert resource["readable_id"] == "18.05+summer_2022"
    assert resource["offered_by"] == {"code": "ocw"}
    assert resource["etl_source"] == "oll"
    assert resource["platform"] == "oll"
    assert resource["resource_type"] == "course"
    assert resource["full_description"] == "About"
    assert resource["image"] == {
        "url": "https://example.com/18.05.jpg",
        "alt": "Introduction to Probability and Statistics",
    }
    assert resource["topics"] == [{"name": "Mathematics"}, {"name": "Data Science"}]
    assert resource["course"] == {
        "course_numbers": [
            {
                "value": "18.05",
                "listing_type": "primary",
                "department": None,
                "sort_coursenum": "18.05",
                "primary": True,
            }
        ]
    }
    [run] = resource["runs"]
    assert run["run_id"] == "MITx+18.05r_10+2022_Summer"
    assert run["level"] == ["undergraduate"]
    assert run["instructors"] == [
        {"full_name": "Jeremy Orloff"},
        {"full_name": "Jennifer French Kamrin"},
    ]
    assert run["status"] == "Archived"
    assert (run["year"], run["semester"]) == (2022, "Summer")


def test_missing_values_become_the_legacy_empty_forms() -> None:
    """The legacy ETL sends [] and "" where the sheet is empty, not None."""
    [resource] = build_resources(
        [
            course_row(
                level=None,
                topics=None,
                instructors=None,
                duration=None,
                min_weeks=None,
                max_weeks=None,
                time_commitment=None,
                min_weekly_hours=None,
                max_weekly_hours=None,
            )
        ]
    )
    assert resource["topics"] == []
    [run] = resource["runs"]
    assert run["level"] == []
    assert run["instructors"] == []
    assert (run["duration"], run["time_commitment"]) == ("", "")
    assert run["min_weekly_hours"] is None


def test_published_follows_the_row() -> None:
    """An unpublished sheet row is delivered unpublished, not left out."""
    [resource] = build_resources([course_row(published=False)])
    assert resource["published"] is False
    assert resource["runs"][0]["published"] is False
