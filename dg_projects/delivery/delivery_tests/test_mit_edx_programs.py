"""Tests for the MIT edX programs payload and the no-program review issues."""

from decimal import Decimal
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

from delivery.assets.mit_edx_programs import (
    build_resources,
    open_unpublish_reviews,
    unpublish_review_issue,
)

PROGRAM_ID = "927093e3-46ba-4f44-a861-0f8c7aec4f74"


def program_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__mit_edx_programs row."""
    return {
        "readable_id": PROGRAM_ID,
        "title": "Circuits and Electronics",
        "description": "<p>Circuits</p><script>x()</script>",
        "url": "https://www.edx.org/xseries/mitx-circuits",
        "image_url": "https://cdn.example.com/banner.png",
        "last_modified": "2024-08-28T07:14:23.507563Z",
        "level": "intermediate",
        "start_date": "2019-06-20T15:00:00Z",
        "end_date": "2099-05-01T15:00:00Z",
        "enrollment_start": None,
        "enrollment_end": "2099-01-20T15:00:00Z",
        "price": Decimal("339.00"),
        "currency": "USD",
        "pace": ["instructor_paced", "self_paced"],
        "availability": "dated",
        "topics": ["Engineering"],
        "duration": "22 weeks",
        "min_weeks": 22,
        "max_weeks": 22,
        "time_commitment": "4-5 hours/week",
        "min_weekly_hours": 4,
        "max_weekly_hours": 5,
        "course_readable_ids": ["MITx+6.002.1x", "MITx+6.002.2x"],
        "etl_source": "mit_edx",
        "platform": "edx",
        "resource_type": "program",
        **overrides,
    }


def instructor_row(position: int, first: str, last: str) -> dict[str, Any]:
    """Build an integrations__learn__mit_edx_program_instructors row."""
    return {
        "readable_id": PROGRAM_ID,
        "first_name": first,
        "last_name": last,
        "full_name": f"{first} {last}",
        "instructor_position": position,
    }


def test_program_is_one_resource_with_one_run() -> None:
    """A program carries its dates, price and effort on a single run."""
    (resource,) = build_resources(
        [program_row()],
        [instructor_row(2, "Grace", "Hopper"), instructor_row(1, "Anant", "Agarwal")],
    )
    (run,) = resource["runs"]
    assert run["run_id"] == PROGRAM_ID
    assert run["prices"] == [{"amount": "339.00", "currency": "USD"}]
    assert run["level"] == ["intermediate"]
    assert [i["full_name"] for i in run["instructors"]] == [
        "Anant Agarwal",
        "Grace Hopper",
    ]
    assert run["time_commitment"] == "4-5 hours/week"
    assert resource["description"] == "<p>Circuits</p>"
    assert run["description"] == "<p>Circuits</p>"
    assert resource["image"] == {
        "url": "https://cdn.example.com/banner.png",
        "description": "Circuits and Electronics",
    }
    assert resource["courses"] == [
        {"readable_id": "MITx+6.002.1x", "platform": "edx"},
        {"readable_id": "MITx+6.002.2x", "platform": "edx"},
    ]
    assert resource["topics"] == [{"name": "Engineering"}]


def test_missing_values_become_empty_lists() -> None:
    """MIT Learn keeps existing topics on None, so absent values are sent as []."""
    (resource,) = build_resources(
        [
            program_row(
                topics=None,
                level=None,
                pace=None,
                course_readable_ids=None,
                image_url=None,
            )
        ],
        [],
    )
    assert resource["topics"] == []
    assert resource["courses"] == []
    assert resource["pace"] == []
    assert resource["image"] is None
    assert resource["runs"][0]["level"] == []
    assert resource["runs"][0]["instructors"] == []


def published_program() -> dict[str, Any]:
    """Build a program as MIT Learn's programs API returns it."""
    return {
        "id": 42,
        "readable_id": PROGRAM_ID,
        "title": "Circuits and Electronics",
        "url": "https://www.edx.org/xseries/mitx-circuits",
    }


def github_stub(open_issue_count: int) -> MagicMock:
    """Build a PyGithub stand-in whose search finds ``open_issue_count`` issues."""
    github = MagicMock()
    existing = MagicMock(totalCount=open_issue_count)
    existing.__getitem__.return_value = SimpleNamespace(
        html_url="https://github.com/o/r/issues/1"
    )
    github.search_issues.return_value = existing
    github.get_repo.return_value.create_issue.return_value = SimpleNamespace(
        html_url="https://github.com/o/r/issues/2"
    )
    return github


def test_review_issue_opened_for_a_program_without_one() -> None:
    """A still-published program with no open review issue gets one."""
    github = github_stub(open_issue_count=0)
    urls = open_unpublish_reviews(
        github, "o/r", [published_program()], checked_on="2026-09-19"
    )
    assert urls == ["https://github.com/o/r/issues/2"]
    title = github.get_repo.return_value.create_issue.call_args.kwargs["title"]
    assert PROGRAM_ID in title


def test_existing_review_issue_is_reused() -> None:
    """A program that stays unlisted keeps one issue rather than one per run."""
    github = github_stub(open_issue_count=1)
    urls = open_unpublish_reviews(
        github, "o/r", [published_program()], checked_on="2026-09-19"
    )
    assert urls == ["https://github.com/o/r/issues/1"]
    github.get_repo.return_value.create_issue.assert_not_called()


def test_review_issue_names_the_program_and_the_decision() -> None:
    """The issue says which program, where it lives in MIT Learn, and what to decide."""
    title, body = unpublish_review_issue(published_program(), checked_on="2026-09-19")
    assert PROGRAM_ID in title
    assert "MIT Learn id 42" in body
    assert "2026-09-19" in body
    assert "unpublish" in body
