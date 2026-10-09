"""Tests for the cohort_policy validate check."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.cohort_policy import COHORT_POLICY_CHECK, check_cohort_policy
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]


def _column(name: str, **cohort: Any) -> dict[str, Any]:
    return {"name": name, "config": {"meta": {"cohort": cohort}}}


def _consistent_columns() -> list[dict[str, Any]]:
    return [
        _column("organization_key", role="dimension"),
        _column("enrolled_learners", role="primary"),
        _column("engaged_learners", role="secondary", contained_in="enrolled_learners"),
        _column("video_watchers", role="secondary", contained_in="engaged_learners"),
        _column("certified_learners", role="secondary", uncontained=True),
        _column("total_videos_watched", role="derived", derived_from=["video_watchers"]),
        _column("seat_utilization_pct", role="measure"),
    ]


def _messages(tmp_path: Path, columns: list[dict[str, Any]]) -> list[str]:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "_models.yml").write_text(yaml.safe_dump({"models": [{"name": "mv", "columns": columns}]}))
    report = ValidationReport()
    check_cohort_policy(tmp_path, report)
    assert all(
        i.check == COHORT_POLICY_CHECK and i.severity == Severity.ERROR and i.model == "mv" for i in report.issues
    )
    return [i.message for i in report.issues]


def test_a_consistent_model_passes(tmp_path: Path) -> None:
    assert _messages(tmp_path, _consistent_columns()) == []


def test_a_model_with_no_declarations_is_not_checked(tmp_path: Path) -> None:
    assert _messages(tmp_path, [{"name": "a"}, {"name": "b"}]) == []


def test_a_column_added_without_a_declaration_is_reported(tmp_path: Path) -> None:
    assert _messages(tmp_path, [*_consistent_columns(), {"name": "problem_attempters"}]) == [
        "problem_attempters declares no config.meta.cohort"
    ]


@pytest.mark.parametrize(
    ("cohort", "message"),
    [
        (
            {"role": "secondary"},
            "new_column: a secondary cohort must set exactly one of contained_in and uncontained: true",
        ),
        (
            {"role": "secondary", "contained_in": "enrolled_learners", "uncontained": True},
            "new_column: a secondary cohort must set exactly one of contained_in and uncontained: true",
        ),
        (
            {"role": "secondary", "contained_in": "total_videos_watched"},
            "new_column: contained_in names total_videos_watched, "
            "which is not a primary or secondary cohort of this model",
        ),
        (
            {"role": "derived"},
            "new_column: a derived column must list the cohort columns it is computed from in derived_from",
        ),
        (
            {"role": "derived", "derived_from": ["seat_utilization_pct", "no_such_column"]},
            "new_column: derived_from names seat_utilization_pct, "
            "which is not a primary or secondary cohort of this model",
        ),
        (
            {"role": "dimension", "contained_in": "enrolled_learners"},
            "new_column: contained_in and uncontained are only for a secondary cohort, not a dimension column",
        ),
        (
            {"role": "measure", "derived_from": ["enrolled_learners"]},
            "new_column: derived_from is only for a derived column, not a measure one",
        ),
        ({"role": "primary"}, "expected exactly one primary cohort, found 2: enrolled_learners, new_column"),
    ],
)
def test_an_inconsistent_declaration_is_reported(tmp_path: Path, cohort: dict[str, Any], message: str) -> None:
    assert message in _messages(tmp_path, [*_consistent_columns(), _column("new_column", **cohort)])


def test_an_unknown_role_or_key_is_reported(tmp_path: Path) -> None:
    messages = _messages(
        tmp_path,
        [*_consistent_columns(), _column("a", role="cohort"), _column("b", role="dimension", subset_of="x")],
    )
    assert len(messages) == 2
    assert messages[0].startswith(
        "a: role: Input should be 'primary', 'secondary', 'derived', 'measure' or 'dimension'"
    )
    assert messages[1] == "b: subset_of: Extra inputs are not permitted"


def test_a_declaration_under_column_level_meta_opts_the_model_in(tmp_path: Path) -> None:
    columns: list[dict[str, Any]] = [
        {"name": "enrolled_learners", "meta": {"cohort": {"role": "primary"}}},
        {"name": "video_watchers"},
    ]
    assert _messages(tmp_path, columns) == ["video_watchers declares no config.meta.cohort"]


def test_a_schema_file_of_the_wrong_shape_is_skipped(tmp_path: Path) -> None:
    models_dir = tmp_path / "models"
    models_dir.mkdir()
    (models_dir / "_list.yml").write_text("- not a mapping\n")
    (models_dir / "_models.yml").write_text(
        yaml.safe_dump(
            {
                "models": [
                    "not a model",
                    {"name": "plain", "columns": [{"name": "a", "config": "text"}, {"name": "b", "meta": ["x"]}]},
                    {"columns": [{"config": {"meta": {"cohort": "primary"}}}]},
                ]
            }
        )
    )
    report = ValidationReport()
    check_cohort_policy(tmp_path, report)
    assert [(i.model, i.message) for i in report.issues] == [
        ("<unnamed model>", "<unnamed column>: Input should be a valid dictionary or instance of ColumnCohort"),
        ("<unnamed model>", "expected exactly one primary cohort, found 0: none"),
    ]


def test_a_model_with_no_primary_is_reported(tmp_path: Path) -> None:
    columns = [column for column in _consistent_columns() if column["name"] == "organization_key"]
    assert _messages(tmp_path, columns) == ["expected exactly one primary cohort, found 0: none"]


def test_a_containment_cycle_is_reported(tmp_path: Path) -> None:
    columns = [
        _column("enrolled_learners", role="primary"),
        _column("a", role="secondary", contained_in="b"),
        _column("b", role="secondary", contained_in="a"),
    ]
    assert _messages(tmp_path, columns) == [
        "a: contained_in forms a cycle through a",
        "b: contained_in forms a cycle through b",
    ]


def test_the_project_declarations_are_consistent() -> None:
    report = ValidationReport()
    check_cohort_policy(REPO_ROOT / "src" / "ol_dbt", report)
    assert [f"{i.model}: {i.message}" for i in report.issues] == []
