"""Tests for ol-dbt exposures export and the exposure_contract validate check."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.exposure_contracts import (
    EXPOSURE_CONTRACT_CHECK,
    check_exposure_contracts,
    render_exposure_contracts,
    write_exposure_contracts,
)
from ol_dbt_cli.lib.validation import Severity, ValidationReport

REPO_ROOT = Path(__file__).resolve().parents[3]


def _project(tmp_path: Path, *, exposures: list[dict[str, Any]] | None = None) -> Path:
    dbt_dir = tmp_path / "src" / "ol_dbt"
    models_dir = dbt_dir / "models"
    models_dir.mkdir(parents=True)
    (dbt_dir / "dbt_project.yml").write_text("name: test\n")
    models = [
        {
            "name": "mv_trend",
            "columns": [
                {"name": "organization_key", "config": {"meta": {"cohort": {"role": "dimension"}}}},
                {"name": "enrolled_learners", "config": {"meta": {"cohort": {"role": "primary"}}}},
            ],
        },
        {"name": "mv_learner", "columns": [{"name": "user_id"}, {"name": "email"}]},
        {"name": "dim_unexposed", "columns": [{"name": "unexposed_pk"}]},
    ]
    if exposures is None:
        exposures = [
            {
                "name": "api_dashboard",
                "type": "application",
                "depends_on": ["ref('mv_trend')", 'ref("mv_learner")'],
            },
            {"name": "superset_dashboard", "type": "dashboard", "depends_on": ["ref('dim_unexposed')"]},
        ]
    (models_dir / "_models.yml").write_text(yaml.safe_dump({"models": models}))
    (models_dir / "_exposures.yml").write_text(yaml.safe_dump({"exposures": exposures}))
    return dbt_dir


def _check(dbt_dir: Path, exposures_dir: Path) -> list[tuple[str, str]]:
    report = ValidationReport()
    check_exposure_contracts(dbt_dir, exposures_dir, report)
    assert all(i.check == EXPOSURE_CONTRACT_CHECK and i.severity == Severity.ERROR for i in report.issues)
    return [(i.model, i.message) for i in report.issues]


def test_export_writes_one_file_per_application_exposure(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    exposures_dir = tmp_path / "exposures"
    write_exposure_contracts(dbt_dir, exposures_dir)

    assert sorted(p.name for p in exposures_dir.iterdir()) == ["api_dashboard.json"]
    assert json.loads((exposures_dir / "api_dashboard.json").read_text()) == {
        "generated_by": "ol-dbt exposures export",
        "exposure": "api_dashboard",
        "models": {
            "mv_trend": {
                "columns": [
                    {"name": "organization_key", "cohort": {"role": "dimension"}},
                    {"name": "enrolled_learners", "cohort": {"role": "primary"}},
                ]
            },
            "mv_learner": {"columns": [{"name": "user_id"}, {"name": "email"}]},
        },
    }
    assert _check(dbt_dir, exposures_dir) == []


def test_a_column_change_is_one_line_of_the_file(tmp_path: Path) -> None:
    text = render_exposure_contracts(_project(tmp_path)).files["api_dashboard"]
    assert '        {"name": "enrolled_learners", "cohort": {"role": "primary"}}' in text.splitlines()


def test_a_missing_file_is_reported(tmp_path: Path) -> None:
    assert _check(_project(tmp_path), tmp_path / "exposures") == [
        ("api_dashboard", "api_dashboard.json does not exist")
    ]


def test_a_changed_cohort_declaration_makes_the_file_stale(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    exposures_dir = tmp_path / "exposures"
    write_exposure_contracts(dbt_dir, exposures_dir)
    models_file = dbt_dir / "models" / "_models.yml"
    models_file.write_text(models_file.read_text().replace("role: dimension", "role: measure"))

    assert _check(dbt_dir, exposures_dir) == [("api_dashboard", "api_dashboard.json does not match the model YAML")]


def test_a_file_without_an_exposure_is_reported_and_export_removes_it(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    exposures_dir = tmp_path / "exposures"
    write_exposure_contracts(dbt_dir, exposures_dir)
    (exposures_dir / "retired_app.json").write_text("{}\n")
    (exposures_dir / "README.md").write_text("kept\n")

    assert _check(dbt_dir, exposures_dir) == [("retired_app", "retired_app.json belongs to no application exposure")]
    write_exposure_contracts(dbt_dir, exposures_dir)
    assert sorted(p.name for p in exposures_dir.iterdir()) == ["README.md", "api_dashboard.json"]


def test_an_exposure_that_cannot_be_rendered_writes_nothing(tmp_path: Path) -> None:
    dbt_dir = _project(
        tmp_path,
        exposures=[
            {"name": "ok_app", "type": "application", "depends_on": ["ref('mv_trend')"]},
            {
                "name": "broken_app",
                "type": "application",
                "depends_on": ["source('raw', 'users')", "ref('no_such_model')"],
            },
        ],
    )
    exposures_dir = tmp_path / "exposures"

    contracts = write_exposure_contracts(dbt_dir, exposures_dir)
    assert contracts.problems == {
        "broken_app": [
            "depends_on entry \"source('raw', 'users')\" is not a ref(); only models can be exported",
            "no_such_model has no columns declared in YAML",
        ]
    }
    assert not exposures_dir.exists()
    assert _check(dbt_dir, exposures_dir) == [
        ("broken_app", "depends_on entry \"source('raw', 'users')\" is not a ref(); only models can be exported"),
        ("broken_app", "no_such_model has no columns declared in YAML"),
        ("ok_app", "ok_app.json does not exist"),
    ]


def test_the_committed_files_match_the_project() -> None:
    assert _check(REPO_ROOT / "src" / "ol_dbt", REPO_ROOT / "exposures") == []


def test_cohort_keys_spelled_at_their_default_are_dropped(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    models_file = dbt_dir / "models" / "_models.yml"
    models = yaml.safe_load(models_file.read_text())
    models["models"][0]["columns"][0]["config"]["meta"]["cohort"] |= {"contained_in": None, "uncontained": False}
    models_file.write_text(yaml.safe_dump(models))

    text = render_exposure_contracts(dbt_dir).files["api_dashboard"]
    assert '        {"name": "organization_key", "cohort": {"role": "dimension"}},' in text.splitlines()


def test_an_invalid_cohort_block_is_a_problem_not_a_crash(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    models_file = dbt_dir / "models" / "_models.yml"
    models_file.write_text(models_file.read_text().replace("role: dimension", "role: headline"))

    assert render_exposure_contracts(dbt_dir).problems == {
        "api_dashboard": ["mv_trend has an invalid config.meta.cohort; see the cohort_policy check"]
    }


def test_export_from_a_path_that_is_not_a_dbt_project_leaves_the_files_alone(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    exposures_dir = tmp_path / "exposures"
    write_exposure_contracts(dbt_dir, exposures_dir)
    before = (exposures_dir / "api_dashboard.json").read_text()

    with pytest.raises(FileNotFoundError, match="is not a dbt project"):
        write_exposure_contracts(tmp_path / "src" / "ol_db", exposures_dir)
    assert (exposures_dir / "api_dashboard.json").read_text() == before


def _set_meta(dbt_dir: Path, model: int, column: int, meta: dict[str, Any], *, column_level: bool = False) -> None:
    models_file = dbt_dir / "models" / "_models.yml"
    models = yaml.safe_load(models_file.read_text())
    entry = models["models"][model]["columns"][column]
    parent = entry if column_level else entry.setdefault("config", {})
    parent.setdefault("meta", {}).update(meta)
    models_file.write_text(yaml.safe_dump(models))


def test_an_api_description_is_exported_after_the_cohort(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    _set_meta(dbt_dir, 0, 1, {"api_description": "Learners enrolled in the course."})

    text = render_exposure_contracts(dbt_dir).files["api_dashboard"]
    assert (
        '        {"name": "enrolled_learners", "cohort": {"role": "primary"}, '
        '"api_description": "Learners enrolled in the course."}'
    ) in text.splitlines()
    assert '        {"name": "organization_key", "cohort": {"role": "dimension"}},' in text.splitlines()


def test_an_api_description_under_the_older_column_meta_is_exported(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    _set_meta(dbt_dir, 1, 0, {"api_description": "The learner's ID."}, column_level=True)

    document = json.loads(render_exposure_contracts(dbt_dir).files["api_dashboard"])
    assert document["models"]["mv_learner"]["columns"][0] == {"name": "user_id", "api_description": "The learner's ID."}


def test_a_changed_api_description_makes_the_file_stale(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    _set_meta(dbt_dir, 0, 1, {"api_description": "Learners enrolled in the course."})
    exposures_dir = tmp_path / "exposures"
    write_exposure_contracts(dbt_dir, exposures_dir)
    _set_meta(dbt_dir, 0, 1, {"api_description": "Learners enrolled in the course run."})

    assert _check(dbt_dir, exposures_dir) == [("api_dashboard", "api_dashboard.json does not match the model YAML")]


@pytest.mark.parametrize("value", [None, "", "Ends in a newline.\n", " Leading space.", 3, ["a list"]])
def test_an_api_description_no_field_could_match_is_a_problem(tmp_path: Path, value: object) -> None:
    # None is also what a bare ``api_description:`` key loads as.
    dbt_dir = _project(tmp_path)
    _set_meta(dbt_dir, 1, 1, {"api_description": value})

    assert render_exposure_contracts(dbt_dir).problems == {
        "api_dashboard": [
            "mv_learner.email has a config.meta.api_description that is not a non-empty string without "
            "surrounding whitespace (write a folded block as >-, not >)"
        ]
    }


def test_an_invalid_api_description_and_cohort_on_one_model_are_both_reported(tmp_path: Path) -> None:
    dbt_dir = _project(tmp_path)
    models_file = dbt_dir / "models" / "_models.yml"
    models_file.write_text(models_file.read_text().replace("role: dimension", "role: headline"))
    _set_meta(dbt_dir, 0, 1, {"api_description": ""})

    assert render_exposure_contracts(dbt_dir).problems == {
        "api_dashboard": [
            "mv_trend.enrolled_learners has a config.meta.api_description that is not a non-empty string "
            "without surrounding whitespace (write a folded block as >-, not >)",
            "mv_trend has an invalid config.meta.cohort; see the cohort_policy check",
        ]
    }
