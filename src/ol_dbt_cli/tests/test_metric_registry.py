"""Tests for business metric definitions as code (the metric_registry validate check)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.metric_registry import (
    METRIC_REGISTRY_CHECK,
    check_metric_registry,
    load_metrics,
)
from ol_dbt_cli.lib.sql_parser import ParsedModel
from ol_dbt_cli.lib.validation import Severity, ValidationReport
from ol_dbt_cli.lib.yaml_registry import YamlColumn, YamlModel, YamlRegistry

REPO_ROOT = Path(__file__).resolve().parents[3]
NAME = "learner_completion_status"
MODEL = "afact_learner_courserun_progress"


def _metric(body: dict[str, Any] | None = None, **top_level: Any) -> dict[str, Any]:
    return {
        "metric": {"name": NAME, "metricType": "OTHER", **(body or {})},
        "status": "Approved",
        "implemented_by": [{"dbt_model": MODEL, "columns": ["completion_status"]}],
        **top_level,
    }


def _write(directory: Path, data: dict[str, Any], filename: str = f"{NAME}.yaml") -> None:
    (directory / filename).write_text(yaml.safe_dump(data))


def _project(
    yaml_columns: set[str] | None = None, sql_columns: set[str] | None = None, *, has_star: bool = False
) -> tuple[YamlRegistry, dict[str, ParsedModel]]:
    """Build a one-model project; ``None`` leaves the model without a YAML entry or a SQL file."""
    registry = YamlRegistry()
    if yaml_columns is not None:
        registry.models[MODEL] = YamlModel(
            name=MODEL, source_file=Path("_models.yml"), columns={c: YamlColumn(name=c) for c in yaml_columns}
        )
    parsed = (
        {MODEL: ParsedModel(name=MODEL, output_columns=sql_columns, has_star=has_star)}
        if sql_columns is not None
        else {}
    )
    return registry, parsed


INTACT = ({"completion_status"}, {"completion_status"})


def _run(
    tmp_path: Path,
    data: dict[str, Any],
    project: tuple[YamlRegistry, dict[str, ParsedModel]] | None = None,
    filename: str = f"{NAME}.yaml",
) -> list[str]:
    _write(tmp_path, data, filename)
    report = ValidationReport()
    check_metric_registry(load_metrics(tmp_path), *(project or _project(*INTACT)), report)
    assert all(i.check == METRIC_REGISTRY_CHECK and i.severity == Severity.ERROR for i in report.issues)
    return [i.message for i in report.issues]


def test_repo_metrics_load() -> None:
    metrics = load_metrics(REPO_ROOT / "metrics")
    assert [m.path.stem for m in metrics] == [m.name for m in metrics]


def test_intact_metric_passes(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric()) == []


def test_missing_model_errors(tmp_path: Path) -> None:
    messages = _run(tmp_path, _metric(), _project())
    assert messages == [f"Metric {NAME}.yaml names a dbt_model that does not exist"]


def test_column_missing_from_sql_reports_only_that(tmp_path: Path) -> None:
    data = _metric(implemented_by=[{"dbt_model": MODEL, "columns": ["completion_status", "is_certified"]}])
    assert _run(tmp_path, data, _project({"completion_status", "is_certified"}, {"completion_status"})) == [
        "Column 'is_certified' is not selected by the model SQL"
    ]


def test_column_not_declared_in_yaml_errors(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric(), _project(set(), {"completion_status"})) == [
        "Column 'completion_status' is not declared in the YAML"
    ]


def test_model_without_yaml_entry_errors(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric(), _project(None, {"completion_status"})) == [
        "Column 'completion_status' is not declared in the YAML"
    ]


def test_unresolved_select_star_skips_the_sql_half(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric(), _project({"completion_status"}, {"other"}, has_star=True)) == []


def test_unparsed_sql_skips_the_sql_half(tmp_path: Path) -> None:
    registry, parsed = _project({"completion_status"}, {"other"})
    parsed[MODEL].parse_error = "could not parse"
    assert _run(tmp_path, _metric(), (registry, parsed)) == []


def test_column_names_are_case_insensitive(tmp_path: Path) -> None:
    data = _metric(implemented_by=[{"dbt_model": MODEL, "columns": ["Completion_Status"]}])
    assert _run(tmp_path, data) == []


def test_fqn_binding_is_not_checked_locally(tmp_path: Path) -> None:
    data = _metric(implemented_by=[{"fqn": "Superset.model.41", "type": "dashboardDataModel", "columns": ["c"]}])
    assert _run(tmp_path, data, _project()) == []


def test_duplicate_metric_name_errors(tmp_path: Path) -> None:
    _write(tmp_path, _metric(), f"{NAME}.yml")
    messages = _run(tmp_path, _metric())
    assert messages == [f"Metric '{NAME}' is declared in 2 files: {NAME}.yaml, {NAME}.yml"]


@pytest.mark.parametrize("name", ["completion", "learner__completion", "LearnerCompletion", "learner-completion", "_x"])
def test_name_breaking_the_convention_errors(tmp_path: Path, name: str) -> None:
    messages = _run(tmp_path, _metric({"name": name}), filename=f"{name}.yaml")
    assert messages == [f"Metric name '{name}' breaks the naming convention"]


def test_file_not_named_after_the_metric_errors(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric(), filename="completion.yaml") == [f"File completion.yaml declares metric '{NAME}'"]


@pytest.mark.parametrize(
    ("field", "value"),
    [("metricType", "GAUGE"), ("unitOfMeasurement", "SECONDS"), ("granularity", "day"), ("metricType", ["COUNT"])],
)
def test_value_outside_the_openmetadata_enum_errors(tmp_path: Path, field: str, value: Any) -> None:
    assert _run(tmp_path, _metric({field: value})) == [
        f"`metric.{field}` is {value!r}, which OpenMetadata does not accept"
    ]


def test_openmetadata_enum_values_pass(tmp_path: Path) -> None:
    body = {"metricType": "COUNT", "unitOfMeasurement": "COUNT", "granularity": "MONTH"}
    assert _run(tmp_path, _metric(body)) == []


@pytest.mark.parametrize("field", ["id", "fullyQualifiedName", "glossaryTerms"])
def test_field_the_file_may_not_set_errors(tmp_path: Path, field: str) -> None:
    assert _run(tmp_path, _metric({field: "x"})) == [f"`metric.{field}` is not a field the file may set"]


def test_unknown_status_errors(tmp_path: Path) -> None:
    assert _run(tmp_path, _metric(status="Live")) == ["`status` is 'Live', which OpenMetadata does not accept"]


def test_status_is_optional(tmp_path: Path) -> None:
    data = _metric()
    del data["status"]
    assert _run(tmp_path, data) == []


@pytest.mark.parametrize(
    ("data", "message"),
    [
        ({"implemented_by": []}, "top-level `metric` mapping"),
        ({"metric": {"metricType": "OTHER"}, "implemented_by": []}, "`metric` needs a `name`"),
        ({"metric": {"name": NAME}}, "`implemented_by` must list at least one"),
        ({"metric": {"name": NAME}, "implemented_by": []}, "`implemented_by` must list at least one"),
        ({"metric": {"name": NAME}, "implemented_by": ["m"]}, "must be a mapping"),
        ({"metric": {"name": NAME}, "implemented_by": [{"dbt_model": "m"}]}, "non-empty `columns` list"),
        ({"metric": {"name": NAME}, "implemented_by": [{"dbt_model": "m", "columns": []}]}, "non-empty `columns` list"),
        ({"metric": {"name": NAME}, "implemented_by": [{"columns": ["c"]}]}, "exactly one of"),
        (
            {"metric": {"name": NAME}, "implemented_by": [{"dbt_model": "m", "fqn": "a.b", "columns": ["c"]}]},
            "exactly one of",
        ),
        (
            {"metric": {"name": NAME}, "implemented_by": [{"dbt_source": "raw.users", "columns": ["c"]}]},
            "can't be a dbt_source",
        ),
        ({"metric": {"name": NAME}, "implemented_by": [{"dbt_model": None, "columns": ["c"]}]}, "needs a name"),
        ({"metric": {"name": NAME}, "implemented_by": [{"fqn": {"a": "b"}, "columns": ["c"]}]}, "needs a name"),
    ],
)
def test_malformed_file_raises(tmp_path: Path, data: dict[str, Any], message: str) -> None:
    _write(tmp_path, data)
    with pytest.raises(ValueError, match=message):
        load_metrics(tmp_path)


def test_no_metrics_dir_is_silent(tmp_path: Path) -> None:
    assert load_metrics(tmp_path / "missing") == []


def test_non_yaml_files_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "README.md").write_text("# Metrics")
    assert load_metrics(tmp_path) == []
