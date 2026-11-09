"""Tests for the glossary_terms check: every glossary FQN in dbt meta names a term in ``glossary/``."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.glossary_registry import GLOSSARY_TERMS_CHECK, check_glossary_terms
from ol_dbt_cli.lib.validation import ValidationReport

REPO_GLOSSARY = Path(__file__).parents[3] / "glossary"


def _dbt_project(tmp_path: Path, fqns: Any) -> Path:
    dbt_dir = tmp_path / "src" / "ol_dbt"
    (dbt_dir / "models" / "dimensional").mkdir(parents=True)
    (dbt_dir / "dbt_project.yml").write_text("name: open_learning\n")
    column = {"name": "email", "config": {"meta": {"openmetadata": {"glossary": fqns}}}}
    schema = {"version": 2, "models": [{"name": "dim_user", "columns": [column]}]}
    (dbt_dir / "models" / "dimensional" / "_dim_user.yml").write_text(yaml.safe_dump(schema))
    return dbt_dir


def test_known_glossary_fqns_in_dbt_meta_pass(tmp_path: Path) -> None:
    dbt_dir = _dbt_project(tmp_path, ["open-learning.identifiability.direct-identifier", "open-learning.pii"])
    report = ValidationReport()
    check_glossary_terms(dbt_dir, REPO_GLOSSARY, report)
    assert report.issues == []


@pytest.mark.parametrize(
    ("fqns", "problem"),
    [
        (["open-learning.direct-identifier"], "'open-learning.direct-identifier' is not declared"),
        (["open-learning.identifiability.direct-identifer"], "is not declared"),
        ("open-learning.pii", "must be a list"),
    ],
)
def test_an_unknown_glossary_fqn_in_dbt_meta_fails(tmp_path: Path, fqns: Any, problem: str) -> None:
    dbt_dir = _dbt_project(tmp_path, fqns)
    report = ValidationReport()
    check_glossary_terms(dbt_dir, REPO_GLOSSARY, report)
    [issue] = report.errors
    assert issue.check == GLOSSARY_TERMS_CHECK
    assert issue.model == "dim_user.email"
    assert problem in issue.message


def test_the_repository_dbt_project_names_only_declared_terms() -> None:
    report = ValidationReport()
    check_glossary_terms(REPO_GLOSSARY.parent / "src" / "ol_dbt", REPO_GLOSSARY, report)
    assert report.issues == []
