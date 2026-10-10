"""Hold each model's ``meta.cohort`` column declarations to one consistent policy.

An aggregate view that a consumer publishes under a k-anonymity floor says, per
column, what the floor has to do with it: which count is the row's headline
cohort, which other counts are cohorts of their own, and which totals and rates
give a cohort's size away. ol-analytics-api declares the same thing per response
model as a ``CohortPolicy`` and rejects an inconsistent one at import time. This
check applies those rules to the dbt side, so the declaration next to the SQL
that computes the column cannot drift into one the API would refuse.

A model opts in by declaring ``config.meta.cohort`` on any column; from then on
every column must declare it.
"""

from __future__ import annotations

from enum import StrEnum
from typing import TYPE_CHECKING, Any, Self

import yaml
from pydantic import BaseModel, ConfigDict, StrictBool, ValidationError, model_validator

from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from pathlib import Path

COHORT_POLICY_CHECK = "cohort_policy"


class CohortRole(StrEnum):
    # The row's headline cohort. A row whose primary count is under the floor is withheld whole.
    PRIMARY = "primary"
    # Another distinct-learner count in the row, withheld on its own when under the floor.
    SECONDARY = "secondary"
    # A total, rate or average withheld whenever a cohort it is computed from is.
    DERIVED = "derived"
    # A computed value that needs no rule of its own beyond the row being published.
    MEASURE = "measure"
    # A key, label or attribute of the row's grain.
    DIMENSION = "dimension"


class ColumnCohort(BaseModel):
    """One column's ``config.meta.cohort`` block."""

    model_config = ConfigDict(extra="forbid")

    role: CohortRole
    derived_from: tuple[str, ...] = ()
    contained_in: str | None = None
    uncontained: StrictBool = False

    @model_validator(mode="after")
    def _keys_match_role(self) -> Self:
        if self.role == CohortRole.DERIVED:
            if not self.derived_from:
                msg = "a derived column must list the cohort columns it is computed from in derived_from"
                raise ValueError(msg)
        elif self.derived_from:
            msg = f"derived_from is only for a derived column, not a {self.role.value} one"
            raise ValueError(msg)
        if self.role == CohortRole.SECONDARY:
            # Leaving both out would skip the complement rule without anyone having decided to.
            if (self.contained_in is None) == (not self.uncontained):
                msg = "a secondary cohort must set exactly one of contained_in and uncontained: true"
                raise ValueError(msg)
        elif self.contained_in is not None or self.uncontained:
            msg = f"contained_in and uncontained are only for a secondary cohort, not a {self.role.value} column"
            raise ValueError(msg)
        return self


def _column_cohort(column: Mapping[str, Any]) -> Any:
    """Return a column's cohort block from ``config.meta`` or the older column-level ``meta``.

    dbt copies either spelling into the manifest, so a declaration under the older
    one must not leave the model unchecked.
    """
    config = column.get("config")
    for meta in (config.get("meta") if isinstance(config, dict) else None, column.get("meta")):
        if isinstance(meta, dict) and "cohort" in meta:
            return meta["cohort"]
    return None


def _field_message(detail: Mapping[str, Any]) -> str:
    message = str(detail["msg"]).removeprefix("Value error, ")
    field = ".".join(str(part) for part in detail["loc"])
    return f"{field}: {message}" if field else message


def _model_issues(columns: Sequence[Mapping[str, Any]]) -> list[str]:
    """Return what is wrong with one model's cohort declarations.

    :param columns: The model's ``columns:`` entries as loaded from YAML.
    :returns: One message per problem, empty when the declarations are consistent.
    """
    issues: list[str] = []
    cohorts: dict[str, ColumnCohort] = {}
    for column in columns:
        name = str(column.get("name", "<unnamed column>"))
        raw = _column_cohort(column)
        if raw is None:
            issues.append(f"{name} declares no config.meta.cohort")
            continue
        try:
            cohorts[name] = ColumnCohort.model_validate(raw)
        except ValidationError as error:
            issues.extend(f"{name}: {_field_message(detail)}" for detail in error.errors())

    primaries = sorted(name for name, cohort in cohorts.items() if cohort.role == CohortRole.PRIMARY)
    if len(primaries) != 1:
        issues.append(f"expected exactly one primary cohort, found {len(primaries)}: {', '.join(primaries) or 'none'}")
    cohort_columns = {
        name for name, cohort in cohorts.items() if cohort.role in (CohortRole.PRIMARY, CohortRole.SECONDARY)
    }

    for name, cohort in cohorts.items():
        issues.extend(
            f"{name}: derived_from names {source}, which is not a primary or secondary cohort of this model"
            for source in cohort.derived_from
            if source not in cohort_columns
        )
        if cohort.contained_in is None:
            continue
        if cohort.contained_in not in cohort_columns:
            issues.append(
                f"{name}: contained_in names {cohort.contained_in}, "
                "which is not a primary or secondary cohort of this model"
            )
            continue
        seen = {name}
        container: str | None = cohort.contained_in
        while container is not None and container in cohorts:
            if container in seen:
                issues.append(f"{name}: contained_in forms a cycle through {container}")
                break
            seen.add(container)
            container = cohorts[container].contained_in
    return issues


def check_cohort_policy(dbt_dir: Path, report: ValidationReport) -> None:
    """Report every model whose ``meta.cohort`` declarations are incomplete or inconsistent.

    :param dbt_dir: The dbt project directory.
    :param report: The report to add an ERROR to for each problem.
    """
    models_dir = dbt_dir / "models"
    for path in sorted(p for pattern in ("*.yml", "*.yaml") for p in models_dir.rglob(pattern)):
        document = yaml.safe_load(path.read_text())
        # yaml_integrity reports a schema file of the wrong shape; this check only skips it.
        models = document.get("models") if isinstance(document, dict) else None
        for model in models or []:
            if not isinstance(model, dict):
                continue
            columns = [column for column in model.get("columns") or [] if isinstance(column, dict)]
            if not any(_column_cohort(column) is not None for column in columns):
                continue
            for message in _model_issues(columns):
                report.add(
                    COHORT_POLICY_CHECK,
                    Severity.ERROR,
                    str(model.get("name", "<unnamed model>")),
                    message,
                    f"Declared in {path.relative_to(dbt_dir).as_posix()}. The roles are described in "
                    "ol_dbt_cli/lib/cohort_policy.py.",
                )
