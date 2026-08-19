"""Severity levels and the issue/report types shared by every `ol-dbt` check.

Lives in `lib` rather than `commands.validate` so a checker can reuse it without
importing the dbt-manifest machinery — the inventory checks in
`lib.inventory` need the report types and nothing else.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class Severity(StrEnum):
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ValidationIssue:
    check: str
    severity: Severity
    model: str
    message: str
    detail: str = ""


@dataclass
class ValidationReport:
    issues: list[ValidationIssue] = field(default_factory=list)

    def add(
        self,
        check: str,
        severity: Severity,
        model: str,
        message: str,
        detail: str = "",
    ) -> None:
        self.issues.append(
            ValidationIssue(
                check=check,
                severity=severity,
                model=model,
                message=message,
                detail=detail,
            )
        )

    @property
    def errors(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        return [i for i in self.issues if i.severity == Severity.WARNING]
