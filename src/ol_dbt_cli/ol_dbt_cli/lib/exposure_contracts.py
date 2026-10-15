"""Commit what each application exposure reads, so the application's CI can check against it.

ol-analytics-api selects columns of the B2B views by name and declares a
CohortPolicy per response model that has to agree with ``config.meta.cohort``
on those columns. Its CI cannot build this project, and the published dbt docs
are built on a target where those views are disabled. So for every exposure of
type application, ``ol-dbt exposures export`` writes ``exposures/<name>.json``:
the columns of each model the exposure depends on, with their cohort
declarations. The application checks its models against that file.

The file is generated from the model YAML, whose column set ``yaml_sql_sync``
already holds to the SQL. Columns are listed in the order the YAML documents
them; the application selects by name, so the order carries no meaning.

The ``exposure_contract`` check fails when the committed file no longer
matches, so a dbt change to anything an application reads shows up in the
change's own diff.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from pydantic import ValidationError

from ol_dbt_cli.lib.cohort_policy import ColumnCohort, column_cohort
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator

EXPOSURE_CONTRACT_CHECK = "exposure_contract"
DEFAULT_EXPOSURES_DIR = Path("exposures")
EXPORT_COMMAND = "ol-dbt exposures export"

_REF = re.compile(r"""^ref\(\s*['"]([^'"]+)['"]\s*\)$""")


@dataclass
class ExposureContracts:
    """The rendered contract file per exposure, and why any exposure could not be rendered."""

    files: dict[str, str] = field(default_factory=dict)
    problems: dict[str, list[str]] = field(default_factory=dict)


def _schema_documents(dbt_dir: Path) -> Iterator[dict[str, Any]]:
    models_dir = dbt_dir / "models"
    for path in sorted(p for pattern in ("*.yml", "*.yaml") for p in models_dir.rglob(pattern)):
        document = yaml.safe_load(path.read_text())
        # yaml_integrity reports a schema file of the wrong shape; this only skips it.
        if isinstance(document, dict):
            yield document


def _contract_column(column: dict[str, Any]) -> dict[str, Any]:
    """Return one column's contract entry, with its cohort block normalized.

    Normalizing drops keys spelled at their default (``contained_in: null``,
    ``uncontained: false``), so a consumer can test for a key's presence.

    :raises ValidationError: The column's cohort block is invalid.
    """
    entry: dict[str, Any] = {"name": column["name"]}
    cohort = column_cohort(column)
    if cohort is not None:
        entry["cohort"] = ColumnCohort.model_validate(cohort).model_dump(mode="json", exclude_defaults=True)
    return entry


def render_exposure_contracts(dbt_dir: Path) -> ExposureContracts:
    """Render the contract file of every application exposure in the project.

    :param dbt_dir: The dbt project directory.
    :returns: The file text per exposure name, and the problems that kept any exposure from rendering.
    :rtype: ExposureContracts
    """
    columns_by_model: dict[str, list[dict[str, Any]]] = {}
    exposures: list[dict[str, Any]] = []
    for document in _schema_documents(dbt_dir):
        for model in document.get("models") or []:
            if isinstance(model, dict) and isinstance(model.get("columns"), list):
                columns_by_model[str(model["name"])] = [c for c in model["columns"] if isinstance(c, dict)]
        exposures.extend(e for e in document.get("exposures") or [] if isinstance(e, dict))

    result = ExposureContracts()
    for exposure in exposures:
        if exposure.get("type") != "application":
            continue
        name = str(exposure["name"])
        problems: list[str] = []
        models: dict[str, Any] = {}
        for dependency in exposure.get("depends_on") or []:
            match = _REF.match(str(dependency).strip())
            if match is None:
                problems.append(f"depends_on entry {dependency!r} is not a ref(); only models can be exported")
                continue
            model_name = match.group(1)
            columns = columns_by_model.get(model_name)
            if not columns:
                problems.append(f"{model_name} has no columns declared in YAML")
                continue
            try:
                models[model_name] = {"columns": [_contract_column(column) for column in columns]}
            except ValidationError:
                problems.append(f"{model_name} has an invalid config.meta.cohort; see the cohort_policy check")
        if problems:
            result.problems[name] = problems
            continue
        result.files[name] = _render(name, models)
    return result


def _render(name: str, models: dict[str, Any]) -> str:
    """Render one contract file as JSON with one line per column, so a column change is a one-line diff."""
    model_blocks = []
    for model_name, model in models.items():
        column_lines = ",\n".join(f"        {json.dumps(column)}" for column in model["columns"])
        model_blocks.append(f'    {json.dumps(model_name)}: {{\n      "columns": [\n{column_lines}\n      ]\n    }}')
    models_block = ",\n".join(model_blocks)
    return (
        "{\n"
        f'  "generated_by": {json.dumps(EXPORT_COMMAND)},\n'
        f'  "exposure": {json.dumps(name)},\n'
        f'  "models": {{\n{models_block}\n  }}\n'
        "}\n"
    )


def write_exposure_contracts(dbt_dir: Path, exposures_dir: Path) -> ExposureContracts:
    """Write every application exposure's contract file and delete the files of exposures that are gone.

    Nothing is written when any exposure has a problem, so a partial export never lands.

    :param dbt_dir: The dbt project directory.
    :param exposures_dir: The directory the contract files live in.
    :returns: What was rendered, including any problems.
    :rtype: ExposureContracts
    :raises FileNotFoundError: ``dbt_dir`` is not a dbt project, which would otherwise
        render no exposures and delete every committed file.
    """
    if not (dbt_dir / "dbt_project.yml").is_file() or not (dbt_dir / "models").is_dir():
        msg = f"{dbt_dir} is not a dbt project (no dbt_project.yml and models/)"
        raise FileNotFoundError(msg)
    contracts = render_exposure_contracts(dbt_dir)
    if contracts.problems:
        return contracts
    exposures_dir.mkdir(parents=True, exist_ok=True)
    for path in exposures_dir.glob("*.json"):
        if path.stem not in contracts.files:
            path.unlink()
    for name, text in contracts.files.items():
        (exposures_dir / f"{name}.json").write_text(text)
    return contracts


def check_exposure_contracts(dbt_dir: Path, exposures_dir: Path, report: ValidationReport) -> None:
    """Report every application exposure whose committed contract file is missing or out of date.

    :param dbt_dir: The dbt project directory.
    :param exposures_dir: The directory the contract files live in.
    :param report: The report to add an ERROR to for each problem.
    """
    contracts = render_exposure_contracts(dbt_dir)
    for name, problems in contracts.problems.items():
        for problem in problems:
            report.add(EXPOSURE_CONTRACT_CHECK, Severity.ERROR, name, problem)

    regenerate = f"Run `{EXPORT_COMMAND}` and commit the result."
    for name, text in contracts.files.items():
        path = exposures_dir / f"{name}.json"
        if not path.exists():
            report.add(EXPOSURE_CONTRACT_CHECK, Severity.ERROR, name, f"{path.name} does not exist", regenerate)
        elif path.read_text() != text:
            report.add(
                EXPOSURE_CONTRACT_CHECK,
                Severity.ERROR,
                name,
                f"{path.name} does not match the model YAML",
                f"A column the application reads, or its cohort declaration, changed. {regenerate}",
            )
    if exposures_dir.is_dir():
        for path in sorted(exposures_dir.glob("*.json")):
            if path.stem not in contracts.files and path.stem not in contracts.problems:
                report.add(
                    EXPOSURE_CONTRACT_CHECK,
                    Severity.ERROR,
                    path.stem,
                    f"{path.name} belongs to no application exposure",
                    regenerate,
                )
