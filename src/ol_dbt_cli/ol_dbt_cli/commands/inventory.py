"""`ol-dbt inventory` — checks over the ingestion inventory.

The inventory is the human source of truth for what we load: one file per
`(deployment, layer)` unit under ``ingestion/inventory/units/``. See
``docs/specs/INGESTION_INVENTORY_SPEC.md``.

Lives under the dbt CLI because dbt source generation is its first consumer and
because `ol-dbt validate` already has the severity/report plumbing these checks
want. The rules themselves are in ``ol_dbt_cli.lib.inventory``, which imports
neither dbt nor duckdb so it can move to a standalone package when phase 2's dlt
sources need it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.inventory import DEFAULT_INVENTORY_DIR, validate_inventory
from ol_dbt_cli.lib.validation import Severity, ValidationReport

console = Console()
err_console = Console(stderr=True)

inventory_app = App(
    name="inventory",
    help="Validate and report on the ingestion inventory.",
)

SEVERITY_STYLE = {
    Severity.ERROR: "bold red",
    Severity.WARNING: "yellow",
    Severity.INFO: "dim",
}


@inventory_app.command
def validate(
    *,
    inventory_dir: Annotated[
        Path,
        Parameter(
            name=["--inventory-dir", "-i"],
            help="Directory holding vocabulary.yml, schema/ and units/.",
        ),
    ] = DEFAULT_INVENTORY_DIR,
    output_format: Annotated[
        str,
        Parameter(
            name=["--format", "-f"],
            help="Output format: text (default) or json.",
        ),
    ] = "text",
) -> None:
    """Check every unit against the schema, the vocabulary and the §3.3 rules.

    Exits non-zero if anything is an ERROR, so CI can gate on it.
    """
    report = ValidationReport()
    try:
        units = validate_inventory(inventory_dir, report)
    except FileNotFoundError as error:
        err_console.print(f"[bold red]{error}")
        sys.exit(1)

    if output_format == "json":
        print(  # noqa: T201
            json.dumps(
                [
                    {
                        "check": issue.check,
                        "severity": issue.severity.value,
                        "model": issue.model,
                        "message": issue.message,
                        "detail": issue.detail,
                    }
                    for issue in report.issues
                ],
                indent=2,
            )
        )
    else:
        for issue in report.issues:
            style = SEVERITY_STYLE[issue.severity]
            # Messages quote JSON Schema patterns like `^[a-z][a-z0-9_]*$`, which
            # rich would read as markup and swallow.
            console.print(f"[{style}]{issue.severity.value}[/] {escape(issue.model)}: {escape(issue.message)}")
            if issue.detail:
                console.print(f"    [dim]{escape(issue.detail)}[/]")
        tables = sum(len(unit.tables) for unit in units)
        console.print(
            f"\n[bold]Summary:[/] {len(units)} unit(s), {tables} table(s), "
            f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)."
        )

    if report.errors:
        sys.exit(1)
