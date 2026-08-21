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
from typing import Annotated, Any

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.git_utils import get_repo_root, resolve_merge_base
from ol_dbt_cli.lib.inventory import (
    DEFAULT_INVENTORY_DIR,
    RenderError,
    check_removals,
    load_snapshot,
    load_snapshot_at_ref,
    load_units,
    render_airbyte,
    render_dagster_intervals,
    validate_inventory,
)
from ol_dbt_cli.lib.validation import Severity, ValidationIssue, ValidationReport

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
        # The message carries a filesystem path, which may contain brackets.
        err_console.print(f"[bold red]{escape(str(error))}")
        sys.exit(1)

    if output_format == "json":
        _emit_json(report.issues)
    else:
        _emit_text(report.issues)
        tables = sum(len(unit.tables) for unit in units)
        console.print(
            f"\n[bold]Summary:[/] {len(units)} unit(s), {tables} table(s), "
            f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)."
        )

    if report.errors:
        sys.exit(1)


@inventory_app.command(name="check-removals")
def check_removals_command(
    *,
    inventory_dir: Annotated[
        Path,
        Parameter(
            name=["--inventory-dir", "-i"],
            help="Directory holding vocabulary.yml, schema/, units/ and retired.yml.",
        ),
    ] = DEFAULT_INVENTORY_DIR,
    base_ref: Annotated[
        str,
        Parameter(
            name=["--base-ref", "-b"],
            help="Branch to compare against; the diff runs from its merge base with HEAD.",
        ),
    ] = "origin/main",
    output_format: Annotated[
        str,
        Parameter(name=["--format", "-f"], help="Output format: text (default) or json."),
    ] = "text",
) -> None:
    """Fail on any table this change dropped from the inventory without saying so.

    A dropped entry means the loader simply stops loading — no error anywhere,
    and a dbt model that quietly goes stale (§7.2). Acknowledge a removal in
    `retired.yml` with a date and a reason, or, for a rename, with `renamed_from:`
    on the entry that replaced it.

    Exits non-zero if anything is an ERROR. These findings are not baselineable:
    they are always fixable by editing text in the same pull request.
    """
    report = ValidationReport()
    try:
        repo_root = get_repo_root(inventory_dir if inventory_dir.exists() else Path.cwd())
        merge_base = resolve_merge_base(base_ref, repo_root=repo_root)
    except RuntimeError as error:
        err_console.print(f"[bold red]{escape(str(error))}")
        sys.exit(1)

    previous = load_snapshot_at_ref(inventory_dir, merge_base, repo_root=repo_root)
    current = load_snapshot(inventory_dir)
    check_removals(previous, current, report)

    if output_format == "json":
        _emit_json(report.issues)
    else:
        _emit_text(report.issues)
        console.print(
            f"\n[bold]Summary:[/] compared {len(previous.units)} unit(s) at "
            f"{merge_base[:12]} against {len(current.units)} on disk — "
            f"{len(report.errors)} error(s), {len(report.warnings)} warning(s)."
        )

    if report.errors:
        sys.exit(1)


RENDER_TARGETS = ("airbyte", "dagster-intervals")


@inventory_app.command
def render(
    target: Annotated[
        str,
        Parameter(help=f"What to render: {', '.join(RENDER_TARGETS)}."),
    ],
    *,
    inventory_dir: Annotated[
        Path,
        Parameter(name=["--inventory-dir", "-i"], help="Directory holding units/."),
    ] = DEFAULT_INVENTORY_DIR,
    output: Annotated[
        Path | None,
        Parameter(name=["--output", "-o"], help="Write here instead of stdout."),
    ] = None,
) -> None:
    """Generate a downstream artifact from the inventory.

    `airbyte` emits the narrow JSON that is committed into ol-infrastructure
    beside the Pulumi stack that consumes it (§4) — it is not fetched, and no
    pipeline renders it. `dagster-intervals` emits the sync-cadence map that
    replaces the hand-maintained 32-entry literal in `definitions.py`, whose
    silent 24-hour default for a mistyped key is its own class of stale-source
    incident (§1.3).

    Deliberately does not validate first: a render of an inventory that fails
    some rule is a thing you sometimes want to look at, and CI runs `validate`
    on the same PR anyway. The one exception is an inventory whose render is
    undefined rather than merely wrong — a connection carrying a stream no table
    declares. That stops the render with a message rather than silently dropping
    the stream, because this JSON is applied to production Airbyte and a dropped
    stream is a connection reconfigured to stop carrying a table.
    """
    if target not in RENDER_TARGETS:
        err_console.print(f"[bold red]Unknown render target {escape(target)!r}. Expected one of: {RENDER_TARGETS}")
        sys.exit(1)

    units = load_units(inventory_dir)
    try:
        document: Any = render_airbyte(units) if target == "airbyte" else render_dagster_intervals(units)
    except RenderError as error:
        err_console.print(f"[bold red]{escape(str(error))}")
        sys.exit(1)
    text = json.dumps(document, indent=2, sort_keys=False, ensure_ascii=False) + "\n"

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text)
        console.print(f"Wrote {escape(str(output))}")
    else:
        print(text, end="")  # noqa: T201


def _emit_json(issues: list[ValidationIssue]) -> None:
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
                for issue in issues
            ],
            indent=2,
        )
    )


def _emit_text(issues: list[ValidationIssue]) -> None:
    for issue in issues:
        style = SEVERITY_STYLE[issue.severity]
        # Messages quote JSON Schema patterns like `^[a-z][a-z0-9_]*$`, which
        # rich would read as markup and swallow.
        console.print(f"[{style}]{issue.severity.value}[/] {escape(issue.model)}: {escape(issue.message)}")
        if issue.detail:
            console.print(f"    [dim]{escape(issue.detail)}[/]")
