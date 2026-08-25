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
    check_drift,
    check_removals,
    load_snapshot,
    load_snapshot_at_ref,
    load_units,
    reconcile_dbt,
    reconcile_warehouse,
    render_airbyte,
    render_dagster_intervals,
    validate_inventory,
)
from ol_dbt_cli.lib.validation import Severity, ValidationIssue, ValidationReport
from ol_dbt_cli.lib.yaml_registry import collect_source_tables

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


RAW_SOURCE_NAME = "ol_warehouse_raw_data"


@inventory_app.command
def reconcile(
    *,
    inventory_dir: Annotated[
        Path,
        Parameter(name=["--inventory-dir", "-i"], help="Directory holding units/ and retired.yml."),
    ] = DEFAULT_INVENTORY_DIR,
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to the dbt project root. Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
    warehouse_tables_path: Annotated[
        Path | None,
        Parameter(
            name=["--warehouse-tables"],
            help="File of raw table names the warehouse holds — a JSON array or one name per line.",
        ),
    ] = None,
    duckdb_path: Annotated[
        Path | None,
        Parameter(
            name=["--duckdb-path"],
            help="Read the warehouse side from a local `ol-dbt local register` DuckDB instead.",
        ),
    ] = None,
    glue_database: Annotated[
        str | None,
        Parameter(name=["--glue-database"], help="Restrict the DuckDB registry read to one Glue database."),
    ] = None,
    output_format: Annotated[
        str,
        Parameter(name=["--format", "-f"], help="Output format: text (default) or json."),
    ] = "text",
) -> None:
    """Three-way diff: the inventory against the warehouse and against dbt sources.

    The inventory is the source of truth; these two are independent observations
    of it, and reconcile only ever reports their disagreement. Warehouse
    introspection used to *be* the truth for `ol-dbt generate sources`, which is
    the defect the inventory corrects — so nothing here writes to either side.

    The dbt half needs no credentials and always runs. The warehouse half needs
    an observation to compare against and is skipped without one, because a
    silent empty set would report every declared table as missing.
    """
    units = load_units(inventory_dir)
    report = ValidationReport()

    try:
        dbt_dir = Path(dbt_dir_path) if dbt_dir_path else get_repo_root() / "src" / "ol_dbt"
    except RuntimeError:
        dbt_dir = Path("src/ol_dbt").resolve()
    if not (dbt_dir / "dbt_project.yml").exists():
        err_console.print(f"[bold red]dbt project not found at {escape(str(dbt_dir))}. Use --dbt-dir.")
        sys.exit(1)

    dbt_tables = collect_source_tables(dbt_dir / "models", RAW_SOURCE_NAME)
    dbt_result = reconcile_dbt(units, dbt_tables, inventory_dir, report)

    if warehouse_tables_path and duckdb_path:
        err_console.print("[bold red]Pass either --warehouse-tables or --duckdb-path, not both.")
        sys.exit(1)
    if glue_database and not duckdb_path:
        err_console.print("[bold red]--glue-database only applies to --duckdb-path.")
        sys.exit(1)

    warehouse_result = None
    if warehouse_tables_path or duckdb_path:
        try:
            warehouse = (
                _read_table_list(warehouse_tables_path)
                if warehouse_tables_path
                else _warehouse_tables_from_duckdb(duckdb_path, glue_database)  # type: ignore[arg-type]
            )
        except (OSError, ValueError) as error:
            err_console.print(f"[bold red]{escape(str(error))}")
            sys.exit(1)
        # The same reason the half is skipped without a flag: an observation
        # holding no raw table at all is not evidence that every declared table
        # is missing. It is a registry pointed at a non-raw Glue database, or a
        # dump taken before the raw schema existed.
        if not warehouse:
            err_console.print(
                "[bold red]The warehouse observation holds no `raw__` table, so comparing it "
                "would report all "
                f"{len(units)} unit(s)' tables as missing. Check --glue-database, or that the "
                "table list is not empty."
            )
            sys.exit(1)
        warehouse_result = reconcile_warehouse(units, warehouse, report)

    if output_format == "json":
        _emit_json(report.issues)
    else:
        _emit_text(report.issues)
        console.print(
            f"\n[bold]dbt sources:[/] {len(dbt_result.both)} of {len(dbt_tables)} declared raw table(s) map to a unit."
        )
        if warehouse_result is None:
            console.print(
                "[dim]Warehouse not compared — pass --warehouse-tables or --duckdb-path "
                "to get the other two buckets.[/]"
            )
        else:
            console.print(
                f"[bold]Warehouse:[/] {len(warehouse_result.both)} in both, "
                f"{len(warehouse_result.declared_not_observed)} declared but absent, "
                f"{len(warehouse_result.observed_not_declared)} present but undeclared."
            )
        console.print(f"[bold]Summary:[/] {len(report.errors)} error(s), {len(report.warnings)} warning(s).")

    if report.errors:
        sys.exit(1)


@inventory_app.command
def drift(
    *,
    snapshot: Annotated[
        Path,
        Parameter(
            name=["--snapshot", "-s"],
            help="A workspace dump from `bin/airbyte-inventory.py dump`.",
        ),
    ] = Path("airbyte-snapshot.json"),
    inventory_dir: Annotated[
        Path,
        Parameter(name=["--inventory-dir", "-i"], help="Directory holding units/."),
    ] = DEFAULT_INVENTORY_DIR,
    output_format: Annotated[
        str,
        Parameter(name=["--format", "-f"], help="Output format: text (default) or json."),
    ] = "text",
) -> None:
    """Report every way the live Airbyte workspace differs from the inventory.

    Steps 5 and 6 were struck (§6.0), so Airbyte's configuration is
    hand-managed and nothing applies the inventory to it. This is what keeps
    the file honest in between: on a schedule, dump the workspace and diff it,
    because the divergence nothing else can catch is somebody editing a
    connection in the UI (§4).

    Takes a dump rather than reading the API, so the credentialed step stays
    separate and a drift report can be re-derived offline from a saved
    snapshot. Exits non-zero when the inventory is wrong about something it
    declares; a live connection or stream the inventory merely does not cover
    is a warning.
    """
    units = load_units(inventory_dir)
    if not snapshot.exists():
        err_console.print(
            f"[bold red]No snapshot at {escape(str(snapshot))}. Produce one with:\n"
            "  uv run python bin/airbyte-inventory.py dump --username dagster"
        )
        sys.exit(1)

    report = ValidationReport()
    try:
        check_drift(json.loads(snapshot.read_text()), units, report)
    except (json.JSONDecodeError, RenderError) as error:
        err_console.print(f"[bold red]{escape(str(error))}")
        sys.exit(1)

    if output_format == "json":
        _emit_json(report.issues)
    else:
        _emit_text(report.issues)
        live = len(json.loads(snapshot.read_text()).get("connections") or [])
        console.print(
            f"\n[bold]Summary:[/] compared {live} live connection(s) against "
            f"{len(units)} unit(s) — {len(report.errors)} error(s), "
            f"{len(report.warnings)} warning(s)."
        )

    if report.errors:
        sys.exit(1)


def _read_table_list(path: Path) -> set[str]:
    """Read raw table names from a JSON array or a newline-delimited list."""
    text = path.read_text()
    stripped = text.lstrip()
    if stripped.startswith("["):
        return {str(name) for name in json.loads(text)}
    lines = (line.strip() for line in text.splitlines())
    return {line for line in lines if line and not line.startswith("#")}


def _warehouse_tables_from_duckdb(duckdb_path: Path, glue_database: str | None) -> set[str]:
    """Read the registered Glue tables from a local `ol-dbt local register` DuckDB.

    Imported here rather than at module scope so that `validate`, `check-removals`
    and `render` — the three that CI runs — stay free of duckdb.
    """
    import duckdb  # noqa: PLC0415

    if not duckdb_path.exists():
        msg = f"No DuckDB database at {duckdb_path}. Run `ol-dbt local register` first."
        raise OSError(msg)
    query = "SELECT glue_table FROM _glue_source_registry"
    params: tuple[str, ...] = ()
    if glue_database:
        query += " WHERE glue_database = ?"
        params = (glue_database,)
    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        tables = {str(row[0]) for row in conn.execute(query, params).fetchall()}
    # `ol-dbt local register --all-layers` puts staging, intermediate and mart
    # tables in the same registry. Comparing those against the inventory would
    # report every modelled table as undeclared warehouse drift, burying the
    # real finding. The schema constrains every inventory raw table to `raw__`,
    # so that prefix is the layer boundary. Filtered here rather than in SQL
    # because `_` is a LIKE wildcard and the escaping earns nothing.
    return {table for table in tables if table.startswith("raw__")}


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
