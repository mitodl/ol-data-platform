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
from typing import Annotated, Any, cast

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.cursor_audit import Verdict, audit, select_units
from ol_dbt_cli.lib.git_utils import get_repo_root, resolve_merge_base
from ol_dbt_cli.lib.glue_schema import DEFAULT_GLUE_DATABASE, columns_by_table
from ol_dbt_cli.lib.inventory import (
    DEFAULT_INVENTORY_DIR,
    RenderError,
    Unit,
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


VERDICT_STYLE = {
    Verdict.CURSOR_MISSING: "bold red",
    Verdict.NOT_LANDED: "yellow",
    Verdict.INSERT_ONLY: "yellow",
    Verdict.REPLACE: "dim",
    Verdict.SECONDARY_AVAILABLE: "cyan",
    Verdict.CURSOR_AVAILABLE: "green",
    Verdict.CURSOR_OK: "dim green",
}


@inventory_app.command
def cursors(
    *,
    inventory_dir: Annotated[
        Path,
        Parameter(
            name=["--inventory-dir", "-i"],
            help="Directory holding vocabulary.yml, schema/ and units/.",
        ),
    ] = DEFAULT_INVENTORY_DIR,
    glue_database: Annotated[
        str,
        Parameter(help="Glue database holding the landed raw tables."),
    ] = DEFAULT_GLUE_DATABASE,
    region: str = "us-east-1",
    unit: Annotated[
        list[str] | None,
        Parameter(
            help="Limit to these units, as deployment/layer. Repeatable.",
            show_default=False,
        ),
    ] = None,
    attention_only: Annotated[
        bool,
        Parameter(
            help=(
                "Show only findings a human needs to decide: a declared cursor "
                "whose column vanished, an insert-only timestamp, or a wide "
                "table with no time column. Hides join tables and settled rows."
            ),
        ),
    ] = False,
    output_format: Annotated[
        str,
        Parameter(name=["--format", "-f"], help="Output format: text or json."),
    ] = "text",
) -> None:
    """Report which column each table could drive an incremental load from.

    Reads the LANDED schema from Glue, so it answers what a loader can actually
    key on rather than what an ORM declares. Needs AWS credentials; every other
    `inventory` subcommand is offline.

    Run it when an app is added or an app's schema changes. Adding a unit is
    enough to bring a new source into scope -- nothing here is hard-coded per
    app. Exits non-zero if any declared cursor_field names a column that is no
    longer in the landed schema, which is the failure worth gating on: the load
    does not error, it just quietly stops advancing.

    A `cursor_available` finding is a shortlist entry, NOT an approval. Whether
    the column is stamped on every mutation path is not answerable from a
    schema -- see the module docstring in lib/cursor_audit.py.

    --glue-database must name the environment the units describe. Point a
    production-rendered inventory at a QA catalogue (or the reverse) and every
    table it has not loaded there reports `not_landed`, which reads like a
    finding and is really a mismatched argument.
    """
    units = load_units(inventory_dir)
    if unit:
        selected, missing = select_units(units, unit)
        if missing:
            err_console.print(f"[bold red]No unit matched: {', '.join(missing)}")
            sys.exit(1)
        units = cast("list[Unit]", selected)

    prefixes = sorted({str(u.data["table_prefix"]) for u in units if u.data.get("table_prefix")})
    columns = columns_by_table(glue_database, prefixes=prefixes, region=region)
    result = audit(units, columns)

    findings = [f for f in result.findings if not attention_only or f.needs_attention]

    if output_format == "json":
        print(  # noqa: T201
            json.dumps(
                [
                    {
                        "unit": f.unit_key,
                        "raw_table": f.raw_table,
                        "stream": f.stream,
                        "verdict": f.verdict.value,
                        "declared_cursor": f.declared_cursor,
                        "candidate": f.candidate,
                        "source_columns": f.source_columns,
                        "time_like_columns": list(f.time_like_columns),
                    }
                    for f in findings
                ],
                indent=2,
            )
        )
    else:
        for finding in findings:
            style = VERDICT_STYLE[finding.verdict]
            candidate = f" -> {finding.candidate}" if finding.candidate else ""
            console.print(
                f"[{style}]{finding.verdict.value:<19}[/] "
                f"{escape(finding.unit_key)}  {escape(finding.stream)}"
                f"{escape(candidate)}"
            )
        console.print(
            "\n[bold]Summary:[/] "
            + ", ".join(f"{count} {verdict.value}" for verdict, count in result.counts.items() if count)
        )
        if result.broken:
            console.print(
                f"\n[bold red]{len(result.broken)} declared cursor_field(s) name a "
                f"column that is no longer in the landed schema.[/] An incremental "
                f"load on one of these does not fail - it stops advancing."
            )

    if result.broken:
        sys.exit(1)
