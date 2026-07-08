"""Diff command — field-level QA safeguard for dimensional-layer model migrations.

Wraps dbt-labs' `audit_helper` package (`compare_row_counts` / `compare_relations`) to
compare a model's pre-migration output against its migrated replacement with full
row/column precision, as an additional safeguard before deploying migrations tracked
under epic #2072 (see https://github.com/mitodl/ol-data-platform/issues/2407).

Both relations must already be built for the chosen `--target` before running this
command — it only performs the comparison. Building against `dev_local` (DuckDB) or
`dev_production` (Trino) means both models read the *same* production-backed source
data with no manual data copying (see `ol-dbt local register`).

Usage examples::

    ol-dbt run --select old_model new_model --target dev_local
    ol-dbt diff --old old_model --new new_model
    ol-dbt diff --old old_model --new new_model --primary-key user_id --target dev_production
    ol-dbt diff --old old_model --new new_model --exclude-columns updated_at,loaded_at
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated

import cyclopts
from cyclopts import Parameter
from rich.console import Console
from rich.table import Table

from ol_dbt_cli.lib.git_utils import get_repo_root

console = Console()
err_console = Console(stderr=True)

diff_app = cyclopts.App(
    name="diff",
    help="Compare a migrated model's output against its pre-migration source for field-level QA.",
)

DEFAULT_MATCH_THRESHOLD = 100.0
TRUE_STRINGS = {"true", "t", "1", "yes"}


class DiffError(RuntimeError):
    """Raised when dbt output can't be parsed into diff results, or the dbt run fails."""


def _as_float(value: object) -> float | None:
    """Safely coerce a dbt JSON scalar (int, float, or numeric string) to float."""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def is_true(value: object) -> bool:
    """Normalize a boolean-ish value (Python bool, int, or string) from dbt JSON output."""
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value == 1
    if isinstance(value, str):
        return value.strip().lower() in TRUE_STRINGS
    return False


def find_dbt_dir(project_dir: str | None) -> Path:
    """Resolve the dbt project root directory."""
    if project_dir:
        return Path(project_dir).resolve()
    try:
        repo_root = get_repo_root()
        candidate = repo_root / "src" / "ol_dbt"
        if (candidate / "dbt_project.yml").exists():
            return candidate
    except RuntimeError:
        pass
    fallback = Path("src/ol_dbt").resolve()
    if (fallback / "dbt_project.yml").exists():
        return fallback
    msg = "dbt project not found. Pass --project-dir or run from the repo root."
    raise RuntimeError(msg)


def parse_relation_identifier(identifier: str) -> tuple[str | None, str, str]:
    """Parse a ``schema.table`` or ``database.schema.table`` string.

    Returns (database, schema, identifier); database is None for the 2-part form.
    """
    parts = identifier.split(".")
    if len(parts) == 2:
        return None, parts[0], parts[1]
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    msg = f"Relation identifier must be 'schema.table' or 'database.schema.table', got: {identifier!r}"
    raise ValueError(msg)


def build_relation_expr(model: str | None, relation: str | None) -> str:
    """Return a Jinja expression that resolves to a dbt Relation for one side of the diff.

    Pass `model` to resolve via `ref()` (use when both sides are distinct models that
    exist together in the current manifest). Pass `relation` (a `schema.table` or
    `database.schema.table` string) to resolve an arbitrary already-built table directly
    via `adapter.get_relation()` — this is what you need when comparing two builds of the
    *same* model name from different git branches/schemas, since `ref()` can only resolve
    one physical relation per model name per dbt invocation.
    """
    if (model is None) == (relation is None):
        msg = "Exactly one of a model name or a relation identifier must be given."
        raise ValueError(msg)
    if model is not None:
        return f"ref('{model}')"
    database, schema, identifier = parse_relation_identifier(relation)  # type: ignore[arg-type]
    if database:
        return f"adapter.get_relation(database='{database}', schema='{schema}', identifier='{identifier}')"
    return f"adapter.get_relation(database=target.database, schema='{schema}', identifier='{identifier}')"


def build_row_count_inline_sql(a_expr: str, b_expr: str) -> str:
    """Build the audit_helper.compare_row_counts() Jinja call for the given relation expressions."""
    return f"{{{{ audit_helper.compare_row_counts(a_relation={a_expr}, b_relation={b_expr}) }}}}"


def build_compare_relations_inline_sql(
    a_expr: str,
    b_expr: str,
    primary_key: str | None = None,
    exclude_columns: list[str] | None = None,
) -> str:
    """Build the audit_helper.compare_relations() Jinja call for the given relation expressions."""
    args = [f"a_relation={a_expr}", f"b_relation={b_expr}"]
    if primary_key:
        args.append(f"primary_key='{primary_key}'")
    if exclude_columns:
        quoted = ", ".join(f"'{col}'" for col in exclude_columns)
        args.append(f"exclude_columns=[{quoted}]")
    return f"{{{{ audit_helper.compare_relations({', '.join(args)}) }}}}"


def build_dbt_show_command(
    inline_sql: str,
    profiles_dir: Path,
    target: str | None = None,
    vars_arg: str | None = None,
    limit: int = -1,
) -> list[str]:
    """Construct the `dbt show --inline ...` CLI command list."""
    cmd: list[str] = [
        "dbt",
        "show",
        "--inline",
        inline_sql,
        "--profiles-dir",
        str(profiles_dir),
        "--limit",
        str(limit),
        "--output",
        "json",
    ]
    if target:
        cmd += ["--target", target]
    if vars_arg:
        cmd += ["--vars", vars_arg]
    return cmd


def parse_dbt_show_json(stdout: str) -> list[dict[str, object]]:
    """Extract the row records from `dbt show --output json` stdout.

    dbt interleaves regular log lines with the final JSON payload on stdout; this scans
    lines from the bottom up for the first one that parses as JSON and contains a
    "show" key, per the documented `dbt show --output json` schema.
    """
    for line in reversed(stdout.splitlines()):
        stripped = line.strip()
        if not stripped.startswith("{"):
            continue
        try:
            payload = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and "show" in payload:
            rows = payload["show"]
            if isinstance(rows, list):
                return rows
    msg = "Could not find a `show` payload in dbt output. Was --output json passed and did the command succeed?"
    raise DiffError(msg)


@dataclass
class RelationsCompareResult:
    """Structured result of an `audit_helper.compare_relations()` run (summarize=true)."""

    rows: list[dict[str, object]] = field(default_factory=list)

    @property
    def percent_matched(self) -> float | None:
        """Percent of rows present in both relations (identical across all compared columns)."""
        for row in self.rows:
            if is_true(row.get("in_a")) and is_true(row.get("in_b")):
                return _as_float(row.get("percent_of_total"))
        return None

    @property
    def only_in_a_percent(self) -> float | None:
        """Percent of rows only present in the old (`a`) relation."""
        return self._percent_for(in_a=True, in_b=False)

    @property
    def only_in_b_percent(self) -> float | None:
        """Percent of rows only present in the new (`b`) relation."""
        return self._percent_for(in_a=False, in_b=True)

    def _percent_for(self, *, in_a: bool, in_b: bool) -> float | None:
        for row in self.rows:
            if is_true(row.get("in_a")) == in_a and is_true(row.get("in_b")) == in_b:
                return _as_float(row.get("percent_of_total"))
        return None


def evaluate_compare_result(
    rows: list[dict[str, object]], threshold: float = DEFAULT_MATCH_THRESHOLD
) -> tuple[bool, float | None]:
    """Return (passed, percent_matched) given `compare_relations()` result rows."""
    result = RelationsCompareResult(rows=rows)
    percent = result.percent_matched
    if percent is None:
        return False, None
    return percent >= threshold, percent


def _run_dbt_show(cmd: list[str], cwd: Path) -> str:
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True)  # noqa: S603
    if result.returncode != 0:
        err_console.print(result.stdout)
        err_console.print(result.stderr)
        msg = f"dbt show failed with exit code {result.returncode}"
        raise DiffError(msg)
    return result.stdout


def _render_rows(title: str, rows: list[dict[str, object]]) -> None:
    if not rows:
        console.print(f"[yellow]{title}: no rows returned[/]")
        return
    table = Table(title=title)
    for key in rows[0]:
        table.add_column(str(key))
    for row in rows:
        table.add_row(*(str(v) for v in row.values()))
    console.print(table)


@diff_app.default
def diff(  # noqa: PLR0913
    *,
    old: Annotated[
        str | None,
        Parameter(
            name=["--old"],
            help="Pre-migration model name (resolved via ref()). Use when old and new are distinct model names "
            "that both exist in the current manifest. Mutually exclusive with --old-relation.",
        ),
    ] = None,
    new: Annotated[
        str | None,
        Parameter(
            name=["--new"],
            help="Migrated model name (resolved via ref()). Mutually exclusive with --new-relation.",
        ),
    ] = None,
    old_relation: Annotated[
        str | None,
        Parameter(
            name="--old-relation",
            help="Pre-migration table as 'schema.table' or 'database.schema.table', resolved directly via "
            "adapter.get_relation() instead of ref(). Use this when old and new share the same model name "
            "but were built from different git branches into different schemas (the common migration-QA case). "
            "Mutually exclusive with --old.",
        ),
    ] = None,
    new_relation: Annotated[
        str | None,
        Parameter(
            name="--new-relation",
            help="Migrated table as 'schema.table' or 'database.schema.table'. Mutually exclusive with --new.",
        ),
    ] = None,
    primary_key: Annotated[
        str | None,
        Parameter(
            name=["--primary-key", "-k"],
            help="Primary key column, used to order detailed row-level output if you inspect a mismatch manually.",
        ),
    ] = None,
    exclude_columns: Annotated[
        str | None,
        Parameter(
            name="--exclude-columns",
            help="Comma-separated columns to exclude from comparison (e.g. volatile audit timestamps).",
        ),
    ] = None,
    target: Annotated[
        str | None,
        Parameter(name=["--target", "-t"], help="dbt target to compare against (e.g. dev_local, dev_production)."),
    ] = None,
    threshold: Annotated[
        float,
        Parameter(name="--threshold", help="Minimum percent of matching rows required to pass. Default: 100.0."),
    ] = DEFAULT_MATCH_THRESHOLD,
    vars: Annotated[
        str | None,
        Parameter(name="--vars", help='dbt variables as a YAML/JSON string, e.g. \'{"schema_suffix": "alice"}\'.'),
    ] = None,
    project_dir: Annotated[
        str | None,
        Parameter(name="--project-dir", help="Path to the dbt project root. Defaults to src/ol_dbt."),
    ] = None,
) -> None:
    """Compare a pre-migration relation against its migrated replacement, field-by-field.

    Two ways to point at the "old" and "new" sides:

    1. `--old model_a --new model_b` — two distinct model names that both exist in the
       current manifest (e.g. you kept the old model around under a different name).
    2. `--old-relation schema.table --new-relation schema.table` — two already-built
       tables identified directly by schema/table, regardless of model name. This is
       what you want for the typical case: the model name doesn't change during a
       migration, only its SQL — so you build the pre-migration version from `main`
       into one schema and the migrated version from your branch into another, then
       point this command at both schemas directly.

    Either mode requires both models/relations to already be built for the chosen
    `--target` (this command only performs the comparison). Exits non-zero if the match
    percentage falls below `--threshold`, so it can be used as a CI/PR gate.
    """
    try:
        a_expr = build_relation_expr(old, old_relation)
    except ValueError as exc:
        err_console.print(f"[red]Error (--old/--old-relation):[/] {exc}")
        sys.exit(1)
    try:
        b_expr = build_relation_expr(new, new_relation)
    except ValueError as exc:
        err_console.print(f"[red]Error (--new/--new-relation):[/] {exc}")
        sys.exit(1)

    try:
        dbt_dir = find_dbt_dir(project_dir)
    except RuntimeError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    old_label = old or old_relation
    new_label = new or new_relation
    console.print(f"\n[bold]ol-dbt diff[/] — comparing [cyan]{old_label}[/] (old) vs [cyan]{new_label}[/] (new)")

    row_count_cmd = build_dbt_show_command(build_row_count_inline_sql(a_expr, b_expr), dbt_dir, target, vars)
    try:
        row_count_rows = parse_dbt_show_json(_run_dbt_show(row_count_cmd, dbt_dir))
    except DiffError as exc:
        err_console.print(f"[red]Error running row-count comparison:[/] {exc}")
        sys.exit(1)
    _render_rows("Row counts", row_count_rows)

    exclude_list = [c.strip() for c in exclude_columns.split(",")] if exclude_columns else None
    compare_cmd = build_dbt_show_command(
        build_compare_relations_inline_sql(a_expr, b_expr, primary_key, exclude_list), dbt_dir, target, vars
    )
    try:
        compare_rows = parse_dbt_show_json(_run_dbt_show(compare_cmd, dbt_dir))
    except DiffError as exc:
        err_console.print(f"[red]Error running relation comparison:[/] {exc}")
        sys.exit(1)
    _render_rows("Relation comparison (audit_helper.compare_relations)", compare_rows)

    passed, percent = evaluate_compare_result(compare_rows, threshold)
    if percent is None:
        err_console.print("[red]Could not determine match percentage from compare_relations output.[/]")
        sys.exit(1)

    if passed:
        console.print(f"\n[green]✓ PASS[/] — {percent:.4f}% of rows matched (threshold: {threshold}%)")
        sys.exit(0)
    else:
        console.print(f"\n[red]✗ FAIL[/] — only {percent:.4f}% of rows matched (threshold: {threshold}%)")
        sys.exit(1)
