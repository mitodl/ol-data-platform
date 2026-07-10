"""Diff command — same-engine row/column comparison of two model relations.

Wraps the ``dbt_audit_helper`` package to compare a baseline model against a
candidate model (typically an old vs a migrated mart/reporting model). Because
both relations are built on the *same* engine (default target ``dev_local``, a
local DuckDB reading Iceberg directly), dialect differences cancel and the diff
reflects real data divergence rather than SQL-translation noise.

Design notes (the non-obvious bits):

* Column sets are reconciled BEFORE any comparison runs, so a schema mismatch
  produces a clear structured message instead of a raw SQL error from
  audit_helper. The diff proceeds on the intersection minus ``--exclude-columns``.
* Known non-determinism (e.g. ``dim_user.user_pk`` is an email-keyed surrogate
  that collapses NULL emails) is handled by passing ``--primary-key`` and/or
  ``--exclude-columns``.
* The comparison itself is run via ``dbt show --inline`` invoking audit_helper
  macros with ``--output json``; results are parsed tolerantly from stdout.

Driveable both by hand and by CI (Phase 2 auto-diff vs a base ref). See
docs/specs/DBT_WAREHOUSE_CI_QA_SPEC.md §3.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.manifest import (
    ManifestRegistry,
    find_manifest,
    load_manifest,
)
from ol_dbt_cli.lib.sql_parser import (
    ParsedModel,
    find_compiled_dir,
    parse_model_file,
)
from ol_dbt_cli.lib.yaml_registry import YamlRegistry, build_yaml_registry

console = Console()
err_console = Console(stderr=True)

# Cap on how many columns we run a per-column value comparison for, to bound the
# number of dbt invocations. When exceeded we log it (never silently truncate).
_MAX_PER_COLUMN_COMPARISONS = 25

# Model and column names are interpolated into inline Jinja SQL passed to
# `dbt show --inline`, so they must be plain dbt identifiers — anything else
# (quotes, braces, whitespace) could inject Jinja/SQL and is rejected up front.
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class InvalidIdentifierError(ValueError):
    """Raised when a user-supplied name is not a safe dbt identifier."""


def _validate_identifiers(kind: str, names: list[str]) -> None:
    """Reject any *names* that are not plain dbt identifiers.

    Guards against Jinja/SQL injection through the inline template built for
    ``dbt show``. Raises :class:`InvalidIdentifierError` naming the offenders.
    """
    bad = [n for n in names if not _IDENTIFIER_RE.match(n)]
    if bad:
        msg = f"Invalid {kind}: {', '.join(repr(b) for b in bad)}. Expected plain identifiers ([A-Za-z_][A-Za-z0-9_]*)."
        raise InvalidIdentifierError(msg)


class Verdict(StrEnum):
    MATCH = "match"
    MISMATCH = "mismatch"
    SCHEMA_DIVERGENCE = "schema_divergence"


@dataclass
class ColumnReconciliation:
    only_in_old: list[str] = field(default_factory=list)
    only_in_new: list[str] = field(default_factory=list)
    compared: list[str] = field(default_factory=list)

    @property
    def diverged(self) -> bool:
        return bool(self.only_in_old or self.only_in_new)


@dataclass
class ColumnMismatch:
    column: str
    mismatch_rate: float
    mismatched_rows: int


@dataclass
class DiffResult:
    old: str
    new: str
    target: str
    primary_key: list[str]
    excluded_columns: list[str]
    column_reconciliation: ColumnReconciliation
    row_counts: dict[str, int]  # {"old": .., "new": .., "delta": ..}
    column_mismatches: list[ColumnMismatch]
    sample_mismatches: list[dict[str, Any]]
    verdict: Verdict
    notes: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Column reconciliation
# ---------------------------------------------------------------------------


def _resolve_columns(
    name: str,
    sql_models_by_name: dict[str, ParsedModel],
    manifest: ManifestRegistry | None,
    yaml_registry: YamlRegistry,
) -> set[str]:
    """Best-effort resolution of a model's output columns.

    Prefers parsed (compiled) SQL output columns, then manifest column metadata,
    then the YAML schema. Column names are lower-cased for a case-insensitive
    comparison (dbt/DuckDB fold unquoted identifiers to lower case).
    """
    parsed = sql_models_by_name.get(name)
    if parsed is not None and parsed.output_columns:
        return {c.lower() for c in parsed.output_columns}
    if manifest is not None:
        model = manifest.get_model(name)
        if model is not None and model.column_names:
            return {c.lower() for c in model.column_names}
    yaml_model = yaml_registry.get_model(name)
    if yaml_model is not None and yaml_model.column_names:
        return {c.lower() for c in yaml_model.column_names}
    return set()


def reconcile_columns(
    old_cols: set[str],
    new_cols: set[str],
    exclude_columns: set[str],
) -> ColumnReconciliation:
    """Reconcile two column sets, returning the divergence and the compared set.

    The compared set is the intersection minus excluded columns. Excluded
    columns are dropped from the only_in_* lists too — an intentional exclusion
    is not a divergence.
    """
    exclude = {c.lower() for c in exclude_columns}
    old = {c.lower() for c in old_cols if c.lower() not in exclude}
    new = {c.lower() for c in new_cols if c.lower() not in exclude}
    return ColumnReconciliation(
        only_in_old=sorted(old - new),
        only_in_new=sorted(new - old),
        compared=sorted(old & new),
    )


# ---------------------------------------------------------------------------
# dbt show / audit_helper invocation
# ---------------------------------------------------------------------------


def _extract_show_rows(stdout: str) -> list[dict[str, Any]]:
    """Extract the row list from ``dbt show --output json`` stdout.

    dbt interleaves log lines with a JSON document on stdout. The document is
    either an object carrying a ``show`` key whose value is the list of row dicts,
    or a bare list. We parse tolerantly: try the whole payload first, then fall
    back to scanning for a JSON value (object or array) embedded in the output.
    """

    def _rows_from(obj: Any) -> list[dict[str, Any]] | None:
        if isinstance(obj, dict) and isinstance(obj.get("show"), list):
            return obj["show"]
        if isinstance(obj, list):
            return obj
        return None

    stripped = stdout.strip()
    if stripped:
        try:
            rows = _rows_from(json.loads(stripped))
            if rows is not None:
                return rows
        except json.JSONDecodeError:
            pass

    # Fall back: scan for a JSON document (object OR array) embedded among log
    # lines. raw_decode parses one value from the offset and ignores trailing text.
    decoder = json.JSONDecoder()
    for idx, char in enumerate(stdout):
        if char not in "{[":
            continue
        try:
            obj, _ = decoder.raw_decode(stdout[idx:])
        except json.JSONDecodeError:
            continue
        rows = _rows_from(obj)
        if rows is not None:
            return rows
    return []


def _run_dbt_show(
    inline_sql: str,
    dbt_dir: Path,
    target: str,
    limit: int,
) -> list[dict[str, Any]]:
    """Run ``dbt show --inline`` and return the parsed rows.

    Raises RuntimeError on a dbt failure or a missing dbt binary so the caller
    can surface a clean error and exit non-zero.
    """
    cmd = [
        "dbt",
        "show",
        "--inline",
        inline_sql,
        "--target",
        target,
        "--output",
        "json",
        "--limit",
        str(limit),
    ]
    try:
        result = subprocess.run(  # noqa: S603, S607
            cmd,
            cwd=str(dbt_dir),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or str(exc)).strip()
        msg = f"dbt show failed: {detail[-500:]}"
        raise RuntimeError(msg) from exc
    except FileNotFoundError as exc:
        msg = "'dbt' command not found; install dbt and ensure it is on PATH."
        raise RuntimeError(msg) from exc
    return _extract_show_rows(result.stdout)


def _jinja_list(values: list[str]) -> str:
    """Render a Python list of identifiers as a Jinja list literal."""
    inner = ", ".join(f"'{v}'" for v in values)
    return f"[{inner}]"


def _compare_relations_sql(
    old: str,
    new: str,
    primary_key: list[str],
    exclude_columns: list[str],
    *,
    summarize: bool,
) -> str:
    """Build inline SQL calling ``audit_helper.compare_relations``.

    With ``summarize=True`` audit_helper returns the ``in_a``/``in_b``/``count``/
    ``percent_of_total`` grouping; with ``summarize=False`` it returns the actual
    differing rows (used to sample mismatches).
    """
    pk_arg = ""
    if primary_key:
        pk_repr = f"'{primary_key[0]}'" if len(primary_key) == 1 else _jinja_list(primary_key)
        pk_arg = f", primary_key={pk_repr}"
    exclude_arg = f", exclude_columns={_jinja_list(exclude_columns)}" if exclude_columns else ""
    return (
        "{{ audit_helper.compare_relations("
        f"a_relation=ref('{old}'), b_relation=ref('{new}')"
        f"{exclude_arg}{pk_arg}, summarize={'true' if summarize else 'false'}) }}"
    )


def _compare_column_sql(old: str, new: str, primary_key: list[str], column: str) -> str:
    """Build inline SQL calling ``audit_helper.compare_column_values`` for one column."""
    pk = f"'{primary_key[0]}'" if len(primary_key) == 1 else _jinja_list(primary_key)
    # Model names are dbt identifiers rendered into a Jinja template compiled by
    # dbt (not executed as a raw query here), so the S608 heuristic is a false positive.
    return (
        "{{ audit_helper.compare_column_values("  # noqa: S608
        f"a_query=\"select * from {{{{ ref('{old}') }}}}\", "
        f"b_query=\"select * from {{{{ ref('{new}') }}}}\", "
        f"primary_key={pk}, column_to_compare='{column}') }}"
    )


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _summarize_relations(rows: list[dict[str, Any]]) -> tuple[dict[str, int], int]:
    """Turn ``compare_relations(summarize=true)`` rows into row counts + mismatch count.

    Each row is grouped by (in_a, in_b) with a ``count``. Rows in A = sum of
    counts where in_a is true; likewise B. Mismatched rows = rows present in only
    one relation.
    """
    old_rows = new_rows = mismatched = 0
    for row in rows:
        in_a = bool(row.get("in_a"))
        in_b = bool(row.get("in_b"))
        count = _int(row.get("count"))
        if in_a:
            old_rows += count
        if in_b:
            new_rows += count
        if in_a != in_b:
            mismatched += count
    counts = {"old": old_rows, "new": new_rows, "delta": new_rows - old_rows}
    return counts, mismatched


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------


def _result_to_dict(result: DiffResult) -> dict[str, Any]:
    return {
        "old": result.old,
        "new": result.new,
        "target": result.target,
        "primary_key": result.primary_key,
        "excluded_columns": result.excluded_columns,
        "column_reconciliation": {
            "only_in_old": result.column_reconciliation.only_in_old,
            "only_in_new": result.column_reconciliation.only_in_new,
            "compared": result.column_reconciliation.compared,
        },
        "row_counts": result.row_counts,
        "column_mismatches": [
            {"column": c.column, "mismatch_rate": c.mismatch_rate, "mismatched_rows": c.mismatched_rows}
            for c in result.column_mismatches
        ],
        "sample_mismatches": result.sample_mismatches,
        "verdict": result.verdict.value,
        "notes": result.notes,
    }


def _print_json(result: DiffResult) -> None:
    print(json.dumps(_result_to_dict(result), indent=2))


def _print_text(result: DiffResult) -> None:
    color = {
        Verdict.MATCH: "bold green",
        Verdict.MISMATCH: "bold red",
        Verdict.SCHEMA_DIVERGENCE: "bold yellow",
    }[result.verdict]
    console.print(
        f"\n[{color}]{result.verdict.value.upper()}[/]  "
        f"[bold]{result.old}[/] → [bold]{result.new}[/]  ([dim]{result.target}[/])"
    )

    recon = result.column_reconciliation
    if recon.diverged:
        if recon.only_in_old:
            console.print(f"   [yellow]Only in {result.old}:[/] {', '.join(recon.only_in_old)}")
        if recon.only_in_new:
            console.print(f"   [yellow]Only in {result.new}:[/] {', '.join(recon.only_in_new)}")
    console.print(f"   [bold]Compared columns:[/] {len(recon.compared)}")

    rc = result.row_counts
    delta = rc.get("delta", 0)
    delta_str = f"[green]{delta}[/]" if delta == 0 else f"[red]{delta:+d}[/]"
    console.print(
        f"   [bold]Row counts:[/] {result.old}={rc.get('old', 0)}  {result.new}={rc.get('new', 0)}  Δ={delta_str}"
    )

    if result.column_mismatches:
        console.print("   [bold]Column mismatches:[/]")
        for c in result.column_mismatches:
            console.print(f"     • {c.column}: {c.mismatch_rate:.2%} ({c.mismatched_rows} rows)")

    if result.sample_mismatches:
        console.print(f"   [bold]Sample mismatched rows[/] (first {len(result.sample_mismatches)}):")
        for row in result.sample_mismatches:
            console.print(f"     {json.dumps(row, default=str)}")

    for note in result.notes:
        console.print(f"   [dim]{note}[/]")

    console.print(
        f"\n[bold]Summary:[/] {result.verdict.value} — "
        f"row Δ {delta}, {len(result.column_mismatches)} column mismatch(es)."
    )


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def diff(
    old: Annotated[
        str,
        Parameter(name=["--old"], help="Baseline model name (the 'before' relation)."),
    ],
    new: Annotated[
        str,
        Parameter(name=["--new"], help="Candidate model name (the 'after' relation)."),
    ],
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to dbt project root (contains dbt_project.yml). Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
    target: Annotated[
        str,
        Parameter(
            name=["--target", "-t"],
            help=(
                "dbt target to build/compare on (default: dev_local). dev_local uses a local "
                "DuckDB instance reading Iceberg directly and requires no network access; both "
                "sides build on the same engine so dialect differences cancel."
            ),
        ),
    ] = "dev_local",
    primary_key: Annotated[
        tuple[str, ...],
        Parameter(
            name=["--primary-key", "-k"],
            help=(
                "Column(s) uniquely identifying a row, used as the comparison join key. "
                "Repeatable. Required for per-column mismatch rates. Use this for models with "
                "known surrogate-key non-determinism (e.g. dim_user.user_pk email-keyed collapse)."
            ),
        ),
    ] = (),
    exclude_columns: Annotated[
        tuple[str, ...],
        Parameter(
            name=["--exclude-columns"],
            help="Column(s) to exclude from the comparison (e.g. non-deterministic load timestamps). Repeatable.",
        ),
    ] = (),
    output_format: Annotated[
        Literal["text", "json"],
        Parameter(name=["--format", "-f"], help="Output format: text (default) or json."),
    ] = "text",
    limit: Annotated[
        int,
        Parameter(name=["--limit"], help="Cap on sample mismatched rows shown (default: 20)."),
    ] = 20,
    auto_build: Annotated[
        bool,
        Parameter(
            name=["--auto-build"],
            help="Run 'dbt build' on both models (on --target) before comparing, so both relations exist.",
        ),
    ] = False,
) -> None:
    """Diff two dbt model relations (same-engine row/column comparison).

    Reconciles column sets first (so a schema mismatch is reported clearly rather
    than as a raw SQL error), then runs a dbt_audit_helper comparison on the
    intersection minus --exclude-columns. Exits non-zero on any divergence, so it
    is gate-able in CI.

    Examples:
        Compare an old mart against its migrated replacement on local DuckDB:
            ol-dbt diff --old dim_user_old --new dim_user --primary-key user_pk

        Exclude a non-deterministic column and emit JSON for CI:
            ol-dbt diff --old m_old --new m_new -k id --exclude-columns _loaded_at --format json

        Build both sides first, then compare:
            ol-dbt diff --old m_old --new m_new --auto-build

    """
    primary_key_list = list(primary_key)
    exclude_list = list(exclude_columns)
    notes: list[str] = []

    # Validate every value interpolated into inline Jinja SQL before use.
    try:
        _validate_identifiers("model name", [old, new])
        _validate_identifiers("primary key", primary_key_list)
        _validate_identifiers("excluded column", exclude_list)
    except InvalidIdentifierError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    # Resolve dbt project directory (shared preamble with impact/validate).
    if dbt_dir_path:
        dbt_dir = Path(dbt_dir_path).resolve()
    else:
        try:
            dbt_dir = get_repo_root() / "src" / "ol_dbt"
        except RuntimeError:
            dbt_dir = Path("src/ol_dbt").resolve()

    if not (dbt_dir / "dbt_project.yml").exists():
        err_console.print(f"[red]Error:[/] dbt project not found at {dbt_dir}")
        err_console.print("  Use --dbt-dir to specify the path.")
        sys.exit(1)

    models_dir = dbt_dir / "models"

    # Build the metadata sources used for column reconciliation.
    compiled_dir = find_compiled_dir(dbt_dir)
    manifest: ManifestRegistry | None = None
    manifest_path = find_manifest(dbt_dir)
    if manifest_path:
        try:
            manifest = load_manifest(manifest_path)
        except Exception as exc:  # noqa: BLE001
            notes.append(f"Could not load manifest ({exc}); reconciling columns from SQL/YAML only.")

    yaml_registry = build_yaml_registry(models_dir)
    sql_models_by_name: dict[str, ParsedModel] = {}
    for sql_file in models_dir.rglob("*.sql"):
        try:
            sql_models_by_name[sql_file.stem] = parse_model_file(sql_file, compiled_dir=compiled_dir)
        except Exception:  # noqa: BLE001, S112
            continue

    # --- Column reconciliation (before any comparison) ---
    old_cols = _resolve_columns(old, sql_models_by_name, manifest, yaml_registry)
    new_cols = _resolve_columns(new, sql_models_by_name, manifest, yaml_registry)
    unresolved_note = "Column set for '{}' could not be resolved (unparsed / not in manifest); reconciliation skipped."
    if not old_cols:
        notes.append(unresolved_note.format(old))
    if not new_cols:
        notes.append(unresolved_note.format(new))
    recon = reconcile_columns(old_cols, new_cols, set(exclude_list))

    # Optionally build both relations first.
    if auto_build:
        # Single space-joined --select value, consistent with ol-dbt run/impact.
        build_cmd = ["dbt", "build", "--target", target, "--select", f"{old} {new}"]
        if output_format == "text":
            console.print(f"[dim]Running: {' '.join(build_cmd)} ...[/]")
        try:
            subprocess.run(build_cmd, cwd=str(dbt_dir), capture_output=True, text=True, check=True)  # noqa: S603, S607
        except subprocess.CalledProcessError as exc:
            # dbt often logs useful detail to stdout, not stderr — prefer either.
            detail = (exc.stderr or exc.stdout or str(exc)).strip()
            err_console.print(f"[red]Error:[/] dbt build failed: {detail[-500:]}")
            sys.exit(1)
        except FileNotFoundError:
            err_console.print("[red]Error:[/] 'dbt' command not found; install dbt and ensure it is on PATH.")
            sys.exit(1)

    # --- Comparison ---
    row_counts: dict[str, int] = {"old": 0, "new": 0, "delta": 0}
    column_mismatches: list[ColumnMismatch] = []
    sample_mismatches: list[dict[str, Any]] = []
    mismatched_rows = 0

    # If the resolved column sets diverge, a value comparison would fail on
    # mismatched columns — report the divergence clearly and skip it.
    if recon.diverged:
        notes.append("Column schemas diverge; skipping value comparison. Reconcile columns or use --exclude-columns.")
        _emit_and_exit(
            DiffResult(
                old=old,
                new=new,
                target=target,
                primary_key=primary_key_list,
                excluded_columns=exclude_list,
                column_reconciliation=recon,
                row_counts=row_counts,
                column_mismatches=column_mismatches,
                sample_mismatches=sample_mismatches,
                verdict=Verdict.SCHEMA_DIVERGENCE,
                notes=notes,
            ),
            output_format,
        )

    try:
        summary_rows = _run_dbt_show(
            _compare_relations_sql(old, new, primary_key_list, exclude_list, summarize=True),
            dbt_dir,
            target,
            limit=1000,
        )
        row_counts, mismatched_rows = _summarize_relations(summary_rows)

        if mismatched_rows:
            sample_mismatches = _run_dbt_show(
                _compare_relations_sql(old, new, primary_key_list, exclude_list, summarize=False),
                dbt_dir,
                target,
                limit=limit,
            )

        # Per-column mismatch rates require a primary key.
        if primary_key_list and recon.compared:
            cols = recon.compared
            if len(cols) > _MAX_PER_COLUMN_COMPARISONS:
                notes.append(f"Per-column comparison capped at {_MAX_PER_COLUMN_COMPARISONS} of {len(cols)} columns.")
                cols = cols[:_MAX_PER_COLUMN_COMPARISONS]
            for col in cols:
                mismatch = _compare_single_column(old, new, primary_key_list, col, dbt_dir, target)
                if mismatch is not None and mismatch.mismatched_rows > 0:
                    column_mismatches.append(mismatch)
        elif not primary_key_list:
            notes.append("Per-column mismatch rates require --primary-key; skipped.")
    except RuntimeError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    # --- Verdict ---
    if row_counts["delta"] != 0 or mismatched_rows > 0 or column_mismatches:
        verdict = Verdict.MISMATCH
    else:
        verdict = Verdict.MATCH

    _emit_and_exit(
        DiffResult(
            old=old,
            new=new,
            target=target,
            primary_key=primary_key_list,
            excluded_columns=exclude_list,
            column_reconciliation=recon,
            row_counts=row_counts,
            column_mismatches=column_mismatches,
            sample_mismatches=sample_mismatches,
            verdict=verdict,
            notes=notes,
        ),
        output_format,
    )


def _emit_and_exit(result: DiffResult, output_format: str) -> None:
    """Render the result in the requested format and exit non-zero on divergence."""
    if output_format == "json":
        _print_json(result)
    else:
        _print_text(result)
    if result.verdict != Verdict.MATCH:
        sys.exit(1)


def _compare_single_column(
    old: str,
    new: str,
    primary_key: list[str],
    column: str,
    dbt_dir: Path,
    target: str,
) -> ColumnMismatch | None:
    """Run a per-column value comparison, returning its mismatch rate.

    audit_helper.compare_column_values returns rows tagged by ``match_status``;
    the mismatch rate is the share of rows not in a "✅" perfect-match bucket.
    """
    rows = _run_dbt_show(
        _compare_column_sql(old, new, primary_key, column),
        dbt_dir,
        target,
        limit=1000,
    )
    total = 0
    mismatched = 0
    for row in rows:
        count = _int(row.get("count_records") or row.get("count"))
        total += count
        # audit_helper tags the perfect-match bucket with a leading "✅"; every
        # other bucket (value mismatch, missing from a/b) counts as a mismatch.
        if not str(row.get("match_status", "")).startswith("✅"):
            mismatched += count
    if total == 0:
        return None
    return ColumnMismatch(column=column, mismatch_rate=mismatched / total, mismatched_rows=mismatched)
