"""Impact command — column-level lineage analysis for in-progress dbt changes.

Detects which columns changed vs a base ref and traces the forward lineage
to alert when those changes will break or require work in downstream models.
"""

from __future__ import annotations

import json
import subprocess
import sys
from collections import deque
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.lib.git_utils import (
    get_changed_sql_models,
    get_file_at_ref,
    get_repo_root,
)
from ol_dbt_cli.lib.manifest import (
    ManifestRegistry,
    find_manifest,
    load_manifest,
)
from ol_dbt_cli.lib.sql_parser import (
    ParsedModel,
    find_compiled_dir,
    get_columns_read_from_ref,
    parse_model_file,
    parse_model_sql_at_content,
)
from ol_dbt_cli.lib.yaml_registry import YamlRegistry, build_yaml_registry

console = Console()
err_console = Console(stderr=True)


class AlertLevel(StrEnum):
    BREAKING = "BREAKING"
    WARNING = "WARNING"
    INFO = "INFO"


@dataclass
class ColumnChange:
    column: str
    change_type: str  # "removed", "added", "renamed_from", "renamed_to"
    related: str = ""  # for renames: the other side


@dataclass
class DownstreamImpact:
    model_name: str
    unique_id: str = ""
    affected_columns: list[str] = field(default_factory=list)
    depth: int = 0  # how many hops from the changed model


@dataclass
class ImpactAlert:
    level: AlertLevel
    changed_model: str
    column_changes: list[ColumnChange]
    downstream: list[DownstreamImpact]
    message: str
    manifest_available: bool = False


# ---------------------------------------------------------------------------
# Column diff
# ---------------------------------------------------------------------------


def _diff_columns(
    base_cols: set[str],
    current_cols: set[str],
) -> list[ColumnChange]:
    """Compute the list of column changes between *base_cols* and *current_cols*."""
    removed = base_cols - current_cols
    added = current_cols - base_cols

    changes: list[ColumnChange] = []

    # Heuristic rename detection: if exactly one column was removed and one added,
    # and their names share a long common prefix, treat as a rename.
    if len(removed) == 1 and len(added) == 1:
        rem = next(iter(removed))
        add = next(iter(added))
        common = _common_prefix_ratio(rem, add)
        if common >= 0.5:
            changes.append(ColumnChange(column=rem, change_type="renamed_from", related=add))
            changes.append(ColumnChange(column=add, change_type="renamed_to", related=rem))
            return changes

    for col in sorted(removed):
        changes.append(ColumnChange(column=col, change_type="removed"))
    for col in sorted(added):
        changes.append(ColumnChange(column=col, change_type="added"))
    return changes


def _common_prefix_ratio(a: str, b: str) -> float:
    """Return ratio of common prefix length to max string length."""
    common = 0
    for ca, cb in zip(a, b, strict=False):
        if ca == cb:
            common += 1
        else:
            break
    return common / max(len(a), len(b), 1)


# ---------------------------------------------------------------------------
# Downstream impact tracing
# ---------------------------------------------------------------------------


def _get_downstream_manifest(
    unique_id: str,
    registry: ManifestRegistry,
    removed_cols: set[str],
    sql_models_by_name: dict[str, ParsedModel],
) -> list[DownstreamImpact]:
    """Walk forward lineage via manifest to find models consuming *removed_cols*.

    SQL analysis is the primary signal.  For each downstream child:

    1. If compiled/raw SQL is available, use :func:`get_columns_read_from_ref`
       (qualified + unqualified column reads) combined with output-column
       passthrough detection to determine what is consumed.  When SQL is
       conclusive, the result is authoritative — children confirmed not to
       consume the removed columns are not flagged.
    2. If no SQL is available, fall back to manifest column metadata.
       Children with no metadata at all are flagged as potentially affected
       (``affected_columns`` empty = "column metadata unavailable").
    """
    results: list[DownstreamImpact] = []
    seen: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(unique_id, 0)])

    while queue:
        current_id, depth = queue.popleft()
        upstream_name = current_id.rsplit(".", 1)[-1] if "." in current_id else current_id
        for child in registry.get_children(current_id):
            if child.unique_id in seen:
                continue
            seen.add(child.unique_id)

            child_parsed = sql_models_by_name.get(child.name)
            affected: set[str] = set()
            sql_conclusive = False

            if child_parsed is not None:
                # SQL-first: qualified + unqualified column reads from the upstream ref
                sql_cols = get_columns_read_from_ref(child_parsed, upstream_name)
                if sql_cols is not None:
                    sql_conclusive = True
                    affected = removed_cols & sql_cols

                # Output-column passthrough: if the downstream model's final SELECT
                # emits a removed column, it is flowing through from upstream.
                output_passthrough = removed_cols & child_parsed.output_columns
                if output_passthrough:
                    sql_conclusive = True
                    affected |= output_passthrough

            if not sql_conclusive:
                # SQL analysis was unavailable — fall back to manifest column metadata.
                affected = removed_cols & child.column_names
                if affected or not child.columns:
                    results.append(
                        DownstreamImpact(
                            model_name=child.name,
                            unique_id=child.unique_id,
                            affected_columns=sorted(affected),
                            depth=depth + 1,
                        )
                    )
            elif affected:
                # SQL confirmed the child consumes one or more removed columns.
                results.append(
                    DownstreamImpact(
                        model_name=child.name,
                        unique_id=child.unique_id,
                        affected_columns=sorted(affected),
                        depth=depth + 1,
                    )
                )
            # else: SQL analysis ran and confirmed no consumption — child is safe, skip it.

            queue.append((child.unique_id, depth + 1))
    return results


def _get_downstream_yaml(
    model_name: str,
    removed_cols: set[str],
    yaml_registry: YamlRegistry,
    sql_models_by_name: dict[str, ParsedModel],
) -> list[DownstreamImpact]:
    """Walk forward lineage via YAML + sql_parser refs to find consumers of *removed_cols*.

    SQL analysis is the primary signal (same strategy as
    :func:`_get_downstream_manifest`).  YAML column lists are used only as a
    last resort when no compiled/raw SQL is available.
    """
    # Build a ref graph: for each parsed model, which models does it reference?
    # Then invert to get children.
    ref_to_parents: dict[str, list[str]] = {}
    for child_name, parsed in sql_models_by_name.items():
        for ref_name in parsed.refs:
            ref_to_parents.setdefault(ref_name, [])
            if child_name not in ref_to_parents[ref_name]:
                ref_to_parents[ref_name].append(child_name)

    results: list[DownstreamImpact] = []
    seen: set[str] = set()
    queue: deque[tuple[str, int]] = deque([(model_name, 0)])

    while queue:
        current, depth = queue.popleft()
        for child_name in ref_to_parents.get(current, []):
            if child_name in seen:
                continue
            seen.add(child_name)

            child_parsed = sql_models_by_name.get(child_name)
            affected: set[str] = set()
            sql_conclusive = False

            if child_parsed is not None:
                # SQL-first: qualified + unqualified column reads from the upstream ref
                sql_cols = get_columns_read_from_ref(child_parsed, current)
                if sql_cols is not None:
                    sql_conclusive = True
                    affected = removed_cols & sql_cols

                # Output-column passthrough
                output_passthrough = removed_cols & child_parsed.output_columns
                if output_passthrough:
                    sql_conclusive = True
                    affected |= output_passthrough

            if not sql_conclusive:
                # SQL unavailable — fall back to YAML column metadata.
                yaml_m = yaml_registry.get_model(child_name)
                child_cols = yaml_m.column_names if yaml_m else set()
                affected = removed_cols & child_cols
                if affected or yaml_m is None:
                    results.append(
                        DownstreamImpact(
                            model_name=child_name,
                            affected_columns=sorted(affected),
                            depth=depth + 1,
                        )
                    )
            elif affected:
                results.append(
                    DownstreamImpact(
                        model_name=child_name,
                        affected_columns=sorted(affected),
                        depth=depth + 1,
                    )
                )
            # else: SQL confirmed no consumption — skip.

            queue.append((child_name, depth + 1))
    return results


# ---------------------------------------------------------------------------
# Per-model impact analysis
# ---------------------------------------------------------------------------


def _analyse_model(
    model_name: str,
    sql_file: Path,
    base_ref: str,
    yaml_registry: YamlRegistry,
    manifest: ManifestRegistry | None,
    sql_models_by_name: dict[str, ParsedModel],
    repo_root: Path,
    compiled_dir: Path | None = None,
) -> ImpactAlert | None:
    """Return an :class:`ImpactAlert` for *model_name* if there are column changes."""
    # Current column set
    current_parsed = sql_models_by_name.get(model_name)
    if current_parsed is None:
        try:
            current_parsed = parse_model_file(sql_file, compiled_dir=compiled_dir)
        except Exception:  # noqa: BLE001
            return None

    current_cols = current_parsed.output_columns

    # Base (origin/main) column set
    base_content = get_file_at_ref(sql_file, base_ref, repo_root=repo_root)
    if base_content is None:
        # New model — no base to compare against
        return ImpactAlert(
            level=AlertLevel.INFO,
            changed_model=model_name,
            column_changes=[ColumnChange(column=c, change_type="added") for c in sorted(current_cols)],
            downstream=[],
            message=f"New model '{model_name}' — no base to compare against.",
            manifest_available=manifest is not None,
        )

    base_parsed = parse_model_sql_at_content(model_name, base_content)
    base_cols = base_parsed.output_columns

    if base_cols == current_cols:
        return None  # No column changes

    changes = _diff_columns(base_cols, current_cols)
    removed_or_renamed_from = {c.column for c in changes if c.change_type in ("removed", "renamed_from")}

    # Trace downstream impact
    if manifest is not None:
        manifest_model = manifest.get_model(model_name)
        if manifest_model is not None:
            downstream = _get_downstream_manifest(
                manifest_model.unique_id, manifest, removed_or_renamed_from, sql_models_by_name
            )
            manifest_available = True
        else:
            downstream = _get_downstream_yaml(model_name, removed_or_renamed_from, yaml_registry, sql_models_by_name)
            manifest_available = False
    else:
        downstream = _get_downstream_yaml(model_name, removed_or_renamed_from, yaml_registry, sql_models_by_name)
        manifest_available = False

    breaking_downstream = [d for d in downstream if d.affected_columns]
    unknown_impact = [d for d in downstream if not d.affected_columns]

    if breaking_downstream:
        level = AlertLevel.BREAKING
        msg = f"Column change(s) in '{model_name}' will break {len(breaking_downstream)} downstream model(s)."
    elif unknown_impact or any(c.change_type in ("removed", "renamed_from") for c in changes):
        level = AlertLevel.WARNING
        msg = (
            f"Column change(s) in '{model_name}' — {len(unknown_impact)} downstream model(s) "
            "may be affected (column metadata unavailable for precise impact)."
        )
    else:
        level = AlertLevel.INFO
        msg = f"Column(s) added to '{model_name}' — no breaking downstream impact detected."

    return ImpactAlert(
        level=level,
        changed_model=model_name,
        column_changes=changes,
        downstream=downstream,
        message=msg,
        manifest_available=manifest_available,
    )


# ---------------------------------------------------------------------------
# Output rendering
# ---------------------------------------------------------------------------

_LEVEL_COLOR = {
    AlertLevel.BREAKING: "bold red",
    AlertLevel.WARNING: "yellow",
    AlertLevel.INFO: "green",
}
_LEVEL_ICON = {
    AlertLevel.BREAKING: "🔴",
    AlertLevel.WARNING: "⚠️ ",
    AlertLevel.INFO: "ℹ️ ",
}


def _print_text_alerts(alerts: list[ImpactAlert]) -> None:
    if not alerts:
        console.print("[bold green]✅ No column-level impact detected in changed models.[/]")
        return

    for alert in alerts:
        color = _LEVEL_COLOR[alert.level]
        icon = _LEVEL_ICON[alert.level]
        console.print(f"\n{icon} [{color}]{alert.level.value}[/] [bold]{alert.changed_model}[/]")
        console.print(f"   {alert.message}")

        if alert.column_changes:
            console.print("   [bold]Column changes:[/]")
            for change in alert.column_changes:
                prefix = {
                    "removed": "❌",
                    "added": "➕",
                    "renamed_from": "🔄",
                    "renamed_to": "→ ",
                }.get(change.change_type, "•")
                suffix = f" → {change.related}" if change.related and change.change_type == "renamed_from" else ""
                console.print(f"     {prefix} {change.column}{suffix}")

        if alert.downstream:
            console.print("   [bold]Downstream impact:[/]")
            for d in alert.downstream:
                depth_str = f"  {'  ' * d.depth}"
                if d.affected_columns:
                    console.print(
                        f"   {depth_str}↳ [bold]{d.model_name}[/] [red](uses: {', '.join(d.affected_columns)})[/]"
                    )
                else:
                    console.print(f"   {depth_str}↳ [bold]{d.model_name}[/] [dim](column metadata unavailable)[/]")

        if not alert.manifest_available:
            console.print("   [dim]Tip: run `dbt parse` and re-run for more precise impact analysis.[/]")


def _print_json_alerts(alerts: list[ImpactAlert]) -> None:
    data = [
        {
            "level": a.level.value,
            "changed_model": a.changed_model,
            "message": a.message,
            "manifest_available": a.manifest_available,
            "column_changes": [
                {"column": c.column, "change_type": c.change_type, "related": c.related} for c in a.column_changes
            ],
            "downstream": [
                {
                    "model": d.model_name,
                    "unique_id": d.unique_id,
                    "affected_columns": d.affected_columns,
                    "depth": d.depth,
                }
                for d in a.downstream
            ],
        }
        for a in alerts
    ]
    print(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def impact(
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to dbt project root (contains dbt_project.yml). Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
    model: Annotated[
        str | None,
        Parameter(
            name=["--model", "-m"],
            help="Analyse a specific model instead of all changed models.",
        ),
    ] = None,
    base_ref: Annotated[
        str,
        Parameter(
            name=["--base-ref"],
            help="Git ref to compare against (default: origin/main).",
        ),
    ] = "origin/main",
    output_format: Annotated[
        str,
        Parameter(
            name=["--format", "-f"],
            help="Output format: text (default) or json.",
        ),
    ] = "text",
    compiled_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--compiled-dir"],
            help=(
                "Path to the compiled models directory (e.g. target/compiled/open_learning/models). "
                "Uses Jinja-rendered SQL for accurate column extraction. "
                "Auto-detected from {dbt-dir}/target/compiled/ if present."
            ),
        ),
    ] = None,
    auto_compile: Annotated[
        bool,
        Parameter(
            name=["--auto-compile"],
            help=(
                "Run 'dbt compile' on changed models and their downstream dependents before "
                "analysis to ensure compiled SQL is up to date. Uses --target (default: dev_local, "
                "a local DuckDB instance) so no network or credentials are required."
            ),
        ),
    ] = False,
    compile_target: Annotated[
        str,
        Parameter(
            name=["--target", "-t"],
            help=(
                "dbt target to use when --auto-compile is set (default: dev_local). "
                "dev_local uses a local DuckDB instance and requires no network access. "
                "Other targets (dev_qa, production) require Trino credentials."
            ),
        ),
    ] = "dev_local",
) -> None:
    """Analyse column-level impact of in-progress dbt model changes.

    Compares each changed model's SQL column output vs the base ref and walks
    the forward dependency graph to identify downstream models that will be broken
    or require follow-up work.

    Alert levels:
      🔴 BREAKING  — a removed/renamed column is used by a downstream model
      ⚠️  WARNING   — column changed but downstream impact cannot be fully determined
      ℹ️  INFO      — only additive changes (new columns), no downstream breakage

    Uses dbt manifest.json when available (run `dbt parse` first) for accurate
    lineage. Falls back to sqlglot-based ref() parsing otherwise.

    Examples:
        Analyse all changed models vs origin/main:
            ol-dbt impact

        Analyse a specific model:
            ol-dbt impact --model stg__mitlearn__app__postgres__users_user

        Compare against a different base:
            ol-dbt impact --base-ref origin/develop

        JSON output for CI integration:
            ol-dbt impact --format json

        Compile changed models and downstream before analysis (uses dev_local DuckDB):
            ol-dbt impact --auto-compile

        Compile using a specific target:
            ol-dbt impact --auto-compile --target dev_qa

    """
    # Resolve dbt project directory
    if dbt_dir_path:
        dbt_dir = Path(dbt_dir_path).resolve()
    else:
        try:
            repo_root = get_repo_root()
            dbt_dir = repo_root / "src" / "ol_dbt"
        except RuntimeError:
            dbt_dir = Path("src/ol_dbt").resolve()

    if not (dbt_dir / "dbt_project.yml").exists():
        err_console.print(f"[red]Error:[/] dbt project not found at {dbt_dir}")
        err_console.print("  Use --dbt-dir to specify the path.")
        sys.exit(1)

    try:
        repo_root = get_repo_root(dbt_dir)
    except RuntimeError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    models_dir = dbt_dir / "models"

    # Resolve compiled SQL directory (Jinja-free, most accurate)
    compiled_dir: Path | None = None
    if compiled_dir_path:
        compiled_dir = Path(compiled_dir_path).resolve()
        if not compiled_dir.is_dir():
            err_console.print(f"[yellow]Warning:[/] --compiled-dir '{compiled_dir}' not found; using raw SQL parsing.")
            compiled_dir = None
    else:
        compiled_dir = find_compiled_dir(dbt_dir)

    if compiled_dir is not None and output_format == "text":
        console.print(f"[dim]Using compiled SQL: {compiled_dir}[/]")

    # Load manifest if available
    manifest: ManifestRegistry | None = None
    manifest_path = find_manifest(dbt_dir)
    if manifest_path:
        try:
            manifest = load_manifest(manifest_path)
            if output_format == "text":
                console.print(f"[dim]Using manifest: {manifest_path}[/]")
        except Exception as exc:  # noqa: BLE001
            if output_format == "text":
                console.print(f"[yellow]Warning:[/] Could not load manifest ({exc}); using raw SQL parsing.")

    # Build YAML registry and parse all SQL models
    yaml_registry = build_yaml_registry(models_dir)
    all_sql_files = sorted(models_dir.rglob("*.sql"))
    sql_file_map: dict[str, Path] = {f.stem: f for f in all_sql_files}

    sql_models_by_name: dict[str, ParsedModel] = {}
    parse_errors: dict[str, str] = {}
    for name, path in sql_file_map.items():
        try:
            parsed = parse_model_file(path, compiled_dir=compiled_dir)
            sql_models_by_name[name] = parsed
            # parse_model_file may succeed but record internal parse errors
            if parsed.parse_error:
                parse_errors[name] = parsed.parse_error
        except Exception as exc:  # noqa: BLE001
            parse_errors[name] = str(exc)

    # Determine which models to analyse
    if model:
        target_names = [model]
    else:
        try:
            target_names = get_changed_sql_models(dbt_dir, base_ref=base_ref, repo_root=repo_root)
        except RuntimeError as exc:
            err_console.print(f"[red]Error resolving git diff:[/] {exc}")
            sys.exit(1)
        if not target_names:
            console.print(f"[dim]No SQL model changes detected vs {base_ref}.[/]")
            return

    # Warn about any models in the target set that failed to parse — these will be
    # analysed with limited accuracy (no output-column info) and may miss downstream impacts.
    for name in target_names:
        if name in parse_errors:
            err_console.print(
                f"[yellow]Warning:[/] Could not parse '{name}': {parse_errors[name][:200]}. "
                "Downstream impact may be incomplete."
            )

    if output_format == "text":
        console.print(f"\n[bold]Analysing {len(target_names)} model(s) for column-level impact[/]\n")

    # Auto-compile: run dbt compile on changed models + their downstream deps.
    # Uses dev_local (DuckDB) by default — no network or credentials required.
    if auto_compile and target_names:
        selector = " ".join(f"{n}+" for n in target_names)
        cmd = ["dbt", "compile", "--target", compile_target, "--select", selector]
        if output_format == "text":
            console.print(f"[dim]Running: {' '.join(cmd)} ...[/]")
        try:
            subprocess.run(  # noqa: S603, S607
                cmd,
                cwd=str(dbt_dir),
                capture_output=True,
                text=True,
                check=True,
            )
            if output_format == "text":
                console.print("[dim]dbt compile succeeded.[/]")
            # Re-detect compiled dir after compile
            compiled_dir = find_compiled_dir(dbt_dir) if compiled_dir is None else compiled_dir
            # Reload parsed models with fresh compiled SQL
            for name, path in sql_file_map.items():
                try:
                    sql_models_by_name[name] = parse_model_file(path, compiled_dir=compiled_dir)
                except Exception:  # noqa: BLE001, S110
                    pass
        except subprocess.CalledProcessError as exc:
            err_console.print(f"[yellow]Warning:[/] dbt compile failed: {exc.stderr[-200:] if exc.stderr else exc}")
            err_console.print("  Analysis will continue with raw SQL.")
        except FileNotFoundError:
            err_console.print("[yellow]Warning:[/] 'dbt' command not found; skipping auto-compile.")
            err_console.print("  Install dbt and ensure it is on PATH, or run dbt compile manually.")

    alerts: list[ImpactAlert] = []
    models_without_compiled: list[str] = []
    for name in target_names:
        sql_file = sql_file_map.get(name)
        if sql_file is None:
            continue
        alert = _analyse_model(
            name,
            sql_file,
            base_ref,
            yaml_registry,
            manifest,
            sql_models_by_name,
            repo_root,
            compiled_dir=compiled_dir,
        )
        if alert is not None:
            alerts.append(alert)
        # Track downstream models missing compiled SQL when manifest is present
        if compiled_dir is not None and alert is not None:
            for d in alert.downstream:
                d_parsed = sql_models_by_name.get(d.model_name)
                if d_parsed is not None and d_parsed.compiled_path is None:
                    models_without_compiled.append(d.model_name)

    # Sort: BREAKING first, then WARNING, then INFO
    order = {AlertLevel.BREAKING: 0, AlertLevel.WARNING: 1, AlertLevel.INFO: 2}
    alerts.sort(key=lambda a: order[a.level])

    if output_format == "json":
        _print_json_alerts(alerts)
    else:
        _print_text_alerts(alerts)
        breaking = sum(1 for a in alerts if a.level == AlertLevel.BREAKING)
        warnings = sum(1 for a in alerts if a.level == AlertLevel.WARNING)
        console.print(
            f"\n[bold]Summary:[/] {breaking} breaking, {warnings} warning(s) across {len(alerts)} changed model(s)."
        )
        if models_without_compiled:
            unique_missing = sorted(set(models_without_compiled))
            changed_str = " ".join(f"{n}+" for n in target_names)
            console.print(
                f"\n[yellow]⚠️  {len(unique_missing)} downstream model(s) had no compiled SQL — "
                "analysis used raw SQL (less accurate).[/]"
            )
            console.print(f"   Run [bold]dbt compile --select {changed_str}[/] for complete analysis.")

    if any(a.level == AlertLevel.BREAKING for a in alerts):
        sys.exit(1)
