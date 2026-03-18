"""Impact command — column-level lineage analysis for in-progress dbt changes.

Detects which columns changed vs a base ref and traces the forward lineage
to alert when those changes will break or require work in downstream models.
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated

import sqlglot
import sqlglot.expressions as exp
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
    _leftmost_select_from_union,
    find_compiled_dir,
    parse_model_file,
    parse_model_sql_at_content,
    strip_jinja,
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
# Column-read analysis (for impact false-negative detection)
# ---------------------------------------------------------------------------


def _get_columns_read_from_ref(
    downstream_parsed: ParsedModel,
    upstream_name: str,
) -> set[str] | None:
    """Find columns a downstream model reads from an upstream model ref.

    Handles the case where a column is read from upstream but aliased to a
    different name in the output, so the renamed output column won't appear
    in ``child.column_names`` even though the upstream column is used.

    Returns the set of column names read from the upstream, or ``None`` if
    the SQL is unavailable or the upstream is not referenced.
    """
    if upstream_name not in downstream_parsed.refs:
        return None

    ref_placeholder = next(
        (k for k, v in downstream_parsed.ref_placeholder_map.items() if v == upstream_name),
        None,
    )
    if ref_placeholder is None:
        return None

    sql_path = downstream_parsed.compiled_path or downstream_parsed.source_path
    if sql_path is None or not sql_path.exists():
        return None

    try:
        raw_sql = sql_path.read_text()
        if downstream_parsed.compiled_path:
            clean_sql = raw_sql
        else:
            clean_sql = strip_jinja(raw_sql).clean_sql

        stmts = sqlglot.parse(clean_sql, dialect="trino", error_level=sqlglot.ErrorLevel.IGNORE)
        parsed_sql = next((s for s in reversed(stmts) if s is not None), None)
        if parsed_sql is None:
            return None
    except Exception:  # noqa: BLE001
        return None

    # Find CTE names that are thin passthroughs to the upstream ref placeholder
    passthrough_aliases: set[str] = {ref_placeholder.lower()}
    for cte in parsed_sql.find_all(exp.CTE):
        cte_name = cte.alias_or_name.lower()
        cte_query = cte.args.get("this")

        cte_select: exp.Select | None = None
        if isinstance(cte_query, exp.Select):
            cte_select = cte_query
        elif isinstance(cte_query, exp.Union):
            cte_select = _leftmost_select_from_union(cte_query)

        if cte_select is None:
            continue

        from_clause = cte_select.args.get("from_")
        if from_clause is None:
            continue
        from_table = from_clause.find(exp.Table)
        if from_table and from_table.alias_or_name.lower() in passthrough_aliases:
            if cte_select.find(exp.Star) is not None:
                passthrough_aliases.add(cte_name)

    # Collect column names referenced with a qualifying table from the passthrough set
    cols_read: set[str] = set()
    for col_node in parsed_sql.find_all(exp.Column):
        table = col_node.args.get("table")
        if table is None:
            continue
        table_name = table.name.lower() if hasattr(table, "name") else str(table).lower()
        if table_name in passthrough_aliases:
            col_name = col_node.name.lower()
            if col_name and col_name != "*":
                cols_read.add(col_name)

    return cols_read if cols_read else None


# ---------------------------------------------------------------------------
# Downstream impact tracing
# ---------------------------------------------------------------------------


def _get_downstream_manifest(
    unique_id: str,
    registry: ManifestRegistry,
    removed_cols: set[str],
    sql_models_by_name: dict[str, ParsedModel],
) -> list[DownstreamImpact]:
    """Walk forward lineage via manifest to find models consuming *removed_cols*."""
    results: list[DownstreamImpact] = []
    seen: set[str] = set()
    queue: list[tuple[str, int]] = [(unique_id, 0)]

    while queue:
        current_id, depth = queue.pop(0)
        for child in registry.get_children(current_id):
            if child.unique_id in seen:
                continue
            seen.add(child.unique_id)
            affected = removed_cols & child.column_names
            if not affected and child.columns:
                # The manifest column list doesn't include the removed columns, but
                # the model might still read them (and alias them to different output
                # names). Fall back to SQL-level column-read analysis.
                child_parsed = sql_models_by_name.get(child.name)
                # Extract the upstream model name from the unique_id (format: model.pkg.name)
                upstream_name = current_id.rsplit(".", 1)[-1] if "." in current_id else current_id
                if child_parsed and upstream_name:
                    cols_read = _get_columns_read_from_ref(child_parsed, upstream_name)
                    if cols_read:
                        affected = removed_cols & cols_read
            if affected or not child.columns:
                # If child has no column metadata, flag it as potentially affected
                results.append(
                    DownstreamImpact(
                        model_name=child.name,
                        unique_id=child.unique_id,
                        affected_columns=sorted(affected),
                        depth=depth + 1,
                    )
                )
            queue.append((child.unique_id, depth + 1))
    return results


def _get_downstream_yaml(
    model_name: str,
    removed_cols: set[str],
    yaml_registry: YamlRegistry,
    sql_models_by_name: dict[str, ParsedModel],
) -> list[DownstreamImpact]:
    """Walk forward lineage via YAML + sql_parser refs to find consumers of *removed_cols*."""
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
    queue: list[tuple[str, int]] = [(model_name, 0)]

    while queue:
        current, depth = queue.pop(0)
        for child_name in ref_to_parents.get(current, []):
            if child_name in seen:
                continue
            seen.add(child_name)
            yaml_m = yaml_registry.get_model(child_name)
            child_cols = yaml_m.column_names if yaml_m else set()
            affected = removed_cols & child_cols
            if not affected and yaml_m is not None:
                # The YAML column list doesn't show the removed columns, but the model
                # might read them and alias them. Fall back to SQL-level analysis.
                child_parsed = sql_models_by_name.get(child_name)
                if child_parsed:
                    cols_read = _get_columns_read_from_ref(child_parsed, current)
                    if cols_read:
                        affected = removed_cols & cols_read
            if affected or yaml_m is None:
                results.append(
                    DownstreamImpact(
                        model_name=child_name,
                        affected_columns=sorted(affected),
                        depth=depth + 1,
                    )
                )
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
) -> ImpactAlert | None:
    """Return an :class:`ImpactAlert` for *model_name* if there are column changes."""
    # Current column set
    current_parsed = sql_models_by_name.get(model_name)
    if current_parsed is None:
        try:
            current_parsed = parse_model_file(sql_file)
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
    for name, path in sql_file_map.items():
        try:
            sql_models_by_name[name] = parse_model_file(path, compiled_dir=compiled_dir)
        except Exception:  # noqa: BLE001, S110
            pass

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

    if output_format == "text":
        console.print(f"\n[bold]Analysing {len(target_names)} model(s) for column-level impact[/]\n")

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
