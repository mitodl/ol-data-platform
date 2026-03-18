"""Validate command — structural validation of dbt models.

Checks:
  1. SQL/YAML sync: columns declared in YAML vs columns produced by SQL
  2. Upstream reference validation: column referenced from ref() actually exists upstream
  3. Docs coverage: .sql models with no YAML definition
  4. YAML integrity: YAML entries for models that have no corresponding .sql file
  5. SELECT *: models using SELECT * that hides column-level lineage
"""

from __future__ import annotations

import json
import subprocess
import sys
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.lib.git_utils import get_changed_sql_models, get_repo_root
from ol_dbt_cli.lib.manifest import ManifestRegistry, find_manifest, load_manifest
from ol_dbt_cli.lib.sql_parser import ParsedModel, find_compiled_dir, parse_model_file
from ol_dbt_cli.lib.yaml_registry import YamlRegistry, build_yaml_registry

console = Console()
err_console = Console(stderr=True)


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


# ---------------------------------------------------------------------------
# Check 1: SQL/YAML column sync
# ---------------------------------------------------------------------------


def _check_yaml_sql_sync(
    model_name: str,
    yaml_registry: YamlRegistry,
    parsed: ParsedModel,
    report: ValidationReport,
) -> None:
    yaml_model = yaml_registry.get_model(model_name)
    if yaml_model is None:
        return  # handled by docs coverage check

    yaml_cols = yaml_model.column_names
    sql_cols = parsed.output_columns

    if parsed.has_star:
        # Unresolved SELECT * — column set is incomplete. Skip phantom/undocumented
        # checks entirely to avoid false positives (all YAML cols would appear phantom).
        source_hint = f" (from '{parsed.star_source}')" if parsed.star_source else ""
        report.add(
            "yaml_sql_sync",
            Severity.WARNING,
            model_name,
            f"SELECT *{source_hint} used — column sync cannot be fully validated",
            "sqlglot could not resolve the star expansion; run `dbt parse` for accurate columns, "
            "or replace SELECT * with explicit column aliases.",
        )
        return  # skip phantom/undocumented — they'd all be false positives

    phantom = yaml_cols - sql_cols
    if phantom:
        report.add(
            "yaml_sql_sync",
            Severity.ERROR,
            model_name,
            f"Columns in YAML but missing from SQL output: {sorted(phantom)}",
            "These YAML column entries have no matching alias in the compiled SQL SELECT list.",
        )

    undocumented = sql_cols - yaml_cols
    if undocumented:
        report.add(
            "yaml_sql_sync",
            Severity.WARNING,
            model_name,
            f"Columns in SQL output but not documented in YAML: {sorted(undocumented)}",
            "Add these columns to the model's YAML schema file.",
        )


# ---------------------------------------------------------------------------
# Check 2: Upstream column reference validation
# ---------------------------------------------------------------------------


def _resolve_upstream_columns(
    ref_name: str,
    yaml_registry: YamlRegistry,
    manifest: ManifestRegistry | None,
    sql_models_by_name: dict[str, ParsedModel],
) -> set[str] | None:
    """Return the known column set for *ref_name*, or None if unknown."""
    if manifest is not None:
        m = manifest.get_model(ref_name)
        if m is not None and m.columns:
            return m.column_names
    yaml_m = yaml_registry.get_model(ref_name)
    if yaml_m is not None and yaml_m.columns:
        return yaml_m.column_names
    if ref_name in sql_models_by_name:
        return sql_models_by_name[ref_name].output_columns
    return None


def _check_upstream_refs(
    model_name: str,
    parsed: ParsedModel,
    yaml_registry: YamlRegistry,
    manifest: ManifestRegistry | None,
    sql_models_by_name: dict[str, ParsedModel],
    report: ValidationReport,
) -> None:
    """Check that every ref() / source() used by the model can be resolved.

    Reports a WARNING when the upstream model's column list is entirely unknown,
    which means lineage and impact analysis will be incomplete.  The more invasive
    "cross-ref column attribution" check has been removed because it cannot
    correctly attribute a YAML column to a *specific* upstream ref without
    SQL-AST-level table alias analysis — any such check produces too many false
    positives when a model joins several refs and adds computed columns.
    """
    for ref_name in parsed.refs:
        upstream_cols = _resolve_upstream_columns(ref_name, yaml_registry, manifest, sql_models_by_name)
        if upstream_cols is None:
            report.add(
                "upstream_refs",
                Severity.WARNING,
                model_name,
                f"Cannot resolve column list for ref('{ref_name}')",
                "Run `dbt parse` to generate manifest.json for accurate upstream column resolution.",
            )


# ---------------------------------------------------------------------------
# Check 3: Docs coverage
# ---------------------------------------------------------------------------


def _check_docs_coverage(
    model_name: str,
    yaml_registry: YamlRegistry,
    report: ValidationReport,
) -> None:
    if yaml_registry.get_model(model_name) is None:
        report.add(
            "docs_coverage",
            Severity.WARNING,
            model_name,
            f"Model '{model_name}' has no YAML documentation",
            "Create or update a _*.yml schema file in the model's directory.",
        )


# ---------------------------------------------------------------------------
# Check 4: YAML references non-existent SQL model
# ---------------------------------------------------------------------------


def _check_yaml_integrity(
    yaml_registry: YamlRegistry,
    sql_model_names: set[str],
    report: ValidationReport,
) -> None:
    for name, yaml_model in yaml_registry.models.items():
        if name not in sql_model_names:
            report.add(
                "yaml_integrity",
                Severity.ERROR,
                name,
                f"YAML defines model '{name}' but no corresponding .sql file found",
                f"Defined in: {yaml_model.source_file}",
            )


# ---------------------------------------------------------------------------
# Registry-aware SELECT * resolution
# ---------------------------------------------------------------------------


def _resolve_star_with_registry(
    parsed: ParsedModel,
    yaml_registry: YamlRegistry,
    manifest: ManifestRegistry | None,
    sql_models_by_name: dict[str, ParsedModel],
) -> set[str] | None:
    """Try to resolve ``SELECT *`` using YAML/manifest column declarations.

    Called when CTE-level resolution failed (``parsed.has_star is True``).
    Returns the resolved column set on success, ``None`` if unresolvable.

    Resolution order for the star's source:
    1. If the source is a ``ref_<model>`` placeholder: look up the model's
       columns in the manifest (most accurate), then the YAML registry, then
       the already-parsed SQL output of that model.
    2. If the source is a ``source_<source>_<table>`` placeholder: look up the
       source table's column declarations in the YAML registry.
    """
    star_source = parsed.star_source
    if not star_source:
        return None

    # --- ref() resolution ---
    if star_source in parsed.ref_placeholder_map:
        upstream_model = parsed.ref_placeholder_map[star_source]

        # Manifest columns are the most authoritative
        if manifest:
            m_model = manifest.get_model(upstream_model)
            if m_model and m_model.columns:
                return m_model.column_names

        # YAML-declared columns
        y_model = yaml_registry.get_model(upstream_model)
        if y_model and y_model.columns:
            return y_model.column_names

        # Parsed SQL output of the upstream model (best-effort)
        upstream_parsed = sql_models_by_name.get(upstream_model)
        if upstream_parsed and not upstream_parsed.has_star and upstream_parsed.output_columns:
            return upstream_parsed.output_columns

    # --- source() resolution ---
    if star_source in parsed.source_placeholder_map:
        source_key = parsed.source_placeholder_map[star_source]  # "source_name.table_name"
        source_name, table_name = source_key.split(".", 1)
        cols = yaml_registry.get_source_columns(source_name, table_name)
        if cols:
            return cols

    return None


# ---------------------------------------------------------------------------
# Check 5: SELECT * usage
# ---------------------------------------------------------------------------


def _check_select_star(
    model_name: str,
    parsed: ParsedModel,
    report: ValidationReport,
) -> None:
    """Warn when a model uses SELECT * that could not be resolved to explicit columns.

    A SELECT * that sqlglot resolved via CTE analysis is flagged at INFO level
    (it works, but makes column lineage opaque). An unresolved SELECT * drawing
    from a ref() placeholder or external table is flagged at WARNING level (it
    prevents column-level validation entirely).
    """
    if parsed.has_star:
        source_hint = f" (from '{parsed.star_source}')" if parsed.star_source else ""
        report.add(
            "select_star",
            Severity.WARNING,
            model_name,
            f"Model uses SELECT *{source_hint} that cannot be statically resolved",
            "Replace SELECT * with explicit column aliases to enable column-level validation "
            "and improve downstream lineage clarity.",
        )
    elif parsed.star_resolved:
        report.add(
            "select_star",
            Severity.INFO,
            model_name,
            "Model uses SELECT * (resolved via CTE or upstream schema analysis)",
            "Consider replacing SELECT * with explicit column aliases for clearer lineage "
            "and to avoid surprises when upstream columns change.",
        )


# ---------------------------------------------------------------------------


def _print_text_report(report: ValidationReport, errors_only: bool = False) -> None:
    visible = [i for i in report.issues if not errors_only or i.severity == Severity.ERROR]

    if not visible:
        if errors_only and report.issues:
            console.print("[bold green]✅ No errors — warnings/info suppressed (--errors-only).[/]")
        else:
            console.print("[bold green]✅ All checks passed — no issues found.[/]")
        return

    checks = sorted({i.check for i in visible})
    for check in checks:
        check_issues = [i for i in visible if i.check == check]
        console.print(f"\n[bold cyan]── {check.upper().replace('_', ' ')} ──[/]")
        for issue in check_issues:
            color = {"ERROR": "red", "WARNING": "yellow", "INFO": "blue"}[issue.severity.value]
            console.print(f"  [{color}]{issue.severity.value}[/] [bold]{issue.model}[/]: {issue.message}")
            if issue.detail:
                console.print(f"    [dim]{issue.detail}[/]")


def _print_json_report(report: ValidationReport, errors_only: bool = False) -> None:
    issues = report.issues if not errors_only else report.errors
    data = [
        {
            "check": i.check,
            "severity": i.severity.value,
            "model": i.model,
            "message": i.message,
            "detail": i.detail,
        }
        for i in issues
    ]
    print(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# Model target resolution
# ---------------------------------------------------------------------------


def _resolve_model_targets(
    raw_values: tuple[str, ...],
    models_dir: Path,
    sql_file_map: dict[str, Path],
) -> tuple[list[str], list[str]]:
    """Expand ``--model`` values into model names, handling names, globs, and directories.

    Each value in *raw_values* can be:

    * A model name (exact stem match): ``stg__mitlearn__app__postgres__users_user``
    * A comma-separated list of names/paths: ``model_a,model_b``
    * A directory path (absolute, or relative to *models_dir*, or a subdirectory
      name found anywhere under *models_dir*): ``staging/mitlearn`` or just ``mitlearn``

    Returns ``(resolved_names, unknown)`` where *unknown* lists values that could
    not be matched to any known model or directory.
    """
    resolved: list[str] = []
    unknown: list[str] = []

    # Flatten comma-separated values so users can do --model a,b or --model a --model b
    tokens: list[str] = []
    for raw in raw_values:
        tokens.extend(part.strip() for part in raw.split(",") if part.strip())

    for token in tokens:
        # 1. Exact model name match
        if token in sql_file_map:
            resolved.append(token)
            continue

        # 2. Directory resolution — try several interpretations
        candidate_dirs: list[Path] = []

        as_abs = Path(token)
        if as_abs.is_absolute() and as_abs.is_dir():
            candidate_dirs.append(as_abs)

        as_rel = models_dir / token
        if as_rel.is_dir():
            candidate_dirs.append(as_rel)

        # Fuzzy: find any subdirectory whose name matches the token
        if not candidate_dirs:
            candidate_dirs = [p for p in models_dir.rglob(token) if p.is_dir()]

        if candidate_dirs:
            before = len(resolved)
            for d in candidate_dirs:
                for sql_file in sorted(d.rglob("*.sql")):
                    if sql_file.stem in sql_file_map:
                        resolved.append(sql_file.stem)
            if len(resolved) > before:
                continue

        unknown.append(token)

    # Deduplicate while preserving order
    seen: set[str] = set()
    deduped: list[str] = []
    for name in resolved:
        if name not in seen:
            seen.add(name)
            deduped.append(name)

    return deduped, unknown


# ---------------------------------------------------------------------------


def validate(
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to dbt project root (contains dbt_project.yml). Defaults to src/ol_dbt relative to repo root.",
        ),
    ] = None,
    model: Annotated[
        tuple[str, ...],
        Parameter(
            name=["--model", "-m"],
            help=(
                "Model name(s), comma-separated list, or directory path to validate. "
                "Repeat the flag for multiple values: --model a --model b. "
                "Paths are resolved relative to the models/ directory; a bare name like "
                "'mitlearn' matches any subdirectory with that name."
            ),
        ),
    ] = (),
    changed_only: Annotated[
        bool,
        Parameter(
            name=["--changed-only", "-c"],
            help="Only validate models that have changed vs origin/main.",
        ),
    ] = False,
    base_ref: Annotated[
        str,
        Parameter(
            name=["--base-ref"],
            help="Git ref to compare against for --changed-only (default: origin/main).",
        ),
    ] = "origin/main",
    output_format: Annotated[
        str,
        Parameter(
            name=["--format", "-f"],
            help="Output format: text (default) or json.",
        ),
    ] = "text",
    skip_checks: Annotated[
        str | None,
        Parameter(
            name=["--skip"],
            help=(
                "Comma-separated list of checks to skip: "
                "yaml_sql_sync, upstream_refs, docs_coverage, yaml_integrity, select_star."
            ),
        ),
    ] = None,
    only_checks: Annotated[
        str | None,
        Parameter(
            name=["--only"],
            help=(
                "Comma-separated list of checks to run exclusively (all others are skipped): "
                "yaml_sql_sync, upstream_refs, docs_coverage, yaml_integrity, select_star. "
                "Mutually exclusive with --skip."
            ),
        ),
    ] = None,
    errors_only: Annotated[
        bool,
        Parameter(
            name=["--errors-only", "-e"],
            help="Only display ERROR-level issues; suppress warnings and info. Exit code still reflects all errors.",
        ),
    ] = False,
    compiled_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--compiled-dir"],
            help=(
                "Path to the compiled models directory (e.g. target/compiled/open_learning/models). "
                "When provided, uses Jinja-rendered SQL for accurate column extraction instead of "
                "regex-based Jinja stripping. Auto-detected from {dbt-dir}/target/compiled/ if present."
            ),
        ),
    ] = None,
    auto_compile: Annotated[
        bool,
        Parameter(
            name=["--auto-compile"],
            help=(
                "Run 'dbt parse' before validation to regenerate manifest.json. "
                "This is fast and offline (no database required). "
                "Use when you want the manifest to reflect recent model changes. "
                "For compiled SQL accuracy, run 'dbt compile' manually first."
            ),
        ),
    ] = False,
) -> None:
    """Validate dbt model SQL and YAML schema files for consistency.

    Runs five checks:

    1. yaml_sql_sync  — columns in YAML match columns in SQL SELECT output
    2. upstream_refs  — columns referenced from upstream ref() models exist
    3. docs_coverage  — every .sql model has a YAML definition
    4. yaml_integrity — every YAML model entry has a corresponding .sql file
    5. select_star    — flag models using SELECT * (WARNING when unresolvable, INFO when resolved)

    Uses dbt manifest.json when available (run `dbt parse` first) for accurate
    column resolution. Falls back to sqlglot-based raw SQL parsing otherwise.

    When SELECT * is encountered, sqlglot attempts to expand it by tracing through
    CTEs in the same file. If successful, yaml_sql_sync proceeds normally. If the
    star draws from a ref() or external table, column sync is skipped for that model
    to avoid false positives, and select_star emits a WARNING instead.

    Examples:
        Validate all models:
            ol-dbt validate

        Only show errors (suppress warnings):
            ol-dbt validate --errors-only

        Validate only changed models vs origin/main:
            ol-dbt validate --changed-only

        Validate a single model by name:
            ol-dbt validate --model stg__mitlearn__app__postgres__users_user

        Validate multiple models (repeat flag or comma-separate):
            ol-dbt validate --model stg__mitlearn__users --model stg__mitlearn__orders
            ol-dbt validate --model stg__mitlearn__users,stg__mitlearn__orders

        Validate all models under a directory:
            ol-dbt validate --model staging/mitlearn
            ol-dbt validate --model mitlearn

        JSON output for CI integration:
            ol-dbt validate --changed-only --format json

        Run only one check:
            ol-dbt validate --only yaml_sql_sync

        Run only two checks:
            ol-dbt validate --only yaml_sql_sync,docs_coverage

        Skip a specific check:
            ol-dbt validate --skip select_star

        Regenerate manifest before validating:
            ol-dbt validate --auto-compile

    """
    all_checks = {"yaml_sql_sync", "upstream_refs", "docs_coverage", "yaml_integrity", "select_star"}
    if skip_checks and only_checks:
        console.print("[bold red]Error:[/] --skip and --only are mutually exclusive.")
        raise SystemExit(1)
    skipped: set[str] = set()
    if skip_checks:
        skipped = {s.strip() for s in skip_checks.split(",")}
    elif only_checks:
        requested = {s.strip() for s in only_checks.split(",")}
        skipped = all_checks - requested

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

    models_dir = dbt_dir / "models"

    # Auto-compile: run dbt parse to regenerate manifest.json before validation.
    # dbt parse is fast and offline (no database required).
    if auto_compile:
        if output_format == "text":
            console.print("[dim]Running: dbt parse ...[/]")
        try:
            subprocess.run(  # noqa: S603, S607
                ["dbt", "parse"],
                cwd=str(dbt_dir),
                capture_output=True,
                text=True,
                check=True,
            )
            if output_format == "text":
                console.print("[dim]dbt parse succeeded — manifest regenerated.[/]")
        except subprocess.CalledProcessError as exc:
            err_console.print(f"[yellow]Warning:[/] dbt parse failed: {exc.stderr[-200:] if exc.stderr else exc}")
            err_console.print("  Validation will continue with existing or no manifest.")
        except FileNotFoundError:
            err_console.print("[yellow]Warning:[/] 'dbt' command not found; skipping auto-compile.")
            err_console.print("  Install dbt and ensure it is on PATH, or run dbt parse manually.")

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

    # Build YAML registry
    yaml_registry = build_yaml_registry(models_dir)

    # Collect all SQL model files
    all_sql_files = sorted(models_dir.rglob("*.sql"))
    sql_model_names: set[str] = {f.stem for f in all_sql_files}

    # Determine which models to validate
    if model:
        sql_file_map: dict[str, Path] = {f.stem: f for f in all_sql_files}
        target_names, unknown_tokens = _resolve_model_targets(model, models_dir, sql_file_map)
        if unknown_tokens:
            for tok in unknown_tokens:
                err_console.print(
                    f"[yellow]Warning:[/] --model value '{tok}' did not match any model name or directory — skipped."
                )
        if not target_names:
            err_console.print("[red]Error:[/] No matching models found for the provided --model values.")
            sys.exit(1)
    elif changed_only:
        try:
            target_names = get_changed_sql_models(dbt_dir, base_ref=base_ref)
        except RuntimeError as exc:
            err_console.print(f"[red]Error resolving git diff:[/] {exc}")
            sys.exit(1)
        if not target_names:
            console.print(f"[dim]No SQL model changes detected vs {base_ref}.[/]")
            return
    else:
        target_names = [f.stem for f in all_sql_files]

    if output_format == "text":
        if model:
            mode = f"{len(target_names)} model(s) from --model filter"
        elif changed_only:
            mode = f"changed models vs {base_ref}"
        else:
            mode = "all models"
        console.print(f"\n[bold]Validating {len(target_names)} {mode}[/]\n")

    # Parse all SQL files up-front (needed for cross-model reference resolution)
    sql_file_map_all: dict[str, Path] = {f.stem: f for f in all_sql_files}
    sql_models_by_name: dict[str, ParsedModel] = {}
    for name, path in sql_file_map_all.items():
        try:
            sql_models_by_name[name] = parse_model_file(path, compiled_dir=compiled_dir)
        except Exception:  # noqa: BLE001, S110
            pass

    # Second pass: upgrade SELECT * models using YAML/manifest column declarations.
    # This runs over ALL parsed models (not just target_names) so upstream models
    # referenced via ref() can in turn be used to resolve downstream stars.
    for parsed_model in sql_models_by_name.values():
        if parsed_model.has_star:
            resolved_cols = _resolve_star_with_registry(parsed_model, yaml_registry, manifest, sql_models_by_name)
            if resolved_cols is not None:
                parsed_model.output_columns = resolved_cols
                parsed_model.has_star = False
                parsed_model.star_resolved = True

    report = ValidationReport()

    # Run checks per model
    for name in target_names:
        parsed = sql_models_by_name.get(name)

        if "yaml_sql_sync" not in skipped and parsed is not None:
            _check_yaml_sql_sync(name, yaml_registry, parsed, report)

        if "upstream_refs" not in skipped and parsed is not None:
            _check_upstream_refs(name, parsed, yaml_registry, manifest, sql_models_by_name, report)

        if "docs_coverage" not in skipped:
            _check_docs_coverage(name, yaml_registry, report)

        if "select_star" not in skipped and parsed is not None:
            _check_select_star(name, parsed, report)

    # YAML integrity is a global check — run once
    if "yaml_integrity" not in skipped:
        _check_yaml_integrity(yaml_registry, sql_model_names, report)

    # Output
    if output_format == "json":
        _print_json_report(report, errors_only=errors_only)
    else:
        _print_text_report(report, errors_only=errors_only)
        n_errors = len(report.errors)
        n_warnings = len(report.warnings)
        suppressed = f", {n_warnings} warning(s) suppressed" if errors_only and n_warnings else ""
        summary = (
            f"\n[bold]Summary:[/] {n_errors} error(s), {n_warnings} warning(s)"
            f" across {len(report.issues)} issue(s){suppressed}."
        )
        console.print(summary)

    if report.errors:
        sys.exit(1)
