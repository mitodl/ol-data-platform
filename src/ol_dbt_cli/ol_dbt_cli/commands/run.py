"""Smart dbt execution with automatic state-based incremental development.

Wraps ``dbt run``, ``dbt build``, and ``dbt test`` with automatic state management
so that only changed or previously errored nodes are re-executed on each iteration.

Workflow:
  1. First run (no state): full execution saves artifacts to ``.dbt-state/``
  2. Subsequent runs: selects only ``state:modified+`` (changed models) and
     ``result:error+ result:fail+`` (previously errored nodes), with ``--defer``
     so upstream refs resolve against the saved state rather than requiring a
     full local build.

State artifacts stored in ``.dbt-state/``:
  - ``manifest.json``   — node graph used by ``state:modified`` selector
  - ``run_results.json`` — execution results used by ``result:error`` selector

Usage examples::

    ol-dbt run                         # incremental build (changed + errored)
    ol-dbt run --full-refresh          # rebuild everything, refresh state
    ol-dbt run --select my_model+      # explicit selection + defer
    ol-dbt run test                    # run tests incrementally
    ol-dbt run run                     # dbt run (models only) incrementally
    ol-dbt run --no-save-state         # skip saving artifacts after run
    ol-dbt run --target dev_production # override dbt target
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path
from typing import Annotated, Literal

import cyclopts
from cyclopts import Parameter
from rich.console import Console

from ol_dbt_cli.lib.git_utils import get_repo_root

console = Console()
err_console = Console(stderr=True)

run_app = cyclopts.App(
    name="run",
    help="Smart dbt execution with automatic state-based incremental development.",
)

DEFAULT_STATE_DIR = ".dbt-state"
STATE_ARTIFACTS = ("manifest.json", "run_results.json")


def _find_dbt_dir(dbt_dir_path: str | None) -> Path:
    """Resolve the dbt project root directory."""
    if dbt_dir_path:
        return Path(dbt_dir_path).resolve()
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


def _state_artifacts_available(state_dir: Path) -> dict[str, bool]:
    """Return which state artifacts are present in *state_dir*."""
    return {name: (state_dir / name).exists() for name in STATE_ARTIFACTS}


def _save_artifacts(target_dir: Path, state_dir: Path) -> list[str]:
    """Copy dbt artifacts from *target_dir* to *state_dir*. Returns list of saved files."""
    state_dir.mkdir(parents=True, exist_ok=True)
    saved: list[str] = []
    for name in STATE_ARTIFACTS:
        src = target_dir / name
        if src.exists():
            shutil.copy2(src, state_dir / name)
            saved.append(name)
    return saved


def _build_dbt_command(
    subcommand: str,
    state_dir: Path,
    available: dict[str, bool],
    select: str | None,
    full_refresh: bool,
    defer: bool,
    target: str | None,
    vars: str | None,
    extra_args: list[str],
) -> list[str]:
    """Construct the dbt CLI command list."""
    cmd: list[str] = ["dbt", subcommand]

    has_manifest = available.get("manifest.json", False)
    has_results = available.get("run_results.json", False)
    use_state = (has_manifest or has_results) and not full_refresh

    if select is not None:
        # Explicit selection — always honour it; still enable defer if state exists
        cmd += ["--select", select]
    elif use_state:
        # Build the incremental selection expression
        parts: list[str] = []
        if has_manifest:
            parts.append("state:modified+")
        if has_results:
            # result:error+ covers failed models (run/build) and failed tests (test/build)
            # result:fail+ covers tests with warn_error
            parts.append("result:error+")
            parts.append("result:fail+")
        if parts:
            cmd += ["--select", " ".join(parts)]

    # --state is required for ANY state-based selector (state:modified+, result:error+).
    # --defer is optional and additionally requires manifest.json.
    if use_state:
        cmd += ["--state", str(state_dir)]
        if defer and has_manifest:
            cmd.append("--defer")

    if full_refresh:
        cmd.append("--full-refresh")

    if target:
        cmd += ["--target", target]

    if vars:
        cmd += ["--vars", vars]

    cmd += extra_args

    return cmd


@run_app.default
def run(  # noqa: PLR0913
    subcommand: Annotated[
        Literal["build", "run", "test"],
        Parameter(
            show_default=True,
            help="dbt subcommand to execute: build (models+tests), run (models only), or test.",
        ),
    ] = "build",
    *,
    target: Annotated[
        str | None,
        Parameter(name=["--target", "-t"], help="dbt target profile to use (e.g. dev_qa, dev_production)."),
    ] = None,
    select: Annotated[
        str | None,
        Parameter(
            name=["--select", "-s"],
            help=(
                "Explicit node selection string. "
                "When omitted and state exists, selects changed + errored nodes automatically."
            ),
        ),
    ] = None,
    full_refresh: Annotated[
        bool,
        Parameter(name="--full-refresh", help="Force a complete rebuild of all models, ignoring state."),
    ] = False,
    defer: Annotated[
        bool,
        Parameter(
            name="--defer",
            help=(
                "When state exists, defer upstream refs to the state manifest "
                "so you don't need to rebuild all parents locally. Default: enabled."
            ),
        ),
    ] = True,
    save_state: Annotated[
        bool,
        Parameter(
            name="--save-state",
            help="Save manifest.json and run_results.json to the state directory after running. Default: enabled.",
        ),
    ] = True,
    state_dir: Annotated[
        str | None,
        Parameter(
            name="--state-dir",
            help=f"Directory to read/write state artifacts. Default: {{dbt-project}}/{DEFAULT_STATE_DIR}/",
        ),
    ] = None,
    vars: Annotated[
        str | None,
        Parameter(name="--vars", help='dbt variables as a YAML/JSON string, e.g. \'{"schema_suffix": "alice"}\'.'),
    ] = None,
    project_dir: Annotated[
        str | None,
        Parameter(
            name="--project-dir",
            help="Path to the dbt project root (must contain dbt_project.yml). Defaults to src/ol_dbt.",
        ),
    ] = None,
) -> None:
    """Execute dbt incrementally using state-based selection.

    On the first run (no saved state) all selected nodes are executed and
    the resulting artifacts are saved.  On every subsequent run only models
    that have changed since the last run *or* that failed last time are
    re-executed, with upstream refs deferred to the saved manifest so you
    don't need to rebuild unrelated parents.

    Pass ``--full-refresh`` to ignore state and rebuild everything (state is
    still updated afterwards so the next run can be incremental again).
    """
    try:
        dbt_dir = _find_dbt_dir(project_dir)
    except RuntimeError as exc:
        err_console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    resolved_state_dir = Path(state_dir).expanduser().resolve() if state_dir else dbt_dir / DEFAULT_STATE_DIR
    target_dir = dbt_dir / "target"
    available = _state_artifacts_available(resolved_state_dir)
    has_any_state = any(available.values())

    # ── Status banner ──────────────────────────────────────────────────────
    console.print(f"\n[bold]ol-dbt run[/] [dim]{subcommand}[/] — dbt project: [cyan]{dbt_dir}[/]")
    if full_refresh:
        console.print("[yellow]⟳  Full refresh — ignoring saved state[/]")
    elif has_any_state and select is None:
        parts: list[str] = []
        if available.get("manifest.json"):
            parts.append("[green]state:modified+[/]")
        if available.get("run_results.json"):
            parts.append("[yellow]result:error+[/] [yellow]result:fail+[/]")
        console.print(f"[dim]State dir:[/] {resolved_state_dir}")
        console.print(f"Incremental selection: {' '.join(parts)}")
        if defer and available.get("manifest.json"):
            console.print("[dim]Upstream refs deferred to state manifest[/]")
        elif defer and not available.get("manifest.json"):
            console.print(
                "[yellow]Note:[/] --defer requested but manifest.json not found in state dir; deferral skipped"
            )
    elif select is not None:
        console.print(f"Explicit selection: [cyan]{select}[/]")
        if has_any_state and defer and available.get("manifest.json"):
            console.print("[dim]Upstream refs deferred to state manifest[/]")
        elif defer and not available.get("manifest.json"):
            console.print("[yellow]Note:[/] --defer requested but no manifest.json in state dir; deferral skipped")
    else:
        console.print("[dim]No saved state — running full build and initialising state[/]")
        if defer:
            console.print("[dim]--defer has no effect on first run (no state manifest yet)[/]")

    # ── Build and run command ──────────────────────────────────────────────
    cmd = _build_dbt_command(
        subcommand=subcommand,
        state_dir=resolved_state_dir,
        available=available,
        select=select,
        full_refresh=full_refresh,
        defer=defer,
        target=target,
        vars=vars,
        extra_args=[],
    )

    console.print(f"\n[dim]$ {' '.join(cmd)}[/]\n")

    result = subprocess.run(cmd, cwd=str(dbt_dir))  # noqa: S603

    # ── Save artifacts ─────────────────────────────────────────────────────
    # Always save when requested — the manifest reflects current code state
    # (needed for state:modified+ on the next run) and run_results captures
    # failures (needed for result:error+ on the next run). Saving on non-zero
    # exit is intentional so the next incremental run can target what failed.
    if save_state:
        saved = _save_artifacts(target_dir, resolved_state_dir)
        if saved:
            status = "[green]✓[/]" if result.returncode == 0 else "[yellow]⚠ (run failed)[/]"
            console.print(f"\n[dim]State saved {status}:[/] {', '.join(saved)} → {resolved_state_dir}")
        else:
            console.print("\n[yellow]Warning:[/] No artifacts found in target/ to save.")

    sys.exit(result.returncode)
