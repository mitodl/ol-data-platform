"""Impact report — git-change blast radius and orphan detection for Superset assets."""

from __future__ import annotations

import json
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any

import yaml
from cyclopts import Parameter

from ol_superset.lib.asset_index import (
    AssetIndex,
    ChartAsset,
    DashboardAsset,
    DatasetAsset,
    build_asset_index,
)
from ol_superset.lib.dbt_registry import DbtModel, DbtRegistry, build_dbt_registry
from ol_superset.lib.utils import get_assets_dir

_UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


# ---------------------------------------------------------------------------
# Reverse-dependency index
# ---------------------------------------------------------------------------


@dataclass
class _ReverseIndex:
    """Reverse dependency maps used for forward impact tracing."""

    # physical dataset table_name → dataset UUIDs backed by that dbt model
    model_to_datasets: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # dataset UUID → chart UUIDs that reference it
    dataset_to_charts: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # chart UUID → dashboard UUIDs that contain it
    chart_to_dashboards: dict[str, list[str]] = field(
        default_factory=lambda: defaultdict(list)
    )
    # Sets used for orphan detection
    referenced_chart_uuids: set[str] = field(default_factory=set)
    referenced_dataset_uuids: set[str] = field(default_factory=set)


def _build_reverse_index(index: AssetIndex) -> _ReverseIndex:
    ri = _ReverseIndex()
    for ds in index.datasets.values():
        if not ds.sql:  # physical dataset — key is the dbt model name
            ri.model_to_datasets[ds.table_name].append(ds.uuid)
    for chart in index.charts.values():
        if chart.dataset_uuid:
            ri.dataset_to_charts[chart.dataset_uuid].append(chart.uuid)
            ri.referenced_dataset_uuids.add(chart.dataset_uuid)
    for dash in index.dashboards.values():
        for chart_uuid in dash.chart_uuids:
            ri.chart_to_dashboards[chart_uuid].append(dash.uuid)
            ri.referenced_chart_uuids.add(chart_uuid)
    return ri


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------


def _git_repo_root() -> Path:
    """Return the absolute path of the current git repository root."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],  # noqa: S603, S607
            capture_output=True,
            text=True,
            check=True,
        )
        return Path(result.stdout.strip())
    except subprocess.CalledProcessError as exc:
        print(
            f"Error: not inside a git repository: {exc.stderr.strip()}",
            file=sys.stderr,
        )
        sys.exit(1)
    except FileNotFoundError:
        print("Error: git not found in PATH", file=sys.stderr)
        sys.exit(1)


def _get_changed_files(
    base_ref: str | None, staged_only: bool
) -> tuple[list[str], str]:
    """
    Return (list of changed repo-relative file paths, human-readable description).

    Args:
        base_ref:    If given, diff HEAD against this ref (e.g. ``origin/main``).
        staged_only: If True (and base_ref is None), only staged changes vs HEAD.
    """
    if base_ref:
        cmd = ["git", "diff", "--name-only", base_ref, "HEAD"]  # noqa: S603, S607
        description = f"HEAD vs {base_ref}"
    elif staged_only:
        cmd = ["git", "diff", "--cached", "--name-only"]  # noqa: S603
        description = "staged changes vs HEAD"
    else:
        cmd = ["git", "diff", "--name-only", "HEAD"]  # noqa: S603
        description = "working tree vs HEAD"

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)  # noqa: S603
        files = [f for f in result.stdout.strip().splitlines() if f]
        return files, description
    except subprocess.CalledProcessError as exc:
        print(f"Error: git diff failed: {exc.stderr.strip()}", file=sys.stderr)
        sys.exit(1)


# ---------------------------------------------------------------------------
# File classification
# ---------------------------------------------------------------------------


@dataclass
class _ChangedAssets:
    dbt_models: list[str] = field(default_factory=list)   # model names (stems)
    dataset_uuids: list[str] = field(default_factory=list)
    chart_uuids: list[str] = field(default_factory=list)
    dashboard_uuids: list[str] = field(default_factory=list)
    other_files: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return (
            len(self.dbt_models)
            + len(self.dataset_uuids)
            + len(self.chart_uuids)
            + len(self.dashboard_uuids)
            + len(self.other_files)
        )


def _extract_uuid(stem: str) -> str | None:
    m = _UUID_RE.search(stem)
    return m.group() if m else None


def _parse_dbt_schema_model_names(path: Path) -> list[str]:
    """Extract model names from a dbt ``_*.yml`` schema file."""
    try:
        data: Any = yaml.safe_load(path.read_text())
    except Exception:  # noqa: BLE001
        return []
    if not isinstance(data, dict):
        return []
    return [
        m["name"]
        for m in data.get("models", [])
        if isinstance(m, dict) and m.get("name")
    ]


def _classify_changed_files(
    files: list[str],
    repo_root: Path,
    assets_dir: Path,
    dbt_dir: Path | None,
) -> _ChangedAssets:
    """Map each changed file path to the Superset/dbt asset it represents."""
    changed = _ChangedAssets()
    datasets_dir = assets_dir / "datasets"
    charts_dir = assets_dir / "charts"
    dashboards_dir = assets_dir / "dashboards"
    dbt_models_dir = (dbt_dir / "models") if dbt_dir else None

    for rel in files:
        abs_path = repo_root / rel

        if abs_path.suffix in {".yaml", ".yml"}:
            if abs_path.is_relative_to(datasets_dir):
                uuid = _extract_uuid(abs_path.stem)
                (changed.dataset_uuids if uuid else changed.other_files).append(
                    uuid or rel
                )
            elif abs_path.is_relative_to(charts_dir):
                uuid = _extract_uuid(abs_path.stem)
                (changed.chart_uuids if uuid else changed.other_files).append(
                    uuid or rel
                )
            elif abs_path.is_relative_to(dashboards_dir):
                uuid = _extract_uuid(abs_path.stem)
                (changed.dashboard_uuids if uuid else changed.other_files).append(
                    uuid or rel
                )
            elif (
                dbt_models_dir is not None
                and abs_path.is_relative_to(dbt_models_dir)
                and abs_path.name.startswith("_")
            ):
                # dbt schema file — parse to get model names it defines
                changed.dbt_models.extend(_parse_dbt_schema_model_names(abs_path))
            else:
                changed.other_files.append(rel)

        elif abs_path.suffix == ".sql" and dbt_models_dir is not None:
            if abs_path.is_relative_to(dbt_models_dir):
                changed.dbt_models.append(abs_path.stem)
            else:
                changed.other_files.append(rel)

        else:
            changed.other_files.append(rel)

    return changed


# ---------------------------------------------------------------------------
# Impact tracing
# ---------------------------------------------------------------------------


@dataclass
class _ImpactedAssets:
    datasets: dict[str, DatasetAsset | None] = field(default_factory=dict)
    charts: dict[str, ChartAsset | None] = field(default_factory=dict)
    dashboards: dict[str, DashboardAsset | None] = field(default_factory=dict)


def _trace_impact(
    changed: _ChangedAssets,
    index: AssetIndex,
    ri: _ReverseIndex,
) -> _ImpactedAssets:
    """Walk the reverse graph to find all transitively affected assets."""
    affected_ds: set[str] = set()
    affected_ch: set[str] = set()
    affected_db: set[str] = set()

    # dbt model → datasets
    for model_name in changed.dbt_models:
        for ds_uuid in ri.model_to_datasets.get(model_name, []):
            affected_ds.add(ds_uuid)

    # directly changed datasets (absorb transitive from dbt too)
    affected_ds.update(changed.dataset_uuids)

    # datasets → charts
    for ds_uuid in affected_ds:
        for ch_uuid in ri.dataset_to_charts.get(ds_uuid, []):
            affected_ch.add(ch_uuid)

    # directly changed charts
    affected_ch.update(changed.chart_uuids)

    # charts → dashboards
    for ch_uuid in affected_ch:
        for db_uuid in ri.chart_to_dashboards.get(ch_uuid, []):
            affected_db.add(db_uuid)

    # directly changed dashboards
    affected_db.update(changed.dashboard_uuids)

    return _ImpactedAssets(
        datasets={uuid: index.datasets.get(uuid) for uuid in affected_ds},
        charts={uuid: index.charts.get(uuid) for uuid in affected_ch},
        dashboards={uuid: index.dashboards.get(uuid) for uuid in affected_db},
    )


# ---------------------------------------------------------------------------
# Orphan detection
# ---------------------------------------------------------------------------


@dataclass
class _OrphanAssets:
    charts: list[ChartAsset] = field(default_factory=list)
    datasets: list[DatasetAsset] = field(default_factory=list)


def _find_orphans(index: AssetIndex, ri: _ReverseIndex) -> _OrphanAssets:
    """Find charts not in any dashboard, and datasets not in any chart."""
    orphans = _OrphanAssets()
    for chart_uuid, chart in sorted(index.charts.items(), key=lambda kv: kv[1].name):
        if chart_uuid not in ri.referenced_chart_uuids:
            orphans.charts.append(chart)
    for ds_uuid, ds in sorted(
        index.datasets.items(), key=lambda kv: kv[1].table_name
    ):
        if ds_uuid not in ri.referenced_dataset_uuids:
            orphans.datasets.append(ds)
    return orphans


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def _render_text(
    description: str,
    changed: _ChangedAssets,
    impact: _ImpactedAssets,
    index: AssetIndex,
    ri: _ReverseIndex,
    dbt_registry: DbtRegistry | None,
    orphans: _OrphanAssets | None,
    raw_files: list[str],
) -> None:
    total_files = len(raw_files)
    print(f"Impact Report  ({description} — {total_files} file(s) changed)")
    print("=" * 72)

    # ── Changed assets summary ──────────────────────────────────────────────
    if changed.dbt_models:
        print(f"\nChanged dbt models ({len(changed.dbt_models)}):")
        for name in sorted(set(changed.dbt_models)):
            layer = ""
            if dbt_registry:
                m = dbt_registry.get_model(name)
                layer = f"  [{m.layer}]" if m else "  [unknown layer]"
            print(f"  🧱 {name}{layer}")

    if changed.dataset_uuids:
        print(f"\nChanged datasets ({len(changed.dataset_uuids)}):")
        for uuid in changed.dataset_uuids:
            ds = index.datasets.get(uuid)
            label = ds.table_name if ds else uuid
            print(f"  🗄️  {label}")

    if changed.chart_uuids:
        print(f"\nChanged charts ({len(changed.chart_uuids)}):")
        for uuid in changed.chart_uuids:
            ch = index.charts.get(uuid)
            label = ch.name if ch else uuid
            print(f"  📈 {label}")

    if changed.dashboard_uuids:
        print(f"\nChanged dashboards ({len(changed.dashboard_uuids)}):")
        for uuid in changed.dashboard_uuids:
            db = index.dashboards.get(uuid)
            label = db.title if db else uuid
            print(f"  📊 {label}")

    has_classified = (
        changed.dbt_models
        or changed.dataset_uuids
        or changed.chart_uuids
        or changed.dashboard_uuids
    )
    if not has_classified:
        print("\nNo Superset or dbt model files changed.")
        if changed.other_files:
            print(f"  ({total_files} other file(s) changed — no asset impact)")

    # ── Impact propagation ──────────────────────────────────────────────────
    print()
    print("Downstream Impact")
    print("-" * 72)

    if not has_classified:
        print("  (nothing to trace)")
    else:
        _print_impact_detail(changed, impact, index, ri, dbt_registry)

    # ── Summary counts ──────────────────────────────────────────────────────
    n_ds = len(impact.datasets)
    n_ch = len(impact.charts)
    n_db = len(impact.dashboards)
    if has_classified and (n_ds or n_ch or n_db):
        print()
        print("Summary:")
        print(f"  🗄️   {n_ds} dataset(s) affected")
        print(f"  📈  {n_ch} chart(s) affected")
        print(f"  📊  {n_db} dashboard(s) affected")
    elif has_classified:
        print("\n  ✅ No downstream Superset assets affected.")

    # ── Orphan report ───────────────────────────────────────────────────────
    if orphans is not None:
        print()
        print("Orphan Report")
        print("=" * 72)
        _print_orphans(orphans)


def _print_impact_detail(
    changed: _ChangedAssets,
    impact: _ImpactedAssets,
    index: AssetIndex,
    ri: _ReverseIndex,
    dbt_registry: DbtRegistry | None,
) -> None:
    """
    Print a tree showing how each changed item propagates to dashboards.

    dbt model → dataset(s) → chart(s) → dashboard(s)
    dataset   → chart(s)   → dashboard(s)
    chart     → dashboard(s)
    dashboard → (itself)
    """

    def _ds_label(ds: DatasetAsset | None, uuid: str) -> str:
        if ds is None:
            return f"[missing dataset {uuid[:8]}…]"
        tag = " [virtual]" if ds.sql else ""
        return f"{ds.table_name}{tag}  [{ds.schema}]"

    def _ch_label(ch: ChartAsset | None, uuid: str) -> str:
        return ch.name if ch else f"[missing chart {uuid[:8]}…]"

    def _db_label(db: DashboardAsset | None, uuid: str) -> str:
        return db.title if db else f"[missing dashboard {uuid[:8]}…]"

    def _print_chart_subtree(ch_uuid: str, indent: str) -> None:
        ch = index.charts.get(ch_uuid)
        print(f"{indent}📈 {_ch_label(ch, ch_uuid)}")
        for db_uuid in sorted(ri.chart_to_dashboards.get(ch_uuid, [])):
            db = index.dashboards.get(db_uuid)
            print(f"{indent}   📊 {_db_label(db, db_uuid)}")

    def _print_dataset_subtree(ds_uuid: str, indent: str) -> None:
        ds = index.datasets.get(ds_uuid)
        print(f"{indent}🗄️  {_ds_label(ds, ds_uuid)}")
        ch_uuids = sorted(ri.dataset_to_charts.get(ds_uuid, []))
        if not ch_uuids:
            print(f"{indent}   (no charts reference this dataset)")
        for ch_uuid in ch_uuids:
            _print_chart_subtree(ch_uuid, indent + "   ")

    # dbt models → datasets → charts → dashboards
    for model_name in sorted(set(changed.dbt_models)):
        layer = ""
        if dbt_registry:
            m: DbtModel | None = dbt_registry.get_model(model_name)
            layer = f"  [{m.layer}]" if m else "  [unknown]"
        print(f"\n🧱 dbt model: {model_name}{layer}")
        ds_uuids = sorted(ri.model_to_datasets.get(model_name, []))
        if not ds_uuids:
            print("   (no datasets backed by this model)")
        for ds_uuid in ds_uuids:
            _print_dataset_subtree(ds_uuid, "   ")

    # directly changed datasets → charts → dashboards
    for ds_uuid in changed.dataset_uuids:
        ds = index.datasets.get(ds_uuid)
        print(f"\n🗄️  changed dataset: {_ds_label(ds, ds_uuid)}")
        for ch_uuid in sorted(ri.dataset_to_charts.get(ds_uuid, [])):
            _print_chart_subtree(ch_uuid, "   ")

    # directly changed charts → dashboards
    for ch_uuid in changed.chart_uuids:
        ch = index.charts.get(ch_uuid)
        print(f"\n📈 changed chart: {_ch_label(ch, ch_uuid)}")
        for db_uuid in sorted(ri.chart_to_dashboards.get(ch_uuid, [])):
            db = index.dashboards.get(db_uuid)
            print(f"   📊 {_db_label(db, db_uuid)}")

    # directly changed dashboards
    for db_uuid in changed.dashboard_uuids:
        db = index.dashboards.get(db_uuid)
        print(f"\n📊 changed dashboard: {_db_label(db, db_uuid)}")


def _print_orphans(orphans: _OrphanAssets) -> None:
    if orphans.charts:
        print(
            f"\n⚠️  Orphaned charts ({len(orphans.charts)})"
            " — not referenced by any dashboard:"
        )
        for ch in orphans.charts:
            ds_label = ""
            if ch.dataset_uuid:
                ds_label = f"  (dataset uuid: {ch.dataset_uuid[:8]}…)"
            print(f"  📈 {ch.name}{ds_label}")
            print(f"     {ch.path.name}")
    else:
        print("\n✅ No orphaned charts.")

    if orphans.datasets:
        print(
            f"\n⚠️  Orphaned datasets ({len(orphans.datasets)})"
            " — not referenced by any chart:"
        )
        for ds in orphans.datasets:
            virtual_tag = " [virtual]" if ds.sql else ""
            print(f"  🗄️  {ds.table_name}{virtual_tag}  [{ds.schema}]")
            print(f"     {ds.path.name}")
    else:
        print("\n✅ No orphaned datasets.")


def _render_json(
    description: str,
    changed: _ChangedAssets,
    impact: _ImpactedAssets,
    index: AssetIndex,
    orphans: _OrphanAssets | None,
    raw_files: list[str],
) -> None:
    def _ds_dict(uuid: str, ds: DatasetAsset | None) -> dict:
        if ds is None:
            return {
                "uuid": uuid, "table_name": None, "schema": None, "is_virtual": None
            }
        return {
            "uuid": uuid,
            "table_name": ds.table_name,
            "schema": ds.schema,
            "is_virtual": ds.sql is not None,
        }

    def _ch_dict(uuid: str, ch: ChartAsset | None) -> dict:
        if ch is None:
            return {"uuid": uuid, "name": None, "dataset_uuid": None}
        return {"uuid": uuid, "name": ch.name, "dataset_uuid": ch.dataset_uuid}

    def _db_dict(uuid: str, db: DashboardAsset | None) -> dict:
        if db is None:
            return {"uuid": uuid, "title": None}
        return {"uuid": uuid, "title": db.title}

    out: dict[str, Any] = {
        "comparison": description,
        "files_changed": len(raw_files),
        "changed": {
            "dbt_models": sorted(set(changed.dbt_models)),
            "datasets": [
                _ds_dict(uuid, index.datasets.get(uuid))
                for uuid in changed.dataset_uuids
            ],
            "charts": [
                _ch_dict(uuid, index.charts.get(uuid))
                for uuid in changed.chart_uuids
            ],
            "dashboards": [
                _db_dict(uuid, index.dashboards.get(uuid))
                for uuid in changed.dashboard_uuids
            ],
        },
        "impact": {
            "datasets": [_ds_dict(u, ds) for u, ds in impact.datasets.items()],
            "charts": [_ch_dict(u, ch) for u, ch in impact.charts.items()],
            "dashboards": [_db_dict(u, db) for u, db in impact.dashboards.items()],
        },
    }

    if orphans is not None:
        out["orphans"] = {
            "charts": [
                {
                    "uuid": ch.uuid,
                    "name": ch.name,
                    "dataset_uuid": ch.dataset_uuid,
                    "file": ch.path.name,
                }
                for ch in orphans.charts
            ],
            "datasets": [
                {
                    "uuid": ds.uuid,
                    "table_name": ds.table_name,
                    "schema": ds.schema,
                    "is_virtual": ds.sql is not None,
                    "file": ds.path.name,
                }
                for ds in orphans.datasets
            ],
        }

    print(json.dumps(out, indent=2))


# ---------------------------------------------------------------------------
# Command
# ---------------------------------------------------------------------------


def impact(
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"],
            help="Assets directory (default: assets/)",
        ),
    ] = None,
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-b"],
            help=(
                "Path to dbt project root (contains dbt_project.yml). "
                "When provided, dbt model changes are traced to datasets."
            ),
        ),
    ] = None,
    base_ref: Annotated[
        str | None,
        Parameter(
            name=["--base", "-B"],
            help=(
                "Git ref to diff against (e.g. origin/main, main~5). "
                "When omitted, compares the working tree (staged + unstaged) to HEAD."
            ),
        ),
    ] = None,
    staged_only: Annotated[
        bool,
        Parameter(
            name=["--staged", "-s"],
            help="Only consider staged changes (vs HEAD). Ignored when --base is set.",
        ),
    ] = False,
    show_orphans: Annotated[
        bool,
        Parameter(
            name=["--orphans", "-o"],
            help=(
                "Also report orphaned charts (not in any dashboard) "
                "and orphaned datasets (not used by any chart)."
            ),
        ),
    ] = False,
    output_format: Annotated[
        str,
        Parameter(
            name=["--format", "-f"],
            help="Output format: text (default) or json.",
        ),
    ] = "text",
) -> None:
    """
    Report which Superset assets are affected by pending git changes.

    Classifies changed files as dbt models, datasets, charts, or dashboards,
    then walks the reverse dependency graph to show every asset that will be
    impacted when those changes reach a deployed Superset instance.

    Optionally (--orphans) reports charts not referenced by any dashboard and
    datasets not referenced by any chart — useful for identifying dead assets
    before a clean-up PR.

    Examples:
        ol-superset impact
        ol-superset impact --base origin/main --dbt-dir ../../ol_dbt
        ol-superset impact --staged --orphans
        ol-superset impact --orphans --format json | jq '.orphans'
        ol-superset impact --base main~5 --format json
    """
    assets_dir = get_assets_dir(assets_dir_path)
    if not assets_dir.exists():
        print(f"Error: Assets directory not found: {assets_dir}", file=sys.stderr)
        sys.exit(1)

    dbt_dir: Path | None = None
    dbt_registry: DbtRegistry | None = None
    if dbt_dir_path is not None:
        dbt_dir = Path(dbt_dir_path).resolve()
        if not dbt_dir.exists():
            print(f"Error: dbt directory not found: {dbt_dir}", file=sys.stderr)
            sys.exit(1)
        try:
            dbt_registry = build_dbt_registry(dbt_dir)
        except Exception as exc:  # noqa: BLE001
            print(f"Error: Failed to load dbt registry: {exc}", file=sys.stderr)
            sys.exit(1)

    repo_root = _git_repo_root()
    raw_files, description = _get_changed_files(base_ref, staged_only)
    index = build_asset_index(assets_dir)
    ri = _build_reverse_index(index)

    changed = _classify_changed_files(
        raw_files, repo_root, assets_dir.resolve(), dbt_dir
    )
    impact_result = _trace_impact(changed, index, ri)
    orphans = _find_orphans(index, ri) if show_orphans else None

    if output_format == "json":
        _render_json(description, changed, impact_result, index, orphans, raw_files)
    else:
        _render_text(
            description,
            changed,
            impact_result,
            index,
            ri,
            dbt_registry,
            orphans,
            raw_files,
        )
