"""Export ignore-list — keep curated exclusions across ``ol-superset export``.

``ol-superset export`` pulls every asset from the live instance additively, so
dashboards/charts we deliberately drop from the repo (e.g. the unmaintained
``MM_Fin_*`` dashboards removed in commit ce937648) silently reappear on the
next export. A checked-in ``.exportignore`` (gitignore-style glob patterns) lets
those exclusions stick: after the pull the matching asset files are pruned.

Matching is against the asset's *identity* fields (dashboard title, chart name,
UUID) and its filename, so it survives dedupe's UUID-based renames. Pruning is
dependency-consistent: removing a dashboard also removes any chart that becomes
orphaned (referenced only by removed dashboards) — mirroring how the assets were
removed by hand. Datasets are never auto-removed: they are the physical layer,
are shared across charts, and are validated separately by ``ol-superset validate
--dbt-dir``.
"""

from __future__ import annotations

import fnmatch
from dataclasses import dataclass, field
from pathlib import Path

from ol_superset.lib.asset_index import build_asset_index
from ol_superset.lib.utils import get_repo_root

IGNORE_FILENAME = ".exportignore"


def get_ignore_path() -> Path:
    """Path to the checked-in ``.exportignore`` (repo root of the package)."""
    return get_repo_root() / IGNORE_FILENAME


def load_ignore_patterns(path: Path | None = None) -> list[str]:
    """
    Read glob patterns from ``.exportignore``.

    Gitignore-style: one pattern per line, ``#`` comments and blank lines
    ignored, surrounding whitespace stripped. Returns ``[]`` if the file does
    not exist.
    """
    ignore_path = path if path is not None else get_ignore_path()
    if not ignore_path.exists():
        return []
    patterns: list[str] = []
    for raw in ignore_path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        patterns.append(line)
    return patterns


def _matches(patterns: list[str], *candidates: str) -> bool:
    """True if any pattern matches any non-empty candidate string."""
    for pattern in patterns:
        for candidate in candidates:
            if candidate and fnmatch.fnmatch(candidate, pattern):
                return True
    return False


@dataclass
class PruneReport:
    """Result of pruning ignored assets from an exported bundle."""

    removed_dashboards: list[Path] = field(default_factory=list)
    removed_charts: list[Path] = field(default_factory=list)
    # Charts removed because every dashboard referencing them was removed
    # (as opposed to matching a pattern by name/uuid directly).
    orphaned_charts: list[Path] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.removed_dashboards) + len(self.removed_charts)


def plan_prune(assets_dir: Path, patterns: list[str]) -> PruneReport:
    """
    Compute which asset files an ``.exportignore`` would remove (no deletion).

    A dashboard is removed when a pattern matches its title, UUID, or filename.
    A chart is removed when a pattern matches its name, UUID, or filename
    directly, OR when it is orphaned — every dashboard that references it is
    itself removed and at least one such dashboard exists. Charts referenced by
    a surviving dashboard are always kept, even if orphaned charts of the same
    name would otherwise match.
    """
    report = PruneReport()
    if not patterns:
        return report

    index = build_asset_index(assets_dir)

    removed_dashboard_uuids: set[str] = set()
    for uuid, dash in index.dashboards.items():
        if _matches(patterns, dash.title, uuid, dash.path.name):
            report.removed_dashboards.append(dash.path)
            removed_dashboard_uuids.add(uuid)

    # Which surviving dashboards still reference each chart.
    surviving_refs: dict[str, int] = {}
    for uuid, dash in index.dashboards.items():
        if uuid in removed_dashboard_uuids:
            continue
        for chart_uuid in dash.chart_uuids:
            surviving_refs[chart_uuid] = surviving_refs.get(chart_uuid, 0) + 1

    charts_on_removed: set[str] = set()
    for uuid in removed_dashboard_uuids:
        charts_on_removed.update(index.dashboards[uuid].chart_uuids)

    for uuid, chart in index.charts.items():
        direct = _matches(patterns, chart.name, uuid, chart.path.name)
        orphaned = uuid in charts_on_removed and surviving_refs.get(uuid, 0) == 0
        if direct:
            report.removed_charts.append(chart.path)
        elif orphaned:
            report.removed_charts.append(chart.path)
            report.orphaned_charts.append(chart.path)

    return report


def prune_ignored_assets(
    assets_dir: Path, patterns: list[str], *, dry_run: bool = False
) -> PruneReport:
    """
    Delete exported asset files matching ``.exportignore`` patterns.

    Returns a :class:`PruneReport` of what was (or would be, when ``dry_run``)
    removed. Datasets are never touched.
    """
    report = plan_prune(assets_dir, patterns)
    if not dry_run:
        for path in (*report.removed_dashboards, *report.removed_charts):
            path.unlink(missing_ok=True)
    return report
