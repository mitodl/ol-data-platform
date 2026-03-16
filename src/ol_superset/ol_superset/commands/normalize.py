"""Normalize exported Superset YAML assets to reduce diff noise.

This module cleans up fields that cause erroneous diffs between exports:

  - ``query_context`` on charts: a massive JSON blob that duplicates ``params``
    and embeds environment-specific numeric datasource IDs. Verified against
    Superset source: if absent, the import pipeline skips its surgical update
    and the frontend regenerates it on first load — no functional impact.

  - ``params.datasource`` / ``params.slice_id`` on charts: environment-specific
    numeric IDs. Superset's ``update_chart_config_dataset()`` overwrites
    ``params.datasource`` from ``dataset_uuid`` at import time; ``Slice.form_data``
    always sets ``slice_id`` from the current ``Slice.id`` column at runtime.

  - ``params.dashboards`` on charts: list of numeric dashboard IDs that differ
    between environments. ``import_chart()`` never reads this field — chart↔
    dashboard relationships are derived from dashboard definitions only.

  - ``params.cache_timeout`` on charts: duplicated from the top-level
    ``cache_timeout`` field. ``Slice.form_data`` always overwrites
    ``params.cache_timeout`` from the ``Slice.cache_timeout`` column at runtime.

  - Non-deterministic dict key ordering inside ``params``: sorting keys
    alphabetically makes diffs reflect only intentional changes.

  - Opaque UUID references (``dataset_uuid``, ``database_uuid``): annotated
    with a ``# <human-readable name>`` inline comment so reviewers can identify
    what each UUID points to without cross-referencing other files.
"""

import re
from pathlib import Path

import cyclopts
import yaml
from rich.console import Console

console = Console()

# Pattern that matches the trailing UUID in a Superset export filename, e.g.
# "My_Chart_1e30a45b-d97d-4f44-ac2c-1d9de9e05bc1"
_UUID_SUFFIX_RE = re.compile(
    r"_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Keys stripped from ``params`` dicts on chart YAMLs because they are
# environment-specific, redundant, or have no import-time meaning.
#
# Verified against Superset source (superset/commands/chart/importers/v1/):
#
# - ``datasource``: overwritten during import by ``update_chart_config_dataset()``
#   which resolves the dataset via ``dataset_uuid``.
# - ``slice_id``: overwritten at runtime by ``Slice.form_data`` which always sets
#   ``slice_id`` from the current ``Slice.id`` database column.
# - ``dashboards``: list of numeric dashboard IDs; ``import_chart()`` never reads
#   this field — chart↔dashboard relationships come from dashboard definitions.
# - ``cache_timeout``: duplicated from the top-level chart field; ``Slice.form_data``
#   always overwrites ``params.cache_timeout`` from the ``Slice.cache_timeout``
#   column, so the value in ``params`` is never authoritative.
_CHART_PARAMS_STRIP_KEYS: frozenset[str] = frozenset(
    {
        "datasource",  # e.g. "141__table" – overwritten from dataset_uuid at import
        "slice_id",  # numeric chart ID – overwritten from Slice.id at runtime
        "dashboards",  # numeric dashboard IDs – environment-specific, unused on import
        "cache_timeout",  # duplicated from top-level field – overwritten at runtime
    }
)


# ---------------------------------------------------------------------------
# UUID → name lookup helpers
# ---------------------------------------------------------------------------


def build_dataset_uuid_map(assets_dir: Path) -> dict[str, str]:
    """Return a mapping of dataset UUID → table_name from local dataset YAMLs."""
    mapping: dict[str, str] = {}
    datasets_dir = assets_dir / "datasets"
    if not datasets_dir.exists():
        return mapping
    for yaml_file in datasets_dir.rglob("*.yaml"):
        if yaml_file.name == "metadata.yaml":
            continue
        try:
            with yaml_file.open() as fh:
                data = yaml.safe_load(fh)
            if data and "uuid" in data and "table_name" in data:
                mapping[data["uuid"]] = data["table_name"]
        except Exception:  # noqa: BLE001, S110
            pass
    return mapping


def build_database_uuid_map(assets_dir: Path) -> dict[str, str]:
    """Return a mapping of database UUID → database_name from local database YAMLs."""
    mapping: dict[str, str] = {}
    db_dir = assets_dir / "databases"
    if not db_dir.exists():
        return mapping
    for yaml_file in db_dir.glob("*.yaml"):
        if yaml_file.name == "metadata.yaml":
            continue
        try:
            with yaml_file.open() as fh:
                data = yaml.safe_load(fh)
            if data and "uuid" in data and "database_name" in data:
                mapping[data["uuid"]] = data["database_name"]
        except Exception:  # noqa: BLE001, S110
            pass
    return mapping


# ---------------------------------------------------------------------------
# Per-file normalization
# ---------------------------------------------------------------------------


def _sorted_dict(d: dict[str, object]) -> dict[str, object]:
    """Recursively sort all dict keys alphabetically."""

    def _sort(v: object) -> object:
        if isinstance(v, dict):
            return {k: _sort(val) for k, val in sorted(v.items())}
        if isinstance(v, list):
            return [_sort(item) for item in v]
        return v

    return {k: _sort(val) for k, val in sorted(d.items())}


def _annotate_uuid_fields(
    content: str,
    uuid_comments: dict[str, str],
) -> str:
    """Add inline YAML comments to UUID-valued scalar fields.

    ``uuid_comments`` maps field_name → comment text, e.g.::

        {"dataset_uuid": "marts__combined_video_engagements"}

    The replacement is done on the serialised YAML text so that PyYAML's
    comment-unaware dump doesn't strip them.
    """
    for field_name, comment in uuid_comments.items():
        # Match lines like: ``dataset_uuid: <uuid>`` with optional existing comment
        pattern = rf"^({re.escape(field_name)}: [^\n#]+?)(\s*#[^\n]*)?\s*$"
        replacement = rf"\1  # {comment}"
        content = re.sub(pattern, replacement, content, flags=re.MULTILINE)
    return content


def _slice_name_from_filename(yaml_file: Path) -> str:
    """Derive a slice_name from the chart YAML filename.

    Superset export filenames follow the pattern ``<Slice_Name>_<uuid>.yaml``.
    We strip the UUID suffix and replace underscores with spaces to recover the
    original chart name, e.g. ``Problem_Engagement_by_Courserun_1e30a45b-…yaml``
    → ``"Problem Engagement by Courserun"``.
    """
    stem = yaml_file.stem  # e.g. "Problem_Engagement_by_Courserun_1e30a45b-..."
    name = _UUID_SUFFIX_RE.sub("", stem)  # strip trailing UUID segment
    return name.replace("_", " ")


def normalize_chart(
    yaml_file: Path,
    dataset_uuid_map: dict[str, str],
    *,
    dry_run: bool = False,
) -> bool:
    """Normalize a single chart YAML file.

    Returns True if any changes were made (or would be made in dry-run).
    """
    with yaml_file.open() as fh:
        original_text = fh.read()
        data = yaml.safe_load(original_text)

    if not isinstance(data, dict):
        return False

    # 1. Strip query_context
    data.pop("query_context", None)

    # 2. Strip env-specific numeric IDs from params
    if isinstance(data.get("params"), dict):
        for key in _CHART_PARAMS_STRIP_KEYS:
            data["params"].pop(key, None)

    # 3. Promote required top-level fields from params if absent.
    #    ImportV1ChartSchema marks both viz_type and slice_name as required=True.
    #    Superset occasionally exports charts where these fields live only inside
    #    params; promoting them here prevents import validation failures.
    raw_params = data.get("params")
    params: dict[str, object] = raw_params if isinstance(raw_params, dict) else {}
    if "viz_type" not in data and "viz_type" in params:
        data["viz_type"] = params["viz_type"]
    if "slice_name" not in data:
        data["slice_name"] = _slice_name_from_filename(yaml_file)

    # 4. Sort all dict keys alphabetically for deterministic output
    data = _sorted_dict(data)

    new_text = yaml.dump(
        data,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
    )

    # 5. Annotate dataset_uuid with the human-readable dataset name
    dataset_uuid = data.get("dataset_uuid")
    uuid_comments: dict[str, str] = {}
    if isinstance(dataset_uuid, str) and dataset_uuid in dataset_uuid_map:
        uuid_comments["dataset_uuid"] = dataset_uuid_map[dataset_uuid]

    new_text = _annotate_uuid_fields(new_text, uuid_comments)

    # Compare final output (including comments) against the file on disk
    changed = new_text.strip() != original_text.strip()

    if not dry_run and changed:
        yaml_file.write_text(new_text)

    return changed


def normalize_dataset(
    yaml_file: Path,
    database_uuid_map: dict[str, str],
    *,
    dry_run: bool = False,
) -> bool:
    """Normalize a single dataset YAML file.

    Returns True if any changes were made (or would be made in dry-run).
    """
    with yaml_file.open() as fh:
        original_text = fh.read()
        data = yaml.safe_load(original_text)

    if not isinstance(data, dict):
        return False

    # Sort all dict keys alphabetically
    data = _sorted_dict(data)

    new_text = yaml.dump(
        data,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
    )

    changed = new_text.strip() != original_text.strip()

    # Annotate database_uuid with the human-readable database name
    database_uuid = data.get("database_uuid")
    uuid_comments: dict[str, str] = {}
    if isinstance(database_uuid, str) and database_uuid in database_uuid_map:
        uuid_comments["database_uuid"] = database_uuid_map[database_uuid]

    new_text = _annotate_uuid_fields(new_text, uuid_comments)

    # Re-compare after annotation to ensure idempotency
    changed = new_text.strip() != original_text.strip()

    if not dry_run and changed:
        yaml_file.write_text(new_text)

    return changed


def normalize_dashboard(
    yaml_file: Path,
    *,
    dry_run: bool = False,
) -> bool:
    """Normalize a single dashboard YAML file (key sorting only).

    Returns True if any changes were made (or would be made in dry-run).
    """
    with yaml_file.open() as fh:
        original_text = fh.read()
        data = yaml.safe_load(original_text)

    if not isinstance(data, dict):
        return False

    data = _sorted_dict(data)

    new_text = yaml.dump(
        data,
        allow_unicode=True,
        default_flow_style=False,
        sort_keys=True,
    )

    changed = new_text.strip() != original_text.strip()

    if not dry_run and changed:
        yaml_file.write_text(new_text)

    return changed


# ---------------------------------------------------------------------------
# CLI command
# ---------------------------------------------------------------------------

normalize_app = cyclopts.App(
    name="normalize",
    help="""
    Normalize exported YAML assets to reduce erroneous diffs.

    Applies the following transformations to make git diffs reflect only
    intentional changes:

      Charts
        • Removes ``query_context`` — a large JSON blob that duplicates
          ``params`` and embeds environment-specific numeric IDs. Verified
          against Superset source: the import pipeline skips it when absent
          and the frontend regenerates it on first load.
        • Removes from ``params``: ``datasource``, ``slice_id``, ``dashboards``,
          and ``cache_timeout`` — all overwritten by Superset at import or
          runtime, and all sources of environment-specific diff noise.
        • Annotates ``dataset_uuid`` with ``# <table_name>`` so reviewers can
          identify the referenced dataset without opening another file.

      Datasets
        • Annotates ``database_uuid`` with ``# <database_name>``.

      All asset types
        • Sorts dict keys alphabetically throughout the YAML for deterministic
          output across successive exports.

    Examples:

      # Preview changes without writing (recommended first run)
      $ ol-superset normalize --dry-run

      # Normalize all assets in place
      $ ol-superset normalize

      # Normalize only charts
      $ ol-superset normalize --charts

      # Normalize from a custom assets directory
      $ ol-superset normalize --assets-dir /tmp/exported-assets
    """,
)


@normalize_app.default
def normalize(
    *,
    assets_dir: Path = Path("assets"),
    charts: bool = False,
    datasets: bool = False,
    dashboards: bool = False,
    dry_run: bool = False,
) -> None:
    """Normalize exported YAML assets to reduce erroneous diffs.

    Args:
        assets_dir: Root assets directory (default: assets/)
        charts: Only normalize charts
        datasets: Only normalize datasets
        dashboards: Only normalize dashboards
        dry_run: Show what would change without writing any files
    """
    process_all = not (charts or datasets or dashboards)

    if dry_run:
        console.print("[yellow]🔍 DRY RUN MODE – no files will be written[/yellow]\n")

    console.print("[bold]Normalizing Superset Assets[/bold]\n")

    # Build UUID → name lookup tables once (used across all asset files)
    dataset_uuid_map = build_dataset_uuid_map(assets_dir)
    database_uuid_map = build_database_uuid_map(assets_dir)

    charts_changed = charts_total = 0
    datasets_changed = datasets_total = 0
    dashboards_changed = dashboards_total = 0

    # --- Charts ---
    if process_all or charts:
        charts_dir = assets_dir / "charts"
        if charts_dir.exists():
            console.print("[bold cyan]📈 Normalizing charts…[/bold cyan]")
            for yaml_file in sorted(charts_dir.rglob("*.yaml")):
                if yaml_file.name == "metadata.yaml":
                    continue
                charts_total += 1
                try:
                    changed = normalize_chart(
                        yaml_file,
                        dataset_uuid_map,
                        dry_run=dry_run,
                    )
                    if changed:
                        charts_changed += 1
                        action = "Would update" if dry_run else "Updated"
                        console.print(f"  [green]{action}:[/green] {yaml_file.name}")
                except Exception as exc:  # noqa: BLE001
                    console.print(
                        f"  [red]⚠ Error processing {yaml_file.name}: {exc}[/red]"
                    )
            console.print(
                f"  [dim]{charts_changed}/{charts_total} chart(s) changed[/dim]\n"
            )

    # --- Datasets ---
    if process_all or datasets:
        datasets_dir = assets_dir / "datasets"
        if datasets_dir.exists():
            console.print("[bold cyan]📊 Normalizing datasets…[/bold cyan]")
            for yaml_file in sorted(datasets_dir.rglob("*.yaml")):
                if yaml_file.name == "metadata.yaml":
                    continue
                datasets_total += 1
                try:
                    changed = normalize_dataset(
                        yaml_file,
                        database_uuid_map,
                        dry_run=dry_run,
                    )
                    if changed:
                        datasets_changed += 1
                        action = "Would update" if dry_run else "Updated"
                        console.print(f"  [green]{action}:[/green] {yaml_file.name}")
                except Exception as exc:  # noqa: BLE001
                    console.print(
                        f"  [red]⚠ Error processing {yaml_file.name}: {exc}[/red]"
                    )
            console.print(
                f"  [dim]{datasets_changed}/{datasets_total} dataset(s) changed[/dim]\n"
            )

    # --- Dashboards ---
    if process_all or dashboards:
        dashboards_dir = assets_dir / "dashboards"
        if dashboards_dir.exists():
            console.print("[bold cyan]📋 Normalizing dashboards…[/bold cyan]")
            for yaml_file in sorted(dashboards_dir.rglob("*.yaml")):
                if yaml_file.name == "metadata.yaml":
                    continue
                dashboards_total += 1
                try:
                    changed = normalize_dashboard(yaml_file, dry_run=dry_run)
                    if changed:
                        dashboards_changed += 1
                        action = "Would update" if dry_run else "Updated"
                        console.print(f"  [green]{action}:[/green] {yaml_file.name}")
                except Exception as exc:  # noqa: BLE001
                    console.print(
                        f"  [red]⚠ Error processing {yaml_file.name}: {exc}[/red]"
                    )
            console.print(
                f"  [dim]{dashboards_changed}/{dashboards_total} dashboard(s) changed"
                "[/dim]\n"
            )

    total_changed = charts_changed + datasets_changed + dashboards_changed
    total_files = charts_total + datasets_total + dashboards_total

    if dry_run:
        console.print(
            f"[yellow]Would update {total_changed}/{total_files} file(s). "
            "Run without --dry-run to apply.[/yellow]"
        )
    else:
        console.print(
            f"[bold green]✅ Done — updated {total_changed}/{total_files} "
            "file(s)[/bold green]"
        )
        if total_changed > 0:
            console.print("\n[bold]Next steps:[/bold]")
            console.print("1. Review: [cyan]git diff assets/[/cyan]")
            console.print(
                "2. Commit: [cyan]git add assets/ && git commit -m "
                "'chore: normalize Superset asset YAML'[/cyan]"
            )
