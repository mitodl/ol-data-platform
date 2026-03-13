"""Lineage command — render the Dashboard → Chart → Dataset → dbt model graph."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

from ol_superset.lib.asset_index import AssetIndex, DatasetAsset, build_asset_index
from ol_superset.lib.dbt_registry import DbtRegistry, build_dbt_registry
from ol_superset.lib.utils import get_assets_dir


def lineage(
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
                "When provided, resolves datasets to their dbt model and layer."
            ),
        ),
    ] = None,
    dashboard_filter: Annotated[
        str | None,
        Parameter(
            name=["--dashboard", "-D"],
            help=(
                "Limit output to dashboards whose title contains this "
                "substring (case-insensitive)."
            ),
        ),
    ] = None,
    output_format: Annotated[
        str,
        Parameter(
            name=["--format", "-f"],
            help="Output format: text (default), json, or mermaid.",
        ),
    ] = "text",
) -> None:
    """
    Print the full dependency lineage: Dashboard → Chart → Dataset → dbt model.

    Resolves every dashboard's chart references, each chart's dataset, and
    each dataset's backing dbt model (when --dbt-dir is supplied).

    Output formats:
      text    — indented tree printed to stdout (default)
      json    — machine-readable JSON array of dashboard objects
      mermaid — Mermaid.js flowchart (paste into docs or GitHub markdown)

    Examples:
        ol-superset lineage
        ol-superset lineage --dbt-dir ../../ol_dbt
        ol-superset lineage --dashboard "Video Engagement" --format mermaid
        ol-superset lineage --format json | jq '.[].title'
    """
    assets_dir = get_assets_dir(assets_dir_path)
    if not assets_dir.exists():
        print(f"Error: Assets directory not found: {assets_dir}", file=sys.stderr)
        sys.exit(1)

    index = build_asset_index(assets_dir)

    dbt_registry: DbtRegistry | None = None
    if dbt_dir_path is not None:
        dbt_path = Path(dbt_dir_path).resolve()
        if not dbt_path.exists():
            print(f"Error: dbt directory not found: {dbt_path}", file=sys.stderr)
            sys.exit(1)
        try:
            dbt_registry = build_dbt_registry(dbt_path)
        except Exception as exc:  # noqa: BLE001
            print(f"Error: Failed to load dbt registry: {exc}", file=sys.stderr)
            sys.exit(1)

    # Filter dashboards
    dashboards = sorted(index.dashboards.values(), key=lambda d: d.title)
    if dashboard_filter:
        needle = dashboard_filter.lower()
        dashboards = [d for d in dashboards if needle in d.title.lower()]

    if not dashboards:
        label = f" matching '{dashboard_filter}'" if dashboard_filter else ""
        print(f"No dashboards found{label}.")
        return

    if output_format == "json":
        _render_json(dashboards, index, dbt_registry)
    elif output_format == "mermaid":
        _render_mermaid(dashboards, index, dbt_registry)
    else:
        _render_text(dashboards, index, dbt_registry)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _dataset_dbt_label(dataset: DatasetAsset, dbt_registry: DbtRegistry | None) -> str:
    """Return the dbt model name and layer for a dataset, or a fallback tag."""
    if dataset.sql:
        return "[virtual SQL]"
    if dbt_registry is None:
        return ""
    model = dbt_registry.get_model(dataset.table_name)
    if model is None:
        return "[no dbt model]"
    return f"[dbt:{model.layer}]"


def _build_graph(
    dashboards: list,
    index: AssetIndex,
    dbt_registry: DbtRegistry | None,
) -> list[dict]:
    """Build a structured list of dashboard → chart → dataset → model dicts."""
    result = []
    for dash in dashboards:
        charts_out = []
        for chart_uuid in sorted(dash.chart_uuids):
            chart = index.charts.get(chart_uuid)
            if chart is None:
                charts_out.append(
                    {"uuid": chart_uuid, "name": "[missing chart]", "dataset": None}
                )
                continue

            dataset = (
                index.datasets.get(chart.dataset_uuid)
                if chart.dataset_uuid
                else None
            )

            dataset_out: dict | None = None
            if dataset is not None:
                model = None
                if dbt_registry and not dataset.sql:
                    model = dbt_registry.get_model(dataset.table_name)
                dataset_out = {
                    "uuid": dataset.uuid,
                    "table_name": dataset.table_name,
                    "schema": dataset.schema,
                    "is_virtual": dataset.sql is not None,
                    "dbt_model": model.name if model else None,
                    "dbt_layer": model.layer if model else None,
                    "dbt_expected_schema": model.expected_schema if model else None,
                }
            elif chart.dataset_uuid:
                dataset_out = {
                    "uuid": chart.dataset_uuid,
                    "table_name": "[missing dataset]",
                    "schema": None,
                    "is_virtual": False,
                    "dbt_model": None,
                    "dbt_layer": None,
                    "dbt_expected_schema": None,
                }

            charts_out.append(
                {
                    "uuid": chart.uuid,
                    "name": chart.name,
                    "dataset": dataset_out,
                }
            )

        result.append(
            {
                "uuid": dash.uuid,
                "title": dash.title,
                "chart_count": len(dash.chart_uuids),
                "charts": sorted(charts_out, key=lambda c: c["name"]),
            }
        )
    return result


# ---------------------------------------------------------------------------
# Renderers
# ---------------------------------------------------------------------------


def _render_text(
    dashboards: list,
    index: AssetIndex,
    dbt_registry: DbtRegistry | None,
) -> None:
    graph = _build_graph(dashboards, index, dbt_registry)
    for dash in graph:
        print(f"📊 {dash['title']}  ({dash['chart_count']} charts)")
        for chart in dash["charts"]:
            print(f"  📈 {chart['name']}")
            ds = chart["dataset"]
            if ds is None:
                print("      ⚠️  No dataset")
            elif ds["table_name"] == "[missing dataset]":
                print(f"      ❌ Dataset UUID {ds['uuid']} not found in assets/")
            else:
                virtual_tag = " [virtual]" if ds["is_virtual"] else ""
                dbt_tag = ""
                if dbt_registry:
                    if ds["is_virtual"]:
                        dbt_tag = "  →  [virtual SQL]"
                    elif ds["dbt_model"]:
                        dbt_tag = (
                            f"  →  dbt:{ds['dbt_layer']}."
                            f"{ds['dbt_model']}"
                        )
                    else:
                        dbt_tag = "  →  ⚠️  no dbt model"
                print(
                    f"      🗄️  {ds['table_name']}{virtual_tag}"
                    f"  [{ds['schema']}]{dbt_tag}"
                )
        print()


def _render_json(
    dashboards: list,
    index: AssetIndex,
    dbt_registry: DbtRegistry | None,
) -> None:
    graph = _build_graph(dashboards, index, dbt_registry)
    print(json.dumps(graph, indent=2))


def _render_mermaid(
    dashboards: list,
    index: AssetIndex,
    dbt_registry: DbtRegistry | None,
) -> None:
    """Emit a Mermaid flowchart LR diagram."""
    graph = _build_graph(dashboards, index, dbt_registry)

    lines = ["flowchart LR"]
    node_ids: dict[str, str] = {}  # uuid/key → mermaid node id
    counter = [0]

    def node_id(key: str) -> str:
        if key not in node_ids:
            counter[0] += 1
            node_ids[key] = f"n{counter[0]}"
        return node_ids[key]

    def mq(text: str) -> str:
        """Quote a label for Mermaid — escape double-quotes."""
        return text.replace('"', "'")

    for dash in graph:
        did = node_id(dash["uuid"])
        lines.append(f'    {did}["📊 {mq(dash["title"])}"]')
        lines.append(f"    style {did} fill:#4e79a7,color:#fff,stroke:#2d5a8a")

        seen_datasets: set[str] = set()

        for chart in dash["charts"]:
            cid = node_id(chart["uuid"])
            lines.append(f'    {cid}["📈 {mq(chart["name"])}"]')
            lines.append(f"    {did} --> {cid}")

            ds = chart["dataset"]
            if ds is None:
                wid = node_id(f"no_ds_{chart['uuid']}")
                lines.append(f'    {wid}["⚠️ no dataset"]')
                lines.append(f"    {cid} --> {wid}")
                continue

            ds_key = ds["uuid"]
            dsid = node_id(ds_key)
            if ds_key not in seen_datasets:
                seen_datasets.add(ds_key)
                if ds["is_virtual"]:
                    label = f"🗄️ {mq(ds['table_name'])} [virtual]"
                    lines.append(f'    {dsid}["{label}"]')
                    lines.append(
                        f"    style {dsid} fill:#f28e2b,color:#fff,stroke:#b86800"
                    )
                elif ds["table_name"] == "[missing dataset]":
                    label = f"❌ {ds['uuid'][:8]}... not found"
                    lines.append(f'    {dsid}["{label}"]')
                    lines.append(
                        f"    style {dsid} fill:#e15759,color:#fff,stroke:#a11"
                    )
                else:
                    label = f"🗄️ {mq(ds['table_name'])}"
                    lines.append(f'    {dsid}["{label}"]')
                    lines.append(
                        f"    style {dsid} fill:#76b7b2,color:#fff,stroke:#3d8580"
                    )

                # dbt model node
                if dbt_registry and ds["dbt_model"]:
                    model_key = f"dbt_{ds['dbt_model']}"
                    mid = node_id(model_key)
                    label = f"🧱 {mq(ds['dbt_model'])} [{ds['dbt_layer']}]"
                    lines.append(f'    {mid}["{label}"]')
                    lines.append(
                        f"    style {mid} fill:#59a14f,color:#fff,stroke:#2d6b25"
                    )
                    lines.append(f"    {dsid} --> {mid}")

            lines.append(f"    {cid} --> {dsid}")

    print("\n".join(lines))
