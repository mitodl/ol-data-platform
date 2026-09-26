"""Tests for `ol-dbt generate sources --from-inventory` (INGESTION_INVENTORY_SPEC step 7).

Step 7's acceptance is that regenerating the sources from the inventory changes
nothing except the loader values that were wrong (§1.2). So the cases below pin
both halves: what does change (a loader, a block split by loader, a modeled
table dbt lacked), and what must not (columns, descriptions, comments, tables
the inventory does not declare, and files that already agree).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.lib.inventory import load_units
from ol_dbt_cli.lib.inventory_sources import RAW_SOURCE_NAME, LoaderChange, plan_sources

EDX = "raw__edxorg__"


def _unit(deployment: str, layer: str, loader: str, tables: list[tuple[str, bool]]) -> dict[str, Any]:
    prefix = f"raw__{deployment}__{layer}__"
    return {
        "schema_version": 1,
        "deployment": deployment,
        "layer": layer,
        "scope": "singleton",
        "strategies": {"qa": "omit", "local": "fixture"},
        "loader": loader,
        "table_prefix": prefix,
        "tables": [
            {"name": name, "raw_table": f"{prefix}{name}", "sync_mode": "full_refresh_overwrite", "modeled": modeled}
            for name, modeled in tables
        ],
    }


UNITS = [
    _unit("edxorg", "bigquery", "airbyte", [("mitx_course", True)]),
    _unit("edxorg", "s3", "dlt", [("auth_user", True), ("auth_userprofile", True), ("unmodeled", False)]),
    _unit("edxorg", "course_structure", "dagster", [("course_video", True)]),
]

MIXED = f"""---
version: 2

sources:
- name: {RAW_SOURCE_NAME}
  loader: airbyte
  database: '{{{{ target.database | default(none) }}}}'
  tables:
  # The bigquery export, provided by IRx.
  - name: {EDX}bigquery__mitx_course
    description: MITx courses
    columns:
    - name: course_id
      description: str, the course
  - name: {EDX}s3__auth_user
    description: edX.org users
  - name: {EDX}course_structure__course_video
    description: videos
  - name: {EDX}s3__auth_userprofile
    description: profiles
  - name: {EDX}retired_table
    description: nothing loads this any more
"""


@pytest.fixture
def inventory(tmp_path: Path) -> Path:
    root = tmp_path / "inventory"
    (root / "units").mkdir(parents=True)
    for unit in UNITS:
        path = root / "units" / f"{unit['deployment']}__{unit['layer']}.yml"
        path.write_text(yaml.safe_dump(unit, sort_keys=False))
    return root


def _write(tmp_path: Path, name: str, content: str) -> Path:
    path = tmp_path / name
    path.write_text(content)
    return path


def _blocks(content: str) -> list[tuple[str, list[str]]]:
    document = yaml.safe_load(content)
    return [(block["loader"], [table["name"] for table in block["tables"]]) for block in document["sources"]]


class TestLoader:
    def test_a_mixed_block_splits_into_one_block_per_loader(self, inventory: Path, tmp_path: Path) -> None:
        path = _write(tmp_path, "_edxorg_sources.yml", MIXED)
        plan = plan_sources(load_units(inventory), [path])

        # The leading block keeps its place and its first table's loader; the
        # retired table, which no unit declares, stays with the loader it had.
        assert _blocks(plan.contents[path]) == [
            ("airbyte", [f"{EDX}bigquery__mitx_course", f"{EDX}retired_table"]),
            ("dlt", [f"{EDX}s3__auth_user", f"{EDX}s3__auth_userprofile"]),
            ("dagster", [f"{EDX}course_structure__course_video"]),
        ]

    def test_every_block_keeps_the_source_name_and_its_other_keys(self, inventory: Path, tmp_path: Path) -> None:
        # dbt resolves `source('ol_warehouse_raw_data', ...)` by name, so a split
        # that renamed a block would break every model reading from it.
        path = _write(tmp_path, "_edxorg_sources.yml", MIXED)
        plan = plan_sources(load_units(inventory), [path])

        for block in yaml.safe_load(plan.contents[path])["sources"]:
            assert block["name"] == RAW_SOURCE_NAME
            assert block["database"] == "{{ target.database | default(none) }}"

    def test_only_the_loader_changes(self, inventory: Path, tmp_path: Path) -> None:
        path = _write(tmp_path, "_edxorg_sources.yml", MIXED)
        plan = plan_sources(load_units(inventory), [path])

        def tables(content: str) -> dict[str, Any]:
            return {t["name"]: t for block in yaml.safe_load(content)["sources"] for t in block["tables"]}

        assert tables(plan.contents[path]) == tables(MIXED)
        assert "# The bigquery export, provided by IRx." in plan.contents[path]

    def test_each_change_is_reported(self, inventory: Path, tmp_path: Path) -> None:
        path = _write(tmp_path, "_edxorg_sources.yml", MIXED)
        plan = plan_sources(load_units(inventory), [path])

        assert plan.loader_changes == [
            LoaderChange(f"{EDX}s3__auth_user", "airbyte", "dlt"),
            LoaderChange(f"{EDX}course_structure__course_video", "airbyte", "dagster"),
            LoaderChange(f"{EDX}s3__auth_userprofile", "airbyte", "dlt"),
        ]

    def test_a_wrong_single_loader_is_corrected_in_place(self, inventory: Path, tmp_path: Path) -> None:
        content = f"""---
version: 2
sources:
- name: {RAW_SOURCE_NAME}
  loader: airbyte
  tables:
  - name: {EDX}s3__auth_user
  - name: {EDX}s3__auth_userprofile
"""
        path = _write(tmp_path, "_openedx_sources.yml", content)
        plan = plan_sources(load_units(inventory), [path])

        assert _blocks(plan.contents[path]) == [("dlt", [f"{EDX}s3__auth_user", f"{EDX}s3__auth_userprofile"])]

    def test_a_file_that_agrees_is_not_rewritten(self, inventory: Path, tmp_path: Path) -> None:
        # Rewriting would reflow every long description (ruamel wraps differently
        # from the yamlfmt hook), which is noise in the one diff that should be
        # empty.
        agreeing = MIXED.split("  - name: raw__edxorg__s3__auth_user")[0]
        path = _write(tmp_path, "_edxorg_sources.yml", agreeing)
        plan = plan_sources(load_units(inventory), [path])

        assert plan.contents == {}
        assert plan.loader_changes == []

    def test_non_raw_sources_are_left_alone(self, inventory: Path, tmp_path: Path) -> None:
        other = MIXED.replace(f"- name: {RAW_SOURCE_NAME}", "- name: dimensional")
        path = _write(tmp_path, "_b2b_sources.yml", other)

        assert plan_sources(load_units(inventory), [path]).contents == {}


class TestModeled:
    def test_a_missing_modeled_table_joins_its_unit(self, inventory: Path, tmp_path: Path) -> None:
        content = f"""---
version: 2
sources:
- name: {RAW_SOURCE_NAME}
  loader: dlt
  tables:
  - name: {EDX}s3__auth_user
    description: edX.org users
- name: {RAW_SOURCE_NAME}
  loader: airbyte
  tables:
  - name: {EDX}bigquery__mitx_course
    description: MITx courses
"""
        path = _write(tmp_path, "_edxorg_sources.yml", content)
        plan = plan_sources(load_units(inventory), [path])

        assert _blocks(plan.contents[path])[0] == ("dlt", [f"{EDX}s3__auth_user", f"{EDX}s3__auth_userprofile"])
        assert plan.added == {f"{EDX}s3__auth_userprofile": path}
        # No block holds a course_structure table, so there is nowhere obvious
        # to put it, and an unmodeled table is never added at all.
        assert plan.unplaced == [f"{EDX}course_structure__course_video"]
        assert f"{EDX}s3__unmodeled" not in plan.contents[path]
